//! Search query building and execution.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

use cypress::elasticsearch::EsClient;

/// Search parameters
pub struct SearchParams {
    pub text: String,
    pub lang: Option<String>,
    pub bbox: Option<[f64; 4]>,
    pub focus_lat: Option<f64>,
    pub focus_lon: Option<f64>,
    pub layers: Option<Vec<String>>,
    pub size: usize,
}

/// Search result in GeoJSON-like format
#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResult {
    #[serde(rename = "type")]
    pub result_type: String,
    pub geometry: Geometry,
    pub properties: Properties,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Geometry {
    #[serde(rename = "type")]
    pub geo_type: String,
    pub coordinates: [f64; 2],
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Properties {
    pub id: String,
    pub layer: String,
    pub name: String,
    /// All available language variants
    pub names: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub housenumber: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub street: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postcode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub county: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locality: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub neighbourhood: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<String>,
    pub confidence: f64,
}

/// Execute a forward geocoding search
pub async fn execute_search(
    client: &EsClient,
    params: SearchParams,
    autocomplete: bool,
) -> Result<Vec<SearchResult>> {
    // Build multi-match query across name fields
    let name_field = if autocomplete {
        "name.default.autocomplete"
    } else {
        "name.default"
    };

    let should_clauses = vec![
        // Main name match
        json!({
            "match": {
                name_field: {
                    "query": &params.text,
                    "boost": 10.0
                }
            }
        }),
        // Phrase match for exact ordering
        json!({
            "match_phrase": {
                "phrase": {
                    "query": &params.text,
                    "boost": 20.0
                }
            }
        }),
        // Wildcard search across all name languages
        json!({
            "multi_match": {
                "query": &params.text,
                "type": "best_fields",
                "fields": ["name.*"],
                "boost": 5.0
            }
        }),
        // Address street match
        json!({
            "match": {
                "address.street": {
                    "query": &params.text,
                    "boost": 3.0
                }
            }
        }),
        // Parent admin matches (for "city" or "country" searches)
        json!({
            "multi_match": {
                "query": &params.text,
                "fields": [
                    "parent.country.name",
                    "parent.region.name",
                    "parent.county.name",
                    "parent.locality.name",
                    "parent.neighbourhood.name"
                ],
                "boost": 2.0
            }
        }),
    ];

    // Build query with optional filters
    let mut bool_query = json!({
        "should": should_clauses,
        "minimum_should_match": 1
    });

    // Add layer filter
    if let Some(ref layers) = params.layers {
        bool_query["filter"] = json!([{
            "terms": { "layer": layers }
        }]);
    }

    // Build function score for location bias
    let mut query = json!({
        "bool": bool_query
    });

    // Wrap with function score for geo bias
    if params.focus_lat.is_some() && params.focus_lon.is_some() {
        let focus_lat = params.focus_lat.unwrap();
        let focus_lon = params.focus_lon.unwrap();

        query = json!({
            "function_score": {
                "query": query,
                "functions": [
                    {
                        "gauss": {
                            "center_point": {
                                "origin": { "lat": focus_lat, "lon": focus_lon },
                                "scale": "50km",
                                "offset": "10km",
                                "decay": 0.5
                            }
                        },
                        "weight": 3
                    }
                ],
                "score_mode": "multiply",
                "boost_mode": "multiply"
            }
        });
    }

    // Build full request body
    let mut body = json!({
        "query": query,
        "size": params.size
    });

    // Add bounding box filter
    if let Some(bbox) = params.bbox {
        let filter = json!({
            "geo_bounding_box": {
                "center_point": {
                    "top_left": { "lon": bbox[0], "lat": bbox[3] },
                    "bottom_right": { "lon": bbox[2], "lat": bbox[1] }
                }
            }
        });

        if let Some(existing_filter) = body["query"]["bool"]["filter"].as_array_mut() {
            existing_filter.push(filter);
        } else if body["query"]["bool"].is_object() {
            body["query"]["bool"]["filter"] = json!([filter]);
        } else if let Some(fq) =
            body["query"]["function_score"]["query"]["bool"]["filter"].as_array_mut()
        {
            fq.push(filter);
        } else if body["query"]["function_score"]["query"]["bool"].is_object() {
            body["query"]["function_score"]["query"]["bool"]["filter"] = json!([filter]);
        }
    }

    debug!("Search query: {}", serde_json::to_string_pretty(&body)?);

    // Execute search
    let response = client
        .client()
        .search(elasticsearch::SearchParts::Index(&[&client.index_name]))
        .body(body)
        .send()
        .await?;

    let response_body = response.json::<serde_json::Value>().await?;

    // Parse results
    let hits = response_body["hits"]["hits"]
        .as_array()
        .map(|a| a.to_vec())
        .unwrap_or_default();

    let results: Vec<SearchResult> = hits
        .into_iter()
        .filter_map(|hit| parse_hit(hit, &params.lang))
        .collect();

    Ok(results)
}

/// Execute a reverse geocoding search
pub async fn execute_reverse(
    client: &EsClient,
    lon: f64,
    lat: f64,
    size: usize,
    layers: Option<Vec<String>>,
) -> Result<Vec<SearchResult>> {
    let mut bool_query = json!({
        "must": {
            "match_all": {}
        }
    });

    if let Some(ref layers) = layers {
        bool_query["filter"] = json!([{
            "terms": { "layer": layers }
        }]);
    }

    let body = json!({
        "query": {
            "bool": bool_query
        },
        "sort": [
            {
                "_geo_distance": {
                    "center_point": { "lat": lat, "lon": lon },
                    "order": "asc",
                    "unit": "m"
                }
            }
        ],
        "size": size
    });

    let response = client
        .client()
        .search(elasticsearch::SearchParts::Index(&[&client.index_name]))
        .body(body)
        .send()
        .await?;

    let response_body = response.json::<serde_json::Value>().await?;

    let hits = response_body["hits"]["hits"]
        .as_array()
        .map(|a| a.to_vec())
        .unwrap_or_default();

    let results: Vec<SearchResult> = hits
        .into_iter()
        .filter_map(|hit| parse_hit(hit, &None))
        .collect();

    Ok(results)
}

/// Parse an Elasticsearch hit into a SearchResult
fn parse_hit(hit: serde_json::Value, preferred_lang: &Option<String>) -> Option<SearchResult> {
    let source = &hit["_source"];
    let score = hit["_score"].as_f64().unwrap_or(0.0);

    // Get coordinates
    let center_point = &source["center_point"];
    let lat = center_point["lat"].as_f64()?;
    let lon = center_point["lon"].as_f64()?;

    // Get all names
    let names_obj = source["name"].as_object()?;
    let names: HashMap<String, String> = names_obj
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
        .collect();

    // Pick display name based on language preference
    let display_name = preferred_lang
        .as_ref()
        .and_then(|lang| names.get(lang))
        .or_else(|| names.get("default"))
        .or_else(|| names.values().next())
        .cloned()
        .unwrap_or_default();

    // Get admin hierarchy for display
    let parent = &source["parent"];

    Some(SearchResult {
        result_type: "Feature".to_string(),
        geometry: Geometry {
            geo_type: "Point".to_string(),
            coordinates: [lon, lat],
        },
        properties: Properties {
            id: source["source_id"].as_str()?.to_string(),
            layer: source["layer"].as_str()?.to_string(),
            name: display_name,
            names,
            housenumber: source["address"]["housenumber"].as_str().map(String::from),
            street: source["address"]["street"].as_str().map(String::from),
            postcode: source["address"]["postcode"].as_str().map(String::from),
            country: parent["country"]["name"].as_str().map(String::from),
            region: parent["region"]["name"].as_str().map(String::from),
            county: parent["county"]["name"].as_str().map(String::from),
            locality: parent["locality"]["name"].as_str().map(String::from),
            neighbourhood: parent["neighbourhood"]["name"].as_str().map(String::from),
            categories: source["categories"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            confidence: score,
        },
    })
}
