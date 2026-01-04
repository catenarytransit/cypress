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
    pub focus_weight: Option<f64>,
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

/// List of fields required for reconstruction when using synthetic source.
const REQUIRED_FIELDS: &[&str] = &[
    "source_id",
    "layer",
    "center_point",
    "name.*",
    "address.housenumber",
    "address.street",
    "address.postcode",
    "categories",
    "parent.country.name",
    "parent.region.name",
    "parent.county.name",
    "parent.locality.name",
    "parent.neighbourhood.name",
];

/// Execute a forward geocoding search
pub async fn execute_search(
    client: &EsClient,
    params: SearchParams,
    autocomplete: bool,
) -> Result<Vec<SearchResult>> {
    let name_field = if autocomplete {
        "name.default.autocomplete"
    } else {
        "name.default"
    };

    let should_clauses = vec![
        json!({
            "match": {
                name_field: {
                    "query": &params.text,
                    "boost": 10.0
                }
            }
        }),
        json!({
            "match_phrase": {
                "phrase": {
                    "query": &params.text,
                    "boost": 20.0
                }
            }
        }),
        json!({
            "match": {
                "name_all": {
                    "query": &params.text,
                    "boost": 5.0
                }
            }
        }),
        json!({
            "match": {
                "address.street": {
                    "query": &params.text,
                    "boost": 3.0
                }
            }
        }),
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
        json!({
            "multi_match": {
                "query": &params.text,
                "type": "cross_fields",
                "fields": [
                    "name.default",
                    "parent.country.name",
                    "parent.macro_region.name",
                    "parent.region.name",
                    "parent.macro_county.name",
                    "parent.county.name",
                    "parent.local_admin.name",
                    "parent.locality.name",
                    "parent.borough.name",
                    "parent.neighbourhood.name"
                ],
                "analyzer": "peliasQuery",
                "operator": "and",
                "boost": 8.0
            }
        }),
    ];

    let mut bool_query = json!({
        "should": should_clauses,
        "minimum_should_match": 1
    });

    if let Some(ref layers) = params.layers {
        bool_query["filter"] = json!([{
            "terms": { "layer": layers }
        }]);
    }

    let mut functions = vec![
        json!({
            "filter": { "match_all": {} },
            "weight": 1.0
        }),
        json!({
            "field_value_factor": {
                "field": "importance",
                "missing": 0.0,
                "factor": 1.0,
                "modifier": "log1p",
            },
             "weight": 2.0
        }),
    ];

    if let (Some(lat), Some(lon)) = (params.focus_lat, params.focus_lon) {
        functions.push(json!({
            "gauss": {
                "center_point": {
                    "origin": { "lat": lat, "lon": lon },
                    "scale": "50km",
                    "offset": "10km",
                    "decay": 0.5
                }
            },
            "weight": params.focus_weight.unwrap_or(3.0)
        }));
    }

    let query = json!({
        "function_score": {
            "query": { "bool": bool_query },
            "functions": functions,
            "score_mode": "sum",
            "boost_mode": "multiply"
        }
    });

    let mut body = json!({
        "query": query,
        "size": params.size,
        "fields": REQUIRED_FIELDS,
        "_source": false
    });

    if let Some(bbox) = params.bbox {
        let filter = json!({
            "geo_bounding_box": {
                "center_point": {
                    "top_left": { "lon": bbox[0], "lat": bbox[3] },
                    "bottom_right": { "lon": bbox[2], "lat": bbox[1] }
                }
            }
        });

        // Simplified filter insertion logic for clarity
        if let Some(bool_obj) = body["query"]["function_score"]["query"]["bool"].as_object_mut() {
            let filters = bool_obj.entry("filter").or_insert(json!([]));
            if let Some(filters_arr) = filters.as_array_mut() {
                filters_arr.push(filter);
            }
        }
    }

    debug!("Search query: {}", serde_json::to_string_pretty(&body)?);

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
        "size": size,
        "fields": REQUIRED_FIELDS,
        "_source": false
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

/// Helper to get a string value from the Elasticsearch 'fields' response object.
/// Elasticsearch returns fields as arrays, e.g., "layer": ["venue"].
fn get_field_str(fields: &serde_json::Value, path: &str) -> Option<String> {
    fields[path].as_array()?.first()?.as_str().map(String::from)
}

/// Parse an Elasticsearch hit into a SearchResult
fn parse_hit(hit: serde_json::Value, preferred_lang: &Option<String>) -> Option<SearchResult> {
    // When using synthetic source and the 'fields' parameter, data is in hit["fields"]
    let fields = &hit["fields"];
    let score = hit["_score"].as_f64().unwrap_or(0.0);

    // Get coordinates from center_point. Synthetic source/fields returns geo_point as strings or objects in array
    // Standard format for geo_point in 'fields' is an array of strings: ["lat,lon"]
    let lat_lon_str = fields["center_point"].as_array()?.first()?.as_str()?;
    let coords: Vec<&str> = lat_lon_str.split(',').collect();
    let lat = coords.get(0)?.parse::<f64>().ok()?;
    let lon = coords.get(1)?.parse::<f64>().ok()?;

    // Reconstruct names map from fields matching "name.*"
    let mut names = HashMap::new();
    if let Some(obj) = fields.as_object() {
        for (key, value) in obj {
            if key.starts_with("name.") {
                let lang = key.strip_prefix("name.").unwrap_or("default");
                if let Some(val_str) = value.as_array().and_then(|a| a.first()).and_then(|v| v.as_str()) {
                    names.insert(lang.to_string(), val_str.to_string());
                }
            }
        }
    }

    let display_name = preferred_lang
        .as_ref()
        .and_then(|lang| names.get(lang))
        .or_else(|| names.get("default"))
        .or_else(|| names.values().next())
        .cloned()
        .unwrap_or_default();

    Some(SearchResult {
        result_type: "Feature".to_string(),
        geometry: Geometry {
            geo_type: "Point".to_string(),
            coordinates: [lon, lat],
        },
        properties: Properties {
            id: get_field_str(fields, "source_id")?,
            layer: get_field_str(fields, "layer")?,
            name: display_name,
            names,
            housenumber: get_field_str(fields, "address.housenumber"),
            street: get_field_str(fields, "address.street"),
            postcode: get_field_str(fields, "address.postcode"),
            country: get_field_str(fields, "parent.country.name"),
            region: get_field_str(fields, "parent.region.name"),
            county: get_field_str(fields, "parent.county.name"),
            locality: get_field_str(fields, "parent.locality.name"),
            neighbourhood: get_field_str(fields, "parent.neighbourhood.name"),
            categories: fields["categories"]
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
