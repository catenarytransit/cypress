use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

use cypress::elasticsearch::EsClient;
use cypress::models::normalized::NormalizedPlace;
use cypress::models::AdminEntry;
use cypress::scylla::ScyllaClient;

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

/// Search result V2 in GeoJSON-like format
#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResultV2 {
    #[serde(rename = "type")]
    pub result_type: String,
    pub geometry: Geometry,
    pub properties: PropertiesV2,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct PropertiesV2 {
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
    pub country_names: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region_names: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub county: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub county_names: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locality: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locality_names: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub neighbourhood: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub neighbourhood_names: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<String>,
    pub confidence: f64,
}

/// Execute a forward geocoding search
pub async fn execute_search(
    client: &EsClient,
    scylla_client: &ScyllaClient,
    params: SearchParams,
    autocomplete: bool,
) -> Result<Vec<SearchResult>> {
    let results = execute_search_internal(client, scylla_client, params, autocomplete).await?;
    let mut search_results = Vec::new();

    for (place, score, preferred_lang, parsed_admin_map) in results {
        if let Some(result) =
            place_to_search_result(place, score, &preferred_lang, &parsed_admin_map)
        {
            search_results.push(result);
        }
    }

    Ok(search_results)
}

/// Execute a forward geocoding search V2
pub async fn execute_search_v2(
    client: &EsClient,
    scylla_client: &ScyllaClient,
    params: SearchParams,
    autocomplete: bool,
) -> Result<Vec<SearchResultV2>> {
    let results = execute_search_internal(client, scylla_client, params, autocomplete).await?;
    let mut search_results = Vec::new();

    for (place, score, preferred_lang, parsed_admin_map) in results {
        if let Some(result) =
            place_to_search_result_v2(place, score, &preferred_lang, &parsed_admin_map)
        {
            search_results.push(result);
        }
    }

    Ok(search_results)
}

async fn execute_search_internal(
    client: &EsClient,
    scylla_client: &ScyllaClient,
    params: SearchParams,
    autocomplete: bool,
) -> Result<
    Vec<(
        NormalizedPlace,
        f64,
        Option<String>,
        HashMap<String, AdminEntry>,
    )>,
> {
    // Build multi-match query across name fields
    let name_field = if autocomplete {
        "name_all.autocomplete"
    } else {
        "name_all"
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
                "name_all": {
                    "query": &params.text,
                    "boost": 20.0
                }
            }
        }),
        // Address matches
        json!({
            "match": {
                "address.street": {
                    "query": &params.text,
                    "boost": 5.0
                }
            }
        }),
        json!({
            "match": {
                "address.city": {
                    "query": &params.text,
                    "boost": 3.0
                }
            }
        }),
        json!({
            "match": {
                "address.postcode": {
                    "query": &params.text,
                    "boost": 5.0
                }
            }
        }),
        // Parent admin matches (aggregated name support)
        json!({
            "multi_match": {
                "query": &params.text,
                "fields": [
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
                "boost": 2.0
            }
        }),
        // Name + Address + Admin hybrid search (Cross Fields)
        json!({
            "multi_match": {
                "query": &params.text,
                "type": "cross_fields",
                "fields": [
                    "name_all",
                    "address.street",
                    "address.city",
                    "address.postcode",
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
                "boost": 15.0
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

    // Build function score for location bias + importance
    let mut functions = vec![
        // Base score to prevent zeroing out documents with 0 importance
        // (Since boost_mode is multiply, we need a base of 1.0)
        json!({
            "filter": { "match_all": {} },
            "weight": 1.0
        }),
        // Importance boosting
        json!({
            "field_value_factor": {
                "field": "importance",
                "missing": 0.0, // Default importance if missing
                "factor": 1.0,  // Tuning parameter
                "modifier": "log1p", // log(1 + importance) - smooth curve
            },
             "weight": 2.0 // Boost importance influence
        }),
    ];

    if params.focus_lat.is_some() && params.focus_lon.is_some() {
        let focus_lat = params.focus_lat.unwrap();
        let focus_lon = params.focus_lon.unwrap();
        functions.push(json!({
            "gauss": {
                "center_point": {
                    "origin": { "lat": focus_lat, "lon": focus_lon },
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
            "score_mode": "sum",    // Sum the scores from functions (importance + geo)
            "boost_mode": "multiply" // Multiply original text score by function score
        }
    });

    // Build full request body
    let mut body = json!({
        "query": query,
        "size": params.size,
        "stored_fields": ["_id"]
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

    // Parse results and fetch from Scylla
    let hits = response_body["hits"]["hits"]
        .as_array()
        .map(|a| a.to_vec())
        .unwrap_or_default();

    let mut places_to_fetch = Vec::new();
    let mut scores = HashMap::new();

    for hit in hits {
        if let Some(id) = hit["_id"].as_str() {
            places_to_fetch.push(id.to_string());
            let score = hit["_score"].as_f64().unwrap_or(0.0);
            scores.insert(id.to_string(), score);
        }
    }

    // Fetch from Scylla in parallel
    let fetch_futures = places_to_fetch.iter().map(|id| scylla_client.get_place(id));
    let mut normalized_places = Vec::new();
    let mut admin_ids = std::collections::HashSet::new();

    for (i, fetch_result) in futures::future::join_all(fetch_futures)
        .await
        .into_iter()
        .enumerate()
    {
        if let Ok(Some(json_data)) = fetch_result {
            if let Ok(place) = serde_json::from_str::<NormalizedPlace>(&json_data) {
                let id = places_to_fetch[i].clone();
                let score = scores.get(&id).copied().unwrap_or(0.0);

                // Collect admin IDs
                collect_admin_ids(&place, &mut admin_ids);

                normalized_places.push((place, score));
            }
        }
    }

    // Batch fetch admin areas
    let admin_ids_vec: Vec<String> = admin_ids.into_iter().collect();
    let admin_map = scylla_client.get_admin_areas(&admin_ids_vec).await?;

    // Parse admin entries map once
    let parsed_admin_map: HashMap<String, AdminEntry> = admin_map
        .iter()
        .filter_map(|(k, v)| {
            serde_json::from_str::<cypress::models::admin::AdminEntryScylla>(v)
                .ok()
                .map(|scylla_entry| (k.clone(), AdminEntry::from_scylla(scylla_entry)))
        })
        .collect();

    let mut results = Vec::new();
    for (place, score) in normalized_places {
        results.push((place, score, params.lang.clone(), parsed_admin_map.clone()));
    }

    Ok(results)
}

/// Execute a reverse geocoding search
pub async fn execute_reverse(
    client: &EsClient,
    scylla_client: &ScyllaClient,
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
        "stored_fields": ["_id"]
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

    let mut places_to_fetch = Vec::new();

    for hit in hits {
        if let Some(id) = hit["_id"].as_str() {
            places_to_fetch.push(id.to_string());
        }
    }

    // Fetch from Scylla
    let fetch_futures = places_to_fetch.iter().map(|id| scylla_client.get_place(id));
    let mut normalized_places = Vec::new();
    let mut admin_ids = std::collections::HashSet::new();

    for fetch_result in futures::future::join_all(fetch_futures).await {
        if let Ok(Some(json_data)) = fetch_result {
            if let Ok(place) = serde_json::from_str::<NormalizedPlace>(&json_data) {
                collect_admin_ids(&place, &mut admin_ids);
                normalized_places.push(place);
            }
        }
    }

    // Batch fetch admin areas
    let admin_ids_vec: Vec<String> = admin_ids.into_iter().collect();
    let admin_map = scylla_client.get_admin_areas(&admin_ids_vec).await?;

    let parsed_admin_map: HashMap<String, AdminEntry> = admin_map
        .iter()
        .filter_map(|(k, v)| {
            serde_json::from_str::<cypress::models::admin::AdminEntryScylla>(v)
                .ok()
                .map(|scylla_entry| (k.clone(), AdminEntry::from_scylla(scylla_entry)))
        })
        .collect();

    let mut results = Vec::new();
    for place in normalized_places {
        if let Some(result) = place_to_search_result(place, 1.0, &None, &parsed_admin_map) {
            results.push(result);
        }
    }
    Ok(results)
}

/// Parse an Elasticsearch hit into a SearchResult
fn collect_admin_ids(place: &NormalizedPlace, ids: &mut std::collections::HashSet<String>) {
    if let Some(ref id) = place.parent.country {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.macro_region {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.region {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.macro_county {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.county {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.local_admin {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.locality {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.borough {
        ids.insert(id.clone());
    }
    if let Some(ref id) = place.parent.neighbourhood {
        ids.insert(id.clone());
    }
}

fn resolve_admin_name(
    id: &Option<String>,
    map: &HashMap<String, AdminEntry>,
    lang: &Option<String>,
) -> Option<String> {
    id.as_ref().and_then(|id_str| {
        map.get(id_str).and_then(|entry| {
            // Try language-specific name first, then fall back to default name
            // Names HashMap uses simple language codes: "de", "fr", "id", etc.
            lang.as_ref()
                .and_then(|l| entry.names.get(l))
                .cloned()
                .or_else(|| entry.names.get("default").cloned())
                .or_else(|| entry.name.clone())
        })
    })
}

fn resolve_admin_names(
    id: &Option<String>,
    map: &HashMap<String, AdminEntry>,
) -> Option<HashMap<String, String>> {
    id.as_ref()
        .and_then(|id_str| map.get(id_str).map(|entry| entry.names.clone()))
}

/// Convert a Place model to SearchResult
fn place_to_search_result(
    place: NormalizedPlace,
    score: f64,
    preferred_lang: &Option<String>,
    admin_map: &HashMap<String, AdminEntry>,
) -> Option<SearchResult> {
    // Pick display name based on language preference
    let display_name = preferred_lang
        .as_ref()
        .and_then(|lang| place.name.get(lang))
        .or_else(|| place.name.get("default"))
        .or_else(|| place.name.values().next())
        .cloned()
        .unwrap_or_default();

    Some(SearchResult {
        result_type: "Feature".to_string(),
        geometry: Geometry {
            geo_type: "Point".to_string(),
            coordinates: [place.center_point.lon, place.center_point.lat],
        },
        properties: Properties {
            id: place.source_id,
            layer: format!("{:?}", place.layer).to_lowercase(), // format! using generic debug or we can impl Display
            name: display_name,
            names: place.name,
            housenumber: place.address.as_ref().and_then(|a| a.housenumber.clone()),
            street: place.address.as_ref().and_then(|a| a.street.clone()),
            postcode: place.address.as_ref().and_then(|a| a.postcode.clone()),
            country: resolve_admin_name(&place.parent.country, admin_map, preferred_lang),
            region: resolve_admin_name(&place.parent.region, admin_map, preferred_lang),
            county: resolve_admin_name(&place.parent.county, admin_map, preferred_lang),
            locality: resolve_admin_name(&place.parent.locality, admin_map, preferred_lang),
            neighbourhood: resolve_admin_name(
                &place.parent.neighbourhood,
                admin_map,
                preferred_lang,
            ),
            categories: place.categories,
            confidence: score,
        },
    })
}

/// Convert a Place model to SearchResult V2
fn place_to_search_result_v2(
    place: NormalizedPlace,
    score: f64,
    preferred_lang: &Option<String>,
    admin_map: &HashMap<String, AdminEntry>,
) -> Option<SearchResultV2> {
    // Pick display name based on language preference
    let display_name = preferred_lang
        .as_ref()
        .and_then(|lang| place.name.get(lang))
        .or_else(|| place.name.get("default"))
        .or_else(|| place.name.values().next())
        .cloned()
        .unwrap_or_default();

    Some(SearchResultV2 {
        result_type: "Feature".to_string(),
        geometry: Geometry {
            geo_type: "Point".to_string(),
            coordinates: [place.center_point.lon, place.center_point.lat],
        },
        properties: PropertiesV2 {
            id: place.source_id,
            layer: format!("{:?}", place.layer).to_lowercase(),
            name: display_name,
            names: place.name,
            housenumber: place.address.as_ref().and_then(|a| a.housenumber.clone()),
            street: place.address.as_ref().and_then(|a| a.street.clone()),
            postcode: place.address.as_ref().and_then(|a| a.postcode.clone()),
            country: resolve_admin_name(&place.parent.country, admin_map, preferred_lang),
            country_names: resolve_admin_names(&place.parent.country, admin_map),
            region: resolve_admin_name(&place.parent.region, admin_map, preferred_lang),
            region_names: resolve_admin_names(&place.parent.region, admin_map),
            county: resolve_admin_name(&place.parent.county, admin_map, preferred_lang),
            county_names: resolve_admin_names(&place.parent.county, admin_map),
            locality: resolve_admin_name(&place.parent.locality, admin_map, preferred_lang),
            locality_names: resolve_admin_names(&place.parent.locality, admin_map),
            neighbourhood: resolve_admin_name(
                &place.parent.neighbourhood,
                admin_map,
                preferred_lang,
            ),
            neighbourhood_names: resolve_admin_names(&place.parent.neighbourhood, admin_map),
            categories: place.categories,
            confidence: score,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cypress::models::normalized::{AdminHierarchyIds, NormalizedPlace};
    use cypress::models::place::{GeoPoint, Layer, OsmType};
    use std::collections::HashMap;

    #[test]
    fn test_place_to_search_result_v2() {
        let mut names = HashMap::new();
        names.insert("default".to_string(), "London".to_string());
        names.insert("fr".to_string(), "Londres".to_string());

        let mut country_names = HashMap::new();
        country_names.insert("default".to_string(), "United Kingdom".to_string());
        country_names.insert("de".to_string(), "Vereinigtes Königreich".to_string());

        let country_entry = AdminEntry {
            name: Some("United Kingdom".to_string()),
            abbr: Some("UK".to_string()),
            id: Some(1),
            bbox: None,
            names: country_names.clone(),
        };

        let mut admin_map = HashMap::new();
        admin_map.insert("relation/1".to_string(), country_entry);

        let mut place = NormalizedPlace {
            source_id: "test:1".to_string(),
            source_file: "test.osm".to_string(),
            import_timestamp: chrono::Utc::now(),
            osm_type: OsmType::Node,
            osm_id: 123,
            wikidata_id: None,
            importance: Some(0.5),
            layer: Layer::Locality,
            categories: vec![],
            name: names.clone(),
            phrase: None,
            address: None,
            center_point: GeoPoint {
                lon: 0.1,
                lat: 51.5,
            },
            bbox: None,
            parent: AdminHierarchyIds::default(),
        };
        place.parent.country = Some("relation/1".to_string());

        let result =
            place_to_search_result_v2(place, 1.0, &Some("fr".to_string()), &admin_map).unwrap();

        assert_eq!(result.properties.name, "Londres");
        assert_eq!(result.properties.names.get("default").unwrap(), "London");
        assert_eq!(result.properties.names.get("fr").unwrap(), "Londres");

        // Verify country names
        assert!(result.properties.country_names.is_some());
        let c_names = result.properties.country_names.unwrap();
        assert_eq!(c_names.get("default").unwrap(), "United Kingdom");
        assert_eq!(c_names.get("de").unwrap(), "Vereinigtes Königreich");

        // Verify that other fields (like region) would also work if populated (mocking logic verification)
        // Since we mocked country, we know the logic is generic.
        // But let's add a region just to be sure.
    }

    #[test]
    fn test_place_to_search_result_v2_full_hierarchy() {
        let mut names = HashMap::new();
        names.insert("default".to_string(), "Paris".to_string());

        let mut country_names = HashMap::new();
        country_names.insert("default".to_string(), "France".to_string());

        let mut region_names = HashMap::new();
        region_names.insert("default".to_string(), "Ile-de-France".to_string());
        region_names.insert("en".to_string(), "Isle of France".to_string());

        let admin_map = HashMap::from([
            (
                "relation/1".to_string(),
                AdminEntry {
                    name: Some("France".to_string()),
                    names: country_names,
                    ..Default::default()
                },
            ),
            (
                "relation/2".to_string(),
                AdminEntry {
                    name: Some("Ile-de-France".to_string()),
                    names: region_names,
                    ..Default::default()
                },
            ),
        ]);

        let mut place = NormalizedPlace {
            source_id: "test:2".to_string(),
            source_file: "test.osm".to_string(),
            import_timestamp: chrono::Utc::now(),
            osm_type: OsmType::Node,
            osm_id: 456,
            wikidata_id: None,
            importance: Some(0.8),
            layer: Layer::Locality,
            categories: vec![],
            name: names,
            phrase: None,
            address: None,
            center_point: GeoPoint {
                lon: 2.35,
                lat: 48.85,
            },
            bbox: None,
            parent: AdminHierarchyIds::default(),
        };
        place.parent.country = Some("relation/1".to_string());
        place.parent.region = Some("relation/2".to_string());

        let result =
            place_to_search_result_v2(place, 1.0, &Some("en".to_string()), &admin_map).unwrap();

        assert_eq!(
            result
                .properties
                .country_names
                .unwrap()
                .get("default")
                .unwrap(),
            "France"
        );
        assert_eq!(
            result.properties.region_names.unwrap().get("en").unwrap(),
            "Isle of France"
        );
    }
}
