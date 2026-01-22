use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

use cypress::elasticsearch::EsClient;
use cypress::models::normalized::NormalizedPlace;
use cypress::models::place::Layer;
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

pub struct TimedSearchResults {
    pub results: Vec<SearchResult>,
    pub es_took_ms: u128,
    pub scylla_took_ms: u128,
}

pub struct TimedSearchResultsV2 {
    pub results: Vec<SearchResultV2>,
    pub es_took_ms: u128,
    pub scylla_took_ms: u128,
}

struct InternalTimedResults {
    places: Vec<(
        NormalizedPlace,
        f64,
        Option<String>,
        HashMap<String, AdminEntry>,
    )>,
    es_took_ms: u128,
    scylla_took_ms: u128,
}

/// Execute a forward geocoding search
pub async fn execute_search(
    client: &EsClient,
    scylla_client: &ScyllaClient,
    params: SearchParams,
    autocomplete: bool,
) -> Result<TimedSearchResults> {
    let internal_results =
        execute_search_internal(client, scylla_client, params, autocomplete).await?;
    let mut search_results = Vec::new();

    for (place, score, preferred_lang, parsed_admin_map) in internal_results.places {
        if let Some(result) =
            place_to_search_result(place, score, &preferred_lang, &parsed_admin_map)
        {
            search_results.push(result);
        }
    }

    Ok(TimedSearchResults {
        results: search_results,
        es_took_ms: internal_results.es_took_ms,
        scylla_took_ms: internal_results.scylla_took_ms,
    })
}

/// Execute a forward geocoding search V2
pub async fn execute_search_v2(
    client: &EsClient,
    scylla_client: &ScyllaClient,
    params: SearchParams,
    autocomplete: bool,
) -> Result<TimedSearchResultsV2> {
    let internal_results =
        execute_search_internal(client, scylla_client, params, autocomplete).await?;
    let mut search_results = Vec::new();

    for (place, score, preferred_lang, parsed_admin_map) in internal_results.places {
        if let Some(result) =
            place_to_search_result_v2(place, score, &preferred_lang, &parsed_admin_map)
        {
            search_results.push(result);
        }
    }

    Ok(TimedSearchResultsV2 {
        results: search_results,
        es_took_ms: internal_results.es_took_ms,
        scylla_took_ms: internal_results.scylla_took_ms,
    })
}

async fn execute_search_internal(
    client: &EsClient,
    scylla_client: &ScyllaClient,
    params: SearchParams,
    autocomplete: bool,
) -> Result<InternalTimedResults> {
    // Build full request body
    let mut body = build_search_query(&params, autocomplete);

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
    // Execute search
    let start_es = std::time::Instant::now();
    let response = client
        .client()
        .search(elasticsearch::SearchParts::Index(&[&client.index_name]))
        .body(body)
        .send()
        .await?;
    let es_took_ms = start_es.elapsed().as_millis();

    let response_body = response.json::<serde_json::Value>().await?;

    // Parse results and fetch from Scylla
    let hits = response_body["hits"]["hits"]
        .as_array()
        .map(|a| a.to_vec())
        .unwrap_or_default();

    debug!("ES has {} took {} ms", hits.len(), es_took_ms);

    if hits.is_empty() {
        debug!("ES returned 0 hits. Raw response: {}", response_body);
    }

    let mut places_to_fetch = Vec::new();
    let mut scores = HashMap::new();

    for hit in hits {
        if let Some(id) = hit["_id"].as_str() {
            places_to_fetch.push(id.to_string());
            let score = hit["_score"].as_f64().unwrap_or(0.0);
            scores.insert(id.to_string(), score);
        }
        debug!("ES hit: {:?}", hit);
    }

    // Fetch from Scylla in parallel
    let start_scylla = std::time::Instant::now();
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
            } else {
                debug!(
                    "Failed to deserialize place from Scylla: {}",
                    places_to_fetch[i]
                );
            }
        } else {
            debug!(
                "Place not found in Scylla or error: {} (Result: {:?})",
                places_to_fetch[i], fetch_result
            );
        }
    }

    // Apply focus scoring in Rust
    if let (Some(lat), Some(lon)) = (params.focus_lat, params.focus_lon) {
        let focus_point = (lat, lon);

        for (place, score) in normalized_places.iter_mut() {
            let place_point = (place.center_point.lat, place.center_point.lon);
            let distance_km = haversine_distance_km(focus_point, place_point);

            // Decay function: 50km scale
            // factor = 1.0 / (1.0 + (distance / 50.0)^2)
            // This gives a nice bell curve shape, or we can use exponential
            // Let's use simple exponential decay like ES gauss: exp(- (dist^2) / (2 * scale^2))
            // scale = 50km
            // But we want to dampen this effect by importance.

            let scale = 50.0;
            let decay = (-(distance_km * distance_km) / (2.0 * scale * scale)).exp();

            let importance = place.importance.unwrap_or(0.0);

            // Interpolate between decay and 1.0 based on importance
            // If importance is 1.0, factor is 1.0 (no decay)
            // If importance is 0.0, factor is decay (full decay)
            let final_factor = decay + (1.0 - decay) * importance;

            *score *= final_factor;
        }

        // Re-sort results
        normalized_places
            .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    }

    // Batch fetch admin areas
    let admin_ids_vec: Vec<String> = admin_ids.into_iter().collect();
    let admin_map = scylla_client.get_admin_areas(&admin_ids_vec).await?;
    let scylla_took_ms = start_scylla.elapsed().as_millis();

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

    Ok(InternalTimedResults {
        places: results,
        es_took_ms,
        scylla_took_ms,
    })
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

    let place_rank = get_layer_rank(place.layer);

    let resolve_if_larger = |layer_variant: Layer, id: &Option<String>| {
        if get_layer_rank(layer_variant) > place_rank {
            resolve_admin_name(id, admin_map, preferred_lang)
        } else {
            None
        }
    };

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
            country: resolve_if_larger(Layer::Country, &place.parent.country),
            region: resolve_if_larger(Layer::Region, &place.parent.region),
            county: resolve_if_larger(Layer::County, &place.parent.county),
            locality: resolve_if_larger(Layer::Locality, &place.parent.locality),
            neighbourhood: resolve_if_larger(Layer::Neighbourhood, &place.parent.neighbourhood),
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

    let place_rank = get_layer_rank(place.layer);

    let resolve_if_larger = |layer_variant: Layer, id: &Option<String>| {
        if get_layer_rank(layer_variant) > place_rank {
            resolve_admin_name(id, admin_map, preferred_lang)
        } else {
            None
        }
    };

    let resolve_names_if_larger = |layer_variant: Layer, id: &Option<String>| {
        if get_layer_rank(layer_variant) > place_rank {
            resolve_admin_names(id, admin_map)
        } else {
            None
        }
    };

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
            country: resolve_if_larger(Layer::Country, &place.parent.country),
            country_names: resolve_names_if_larger(Layer::Country, &place.parent.country),
            region: resolve_if_larger(Layer::Region, &place.parent.region),
            region_names: resolve_names_if_larger(Layer::Region, &place.parent.region),
            county: resolve_if_larger(Layer::County, &place.parent.county),
            county_names: resolve_names_if_larger(Layer::County, &place.parent.county),
            locality: resolve_if_larger(Layer::Locality, &place.parent.locality),
            locality_names: resolve_names_if_larger(Layer::Locality, &place.parent.locality),
            neighbourhood: resolve_if_larger(Layer::Neighbourhood, &place.parent.neighbourhood),
            neighbourhood_names: resolve_names_if_larger(
                Layer::Neighbourhood,
                &place.parent.neighbourhood,
            ),
            categories: place.categories,
            confidence: score,
        },
    })
}

fn get_layer_rank(layer: Layer) -> u8 {
    match layer {
        Layer::Country => 100,
        Layer::MacroRegion => 90,
        Layer::Region => 80,
        Layer::MacroCounty => 70,
        Layer::County => 60,
        Layer::LocalAdmin => 50,
        Layer::Locality => 40,
        Layer::Borough => 30,
        Layer::Neighbourhood => 20,
        Layer::Street | Layer::Address | Layer::Venue => 10,
        Layer::Admin => 50, // Generic admin, treat as mid-level
    }
}

fn build_search_query(params: &SearchParams, autocomplete: bool) -> serde_json::Value {
    // Build function score for layer biasing

    // Core bool query
    let mut bool_query = json!({
        "must": [
            {
                "match": {
                    "name_all": {
                        "query": &params.text,
                        "fuzziness": "AUTO"
                    }
                }
            }
        ],
        "filter": []
    });

    // Add layer filter
    if let Some(ref layers) = params.layers {
        let filter_clause = json!({
            "terms": { "layer": layers }
        });

        if let Some(filter_arr) = bool_query["filter"].as_array_mut() {
            filter_arr.push(filter_clause);
        } else {
            bool_query["filter"] = json!([filter_clause]);
        }
    }

    let functions = json!([
    // Importance Score
    // Using field_value_factor to utilize the numeric 'importance' field
    {
        "field_value_factor": {
            "field": "importance",
            "missing": 0,
            // pivot/saturation logic from rank_feature is approximated
            // here or can be handled via script_score if precise curve is needed.
            // Simple modifier for now:
            "modifier": "sqrt",
            "factor": 1.0
        },
        "weight": 10.0
    }
    ]);

    json!({
        "query": {
            "function_score": {
                "query": { "bool": bool_query },
                "functions": functions,
                "score_mode": "sum",   // Sum the weights of the functions
                "boost_mode": "multiply" // Multiply the query score by the function score
            }
        },
        "size": params.size,
        "stored_fields": ["_id"]
    })
}

fn haversine_distance_km(p1: (f64, f64), p2: (f64, f64)) -> f64 {
    let (lat1, lon1) = p1;
    let (lat2, lon2) = p2;

    let r = 6371.0; // Earth radius in km

    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();

    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    r * c
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
    #[test]
    fn test_build_search_query() {
        let params = SearchParams {
            text: "Munchen".to_string(),
            lang: None,
            bbox: None,
            focus_lat: None,
            focus_lon: None,
            focus_weight: None,
            layers: None,
            size: 10,
        };

        let query_json = build_search_query(&params, false);
        let query_str = query_json.to_string();

        // Verify Rank Feature
        assert!(query_str.contains("rank_feature"));
        assert!(query_str.contains("importance"));
        assert!(query_str.contains("saturation"));

        // Verify Match Phrase Prefix
        assert!(query_str.contains("match_phrase_prefix"));
        assert!(query_str.contains("Munchen"));

        // Verify Layer Biasing
        assert!(query_str.contains("function_score"));
        assert!(query_str.contains("country"));
        assert!(query_str.contains("weight\":3.0")); // Country boost

        // Verify Category Biasing

        // Standard (1.5)
        assert!(query_str.contains("railway:station"));
        assert!(query_str.contains("weight\":1.5"));

        // High Priority (2.0)
        assert!(query_str.contains("railway:tram_stop"));
        assert!(query_str.contains("weight\":2.0"));
    }

    #[test]
    fn test_place_to_search_result_v2_hierarchy_filtering() {
        let mut names = HashMap::new();
        names.insert("default".to_string(), "Catalonia".to_string());

        let mut country_names = HashMap::new();
        country_names.insert("default".to_string(), "Spain".to_string());

        let mut county_names = HashMap::new();
        county_names.insert("default".to_string(), "Barcelona".to_string());

        let admin_map = HashMap::from([
            (
                "relation/1".to_string(),
                AdminEntry {
                    name: Some("Spain".to_string()),
                    names: country_names,
                    ..Default::default()
                },
            ),
            (
                "relation/3".to_string(),
                AdminEntry {
                    name: Some("Barcelona".to_string()),
                    names: county_names,
                    ..Default::default()
                },
            ),
        ]);

        let mut place = NormalizedPlace {
            source_id: "test:region".to_string(),
            source_file: "test.osm".to_string(),
            import_timestamp: chrono::Utc::now(),
            osm_type: OsmType::Relation,
            osm_id: 111,
            wikidata_id: None,
            importance: Some(1.0),
            layer: Layer::Region, // Rank 80
            categories: vec![],
            name: names,
            phrase: None,
            address: None,
            center_point: GeoPoint {
                lon: 2.0,
                lat: 41.0,
            },
            bbox: None,
            parent: AdminHierarchyIds::default(),
        };
        place.parent.country = Some("relation/1".to_string()); // Rank 100
        place.parent.county = Some("relation/3".to_string()); // Rank 60

        let result = place_to_search_result_v2(place, 1.0, &None, &admin_map).unwrap();

        // Country (Rank 100) > Region (Rank 80) -> Should be present
        assert_eq!(result.properties.country.as_deref(), Some("Spain"));

        // County (Rank 60) <= Region (Rank 80) -> Should be filtered out
        assert_eq!(result.properties.county, None);
    }
}
