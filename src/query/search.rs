use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

use cypress::models::memdb::{ArchivedCypressMemDb, CypressMemDb};
use cypress::models::normalized::NormalizedPlace;
use cypress::models::place::Layer;
use cypress::models::AdminEntry;
use cypress::scylla::ScyllaClient;
use regex::Regex;
use std::sync::OnceLock;

use super::memdb::{CosSimMatch, Memdb, GUESS_CONTEXT};

#[derive(Clone)]
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

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResult {
    #[serde(rename = "type")]
    pub result_type: String,
    pub geometry: Geometry,
    pub properties: Properties,
}

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
    pub took_ms: u128,
}

pub struct TimedSearchResultsV2 {
    pub results: Vec<SearchResultV2>,
    pub took_ms: u128,
}

/// Run the bigram cosine similarity search over the rkyv database,
/// returning scored source IDs with their final scores.
fn bigram_search(
    memdb: &Memdb,
    text: &str,
    focus: Option<(f64, f64)>,
    focus_weight: f64,
    bbox: Option<[f64; 4]>,
    max_results: usize,
) -> Vec<(String, f64)> {
    let query = text.trim().to_lowercase();
    if query.len() < 2 {
        return Vec::new();
    }

    let memdb_data = memdb.get_data();
    let db = memdb_data.get_archived();
    let string_count = db.string_bigram_counts.len();

    let mut results = Vec::new();

    GUESS_CONTEXT.with(|cell| {
        let mut ctx = cell.borrow_mut();
        ctx.clear(string_count);

        let query_bytes = query.as_bytes();
        let mut query_bigrams: u32 = 0;

        for window in query_bytes.windows(2) {
            let bigram_key = ((window[0] as u16) << 8) | (window[1] as u16);
            let idx = bigram_key as usize;
            let start = db.bigram_offsets[idx] as usize;
            let end = db.bigram_offsets[idx + 1] as usize;
            for i in start..end {
                let string_idx = db.bigram_data[i] as usize;
                if string_idx < ctx.string_match_counts.len() {
                    ctx.string_match_counts[string_idx] += 1;
                }
            }
            query_bigrams += 1;
        }

        if query_bigrams == 0 {
            return;
        }

        let min_match_count = (2 + query_bigrams / (4 + query_bigrams / 10)) as u16;

        let count_len = ctx.string_match_counts.len();
        for string_idx in 0..count_len {
            let match_count = ctx.string_match_counts[string_idx];
            if match_count < min_match_count {
                continue;
            }
            let str_bigram_count = db.string_bigram_counts[string_idx] as f32;
            if str_bigram_count == 0.0 {
                continue;
            }
            let cos_sim = (match_count as f32 * match_count as f32)
                / (str_bigram_count * query_bigrams as f32);
            if cos_sim >= 0.17 {
                ctx.string_matches.push(CosSimMatch {
                    string_idx: string_idx as u32,
                    cos_sim,
                });
            }
        }

        let max_matches = 6000.min(ctx.string_matches.len());
        if ctx.string_matches.len() > max_matches {
            ctx.string_matches
                .select_nth_unstable_by(max_matches, |a, b| {
                    b.cos_sim
                        .partial_cmp(&a.cos_sim)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            ctx.string_matches.truncate(max_matches);
        }
        ctx.string_matches.sort_unstable_by(|a, b| {
            b.cos_sim
                .partial_cmp(&a.cos_sim)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        struct ScoredPlace {
            source_id: String,
            score: f64,
        }

        let mut scored = Vec::new();

        for text_match in &ctx.string_matches {
            let sid = text_match.string_idx as usize;
            let p_start = db.string_to_places_offsets[sid] as usize;
            let p_end = db.string_to_places_offsets[sid + 1] as usize;

            for i in p_start..p_end {
                let place_id = db.string_to_places_data[i] as usize;
                let place = &db.places[place_id];
                let lat = place.lat as f64;
                let lon = place.lon as f64;

                if let Some(bb) = bbox {
                    if lon < bb[0] || lat < bb[1] || lon > bb[2] || lat > bb[3] {
                        continue;
                    }
                }

                let importance = place.importance as f64;
                let text_score = text_match.cos_sim as f64;

                let decay = if let Some(fp) = focus {
                    let dist = haversine_distance_km(fp, (lat, lon));
                    (-(dist * dist) / (2.0 * focus_weight * focus_weight)).exp()
                } else {
                    1.0
                };

                let final_score = text_score * (1.0 + importance * 10.0) * decay;

                let source_id =
                    String::from_utf8_lossy(&place.source_id_bytes[..place.source_id_len as usize])
                        .into_owned();

                scored.push(ScoredPlace {
                    source_id,
                    score: final_score,
                });
            }
        }

        scored.sort_unstable_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut seen = std::collections::HashSet::new();
        results = scored
            .into_iter()
            .filter(|s| seen.insert(s.source_id.clone()))
            .take(max_results)
            .map(|s| (s.source_id, s.score))
            .collect();
    });

    results
}

/// Use the spatial grid for reverse geocoding — find places near a coordinate.
fn spatial_search(
    memdb: &Memdb,
    lon: f64,
    lat: f64,
    max_results: usize,
    radius_km: f64,
) -> Vec<(String, f64)> {
    let memdb_data = memdb.get_data();
    let db = memdb_data.get_archived();

    let delta_deg = (radius_km / 111.0) as f32;
    let min_lat = (lat as f32 - delta_deg).max(-90.0);
    let max_lat = (lat as f32 + delta_deg).min(89.999);
    let min_lon = (lon as f32 - delta_deg).max(-180.0);
    let max_lon = (lon as f32 + delta_deg).min(179.999);

    let cell_size = CypressMemDb::GRID_CELL_SIZE;
    let cols = CypressMemDb::GRID_COLS;

    let row_start = ((min_lat + 90.0) / cell_size) as usize;
    let row_end = ((max_lat + 90.0) / cell_size) as usize;
    let col_start = ((min_lon + 180.0) / cell_size) as usize;
    let col_end = ((max_lon + 180.0) / cell_size) as usize;

    let mut candidates: Vec<(String, f64)> = Vec::new();

    for row in row_start..=row_end {
        for col in col_start..=col_end {
            let cell_id = (row * cols + col) as u32;

            // Binary search for this cell in the sorted active_cells array
            if let Ok(pos) = db.active_cells.binary_search_by(|c| c.cmp(&cell_id)) {
                let start = db.cell_offsets[pos] as usize;
                let end = db.cell_offsets[pos + 1] as usize;

                for i in start..end {
                    let place_id = db.cell_places[i] as usize;
                    let place = &db.places[place_id];

                    let dist =
                        haversine_distance_km((lat, lon), (place.lat as f64, place.lon as f64));
                    if dist <= radius_km {
                        let source_id = String::from_utf8_lossy(
                            &place.source_id_bytes[..place.source_id_len as usize],
                        )
                        .into_owned();
                        candidates.push((source_id, dist));
                    }
                }
            }
        }
    }

    candidates.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    candidates.truncate(max_results);
    candidates
}

pub async fn execute_search(
    scylla_client: &ScyllaClient,
    memdb: &Arc<Memdb>,
    params: SearchParams,
) -> Result<TimedSearchResults> {
    let start = std::time::Instant::now();

    let focus = match (params.focus_lat, params.focus_lon) {
        (Some(lat), Some(lon)) => Some((lat, lon)),
        _ => None,
    };
    let focus_weight = params.focus_weight.unwrap_or(50.0);

    let scored_ids = bigram_search(
        memdb,
        &params.text,
        focus,
        focus_weight,
        params.bbox,
        params.size,
    );

    let mut results = Vec::new();
    let mut admin_ids = std::collections::HashSet::new();

    let fetch_futures = scored_ids.iter().map(|(id, _)| scylla_client.get_place(id));
    let fetched = futures::future::join_all(fetch_futures).await;

    let mut places_with_scores = Vec::new();
    for (i, fetch_result) in fetched.into_iter().enumerate() {
        if let Ok(Some(json_data)) = fetch_result {
            if let Ok(place) = serde_json::from_str::<NormalizedPlace>(&json_data) {
                collect_admin_ids(&place, &mut admin_ids);
                places_with_scores.push((place, scored_ids[i].1));
            }
        }
    }

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

    for (place, score) in places_with_scores {
        if let Some(result) = place_to_search_result(place, score, &params.lang, &parsed_admin_map)
        {
            results.push(result);
        }
    }

    Ok(TimedSearchResults {
        results,
        took_ms: start.elapsed().as_millis(),
    })
}

pub async fn execute_search_v2(
    scylla_client: &ScyllaClient,
    memdb: &Arc<Memdb>,
    params: SearchParams,
) -> Result<TimedSearchResultsV2> {
    let start = std::time::Instant::now();

    let focus = match (params.focus_lat, params.focus_lon) {
        (Some(lat), Some(lon)) => Some((lat, lon)),
        _ => None,
    };
    let focus_weight = params.focus_weight.unwrap_or(50.0);

    let scored_ids = bigram_search(
        memdb,
        &params.text,
        focus,
        focus_weight,
        params.bbox,
        params.size,
    );

    let mut admin_ids = std::collections::HashSet::new();

    let fetch_futures = scored_ids.iter().map(|(id, _)| scylla_client.get_place(id));
    let fetched = futures::future::join_all(fetch_futures).await;

    let mut places_with_scores = Vec::new();
    for (i, fetch_result) in fetched.into_iter().enumerate() {
        if let Ok(Some(json_data)) = fetch_result {
            if let Ok(place) = serde_json::from_str::<NormalizedPlace>(&json_data) {
                collect_admin_ids(&place, &mut admin_ids);
                places_with_scores.push((place, scored_ids[i].1));
            }
        }
    }

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
    for (place, score) in places_with_scores {
        if let Some(result) =
            place_to_search_result_v2(place, score, &params.lang, &parsed_admin_map)
        {
            results.push(result);
        }
    }

    Ok(TimedSearchResultsV2 {
        results,
        took_ms: start.elapsed().as_millis(),
    })
}

pub async fn execute_reverse(
    scylla_client: &ScyllaClient,
    memdb: &Arc<Memdb>,
    lon: f64,
    lat: f64,
    size: usize,
    _layers: Option<Vec<String>>,
) -> Result<Vec<SearchResult>> {
    let nearby = spatial_search(memdb, lon, lat, size, 1.0);

    let fetch_futures = nearby.iter().map(|(id, _)| scylla_client.get_place(id));
    let fetched = futures::future::join_all(fetch_futures).await;

    let mut admin_ids = std::collections::HashSet::new();
    let mut normalized_places = Vec::new();

    for fetch_result in fetched {
        if let Ok(Some(json_data)) = fetch_result {
            if let Ok(place) = serde_json::from_str::<NormalizedPlace>(&json_data) {
                collect_admin_ids(&place, &mut admin_ids);
                normalized_places.push(place);
            }
        }
    }

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

pub async fn execute_reverse_v2(
    scylla_client: &ScyllaClient,
    memdb: &Arc<Memdb>,
    lon: f64,
    lat: f64,
    size: usize,
    _layers: Option<Vec<String>>,
    lang: Option<String>,
) -> Result<Vec<SearchResultV2>> {
    let nearby = spatial_search(memdb, lon, lat, size, 1.0);

    let fetch_futures = nearby.iter().map(|(id, _)| scylla_client.get_place(id));
    let fetched = futures::future::join_all(fetch_futures).await;

    let mut admin_ids = std::collections::HashSet::new();
    let mut normalized_places = Vec::new();

    for fetch_result in fetched {
        if let Ok(Some(json_data)) = fetch_result {
            if let Ok(place) = serde_json::from_str::<NormalizedPlace>(&json_data) {
                collect_admin_ids(&place, &mut admin_ids);
                normalized_places.push(place);
            }
        }
    }

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
        if let Some(result) = place_to_search_result_v2(place, 1.0, &lang, &parsed_admin_map) {
            results.push(result);
        }
    }

    Ok(results)
}

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

fn place_to_search_result(
    place: NormalizedPlace,
    score: f64,
    preferred_lang: &Option<String>,
    admin_map: &HashMap<String, AdminEntry>,
) -> Option<SearchResult> {
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
            layer: format!("{:?}", place.layer).to_lowercase(),
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

fn place_to_search_result_v2(
    place: NormalizedPlace,
    score: f64,
    preferred_lang: &Option<String>,
    admin_map: &HashMap<String, AdminEntry>,
) -> Option<SearchResultV2> {
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
        Layer::Admin => 50,
    }
}

pub fn haversine_distance_km(p1: (f64, f64), p2: (f64, f64)) -> f64 {
    let (lat1, lon1) = p1;
    let (lat2, lon2) = p2;

    let r = 6371.0;

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

        assert!(result.properties.country_names.is_some());
        let c_names = result.properties.country_names.unwrap();
        assert_eq!(c_names.get("default").unwrap(), "United Kingdom");
        assert_eq!(c_names.get("de").unwrap(), "Vereinigtes Königreich");
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
            layer: Layer::Region,
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
        place.parent.country = Some("relation/1".to_string());
        place.parent.county = Some("relation/3".to_string());

        let result = place_to_search_result_v2(place, 1.0, &None, &admin_map).unwrap();

        // Country (Rank 100) > Region (Rank 80) -> Should be present
        assert_eq!(result.properties.country.as_deref(), Some("Spain"));

        // County (Rank 60) <= Region (Rank 80) -> Should be filtered out
        assert_eq!(result.properties.county, None);
    }
}
