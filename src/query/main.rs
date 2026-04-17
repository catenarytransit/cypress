//! Query server for geocoding searches.
//!
//! All search and autocomplete endpoints use the zero-copy rkyv memory-mapped
//! bigram inverted index. Elasticsearch is not required.

use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use cypress::scylla::ScyllaClient;

mod memdb;
mod search;
use memdb::{CosSimMatch, GuessContext, Memdb, GUESS_CONTEXT};
use search::{SearchParams, SearchResult, SearchResultV2};

#[derive(Parser, Debug)]
#[command(name = "query")]
#[command(about = "Geocoding query server")]
struct Args {
    /// Listen address
    #[arg(short, long, default_value = "0.0.0.0:3000")]
    listen: String,

    /// ScyllaDB URL
    #[arg(long, default_value = "127.0.0.1")]
    scylla_url: String,

    /// Directory containing rkyv memory-mapped files compiled by 'compiler'
    #[arg(long, default_value = "./data/compiled")]
    memdb_dir: String,
}

/// Application state shared across handlers
struct AppState {
    scylla_client: ScyllaClient,
    memdb: Arc<Memdb>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();

    info!("Cypress Query Server");

    info!("Connecting to ScyllaDB at {}", args.scylla_url);
    let scylla_client = ScyllaClient::new(&args.scylla_url).await?;

    info!("Initializing MemDB from {}", args.memdb_dir);
    let memdb = Memdb::new(&args.memdb_dir)?;

    let state = Arc::new(AppState {
        scylla_client,
        memdb,
    });

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/v1/search", get(search_handler))
        .route("/v2/search", get(search_v2_handler))
        .route("/v1/reverse", get(reverse_handler))
        .route("/v2/reverse", get(reverse_v2_handler))
        .route("/v1/autocomplete", get(autocomplete_handler))
        .route("/v1/place/details", get(place_details_handler))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    info!("Starting server on {}", args.listen);

    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_handler(
    State(_state): State<Arc<AppState>>,
) -> Result<Json<HealthResponse>, StatusCode> {
    Ok(Json(HealthResponse { status: "ok" }))
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

/// Autocomplete endpoint — Bigram Cosine Similarity hot path
async fn autocomplete_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchQueryParams>,
) -> Result<Json<AutocompleteResponse>, (StatusCode, String)> {
    let start = std::time::Instant::now();
    let text = params.text.trim().to_lowercase();

    let mut features = Vec::new();

    if text.len() >= 2 {
        let memdb_data = state.memdb.get_data();
        let db = memdb_data.get_archived();

        let size = params.size.unwrap_or(10).min(20);

        let focus_point =
            if let (Some(lat), Some(lon)) = (params.focus_point_lat, params.focus_point_lon) {
                Some((lat, lon))
            } else {
                None
            };
        let weight = params.focus_point_weight.unwrap_or(50.0);

        let string_count = db.string_bigram_counts.len();

        GUESS_CONTEXT.with(|cell| {
            let mut ctx = cell.borrow_mut();
            ctx.clear(string_count);

            let query_bytes = text.as_bytes();
            let mut query_bigrams: u32 = 0;

            // 1. Accumulate bigram match counts
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

            // 2. Compute Cosine Similarity
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

            // 3. Partial sort and truncate
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

            // 4. Entity resolution: map string matches to places
            struct ScoredFeature {
                feature: AutocompleteFeature,
                score: f64,
            }

            let mut scored_features = Vec::with_capacity(size);

            for text_match in &ctx.string_matches {
                let sid = text_match.string_idx as usize;
                let p_start = db.string_to_places_offsets[sid] as usize;
                let p_end = db.string_to_places_offsets[sid + 1] as usize;

                for i in p_start..p_end {
                    let place_id = db.string_to_places_data[i] as usize;
                    let place = &db.places[place_id];

                    let importance = place.importance as f64;
                    let text_score = text_match.cos_sim as f64;

                    let decay = if let Some(focus) = focus_point {
                        let distance_km = search::haversine_distance_km(
                            focus,
                            (place.lat as f64, place.lon as f64),
                        );
                        (-(distance_km * distance_km) / (2.0 * weight * weight)).exp()
                    } else {
                        1.0
                    };

                    let final_score = text_score * (1.0 + importance * 10.0) * decay;

                    let name =
                        String::from_utf8_lossy(&place.name_bytes[..place.name_len as usize])
                            .into_owned();
                    let source_id = String::from_utf8_lossy(
                        &place.source_id_bytes[..place.source_id_len as usize],
                    )
                    .into_owned();

                    scored_features.push(ScoredFeature {
                        feature: AutocompleteFeature {
                            result_type: "Feature".to_string(),
                            geometry: search::Geometry {
                                geo_type: "Point".to_string(),
                                coordinates: [place.lon as f64, place.lat as f64],
                            },
                            properties: AutocompleteProperties {
                                id: source_id,
                                name,
                            },
                        },
                        score: final_score,
                    });
                }
            }

            scored_features.sort_unstable_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            // Deduplicate by source_id, keeping highest score
            let mut seen = std::collections::HashSet::new();
            features = scored_features
                .into_iter()
                .filter(|sf| seen.insert(sf.feature.properties.id.clone()))
                .take(size)
                .map(|sf| sf.feature)
                .collect();
        });
    }

    Ok(Json(AutocompleteResponse {
        features,
        memdb_took_ms: start.elapsed().as_millis(),
    }))
}

/// Forward geocoding search — delegates to bigram engine then hydrates from Scylla
async fn search_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchQueryParams>,
) -> Result<Json<SearchResponse>, (StatusCode, String)> {
    let search_params = SearchParams {
        text: params.text.clone(),
        lang: params.lang.clone(),
        bbox: parse_bbox(&params.bbox),
        focus_lat: params.focus_point_lat,
        focus_lon: params.focus_point_lon,
        focus_weight: params.focus_point_weight,
        layers: params
            .layers
            .as_ref()
            .map(|l| l.split(',').map(String::from).collect()),
        size: params.size.unwrap_or(10).min(40),
    };

    let results = search::execute_search(&state.scylla_client, &state.memdb, search_params)
        .await
        .map_err(|e| {
            tracing::error!("Search execution failed: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    Ok(Json(SearchResponse {
        features: results.results,
        took_ms: results.took_ms,
    }))
}

/// Forward geocoding search V2
async fn search_v2_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchQueryParams>,
) -> Result<Json<SearchResponseV2>, (StatusCode, String)> {
    let search_params = SearchParams {
        text: params.text.clone(),
        lang: params.lang.clone(),
        bbox: parse_bbox(&params.bbox),
        focus_lat: params.focus_point_lat,
        focus_lon: params.focus_point_lon,
        focus_weight: params.focus_point_weight,
        layers: params
            .layers
            .as_ref()
            .map(|l| l.split(',').map(String::from).collect()),
        size: params.size.unwrap_or(10).min(40),
    };

    let results = search::execute_search_v2(&state.scylla_client, &state.memdb, search_params)
        .await
        .map_err(|e| {
            tracing::error!("Search V2 execution failed: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    Ok(Json(SearchResponseV2 {
        features: results.results,
        took_ms: results.took_ms,
    }))
}

/// Reverse geocoding — uses spatial grid
async fn reverse_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ReverseQueryParams>,
) -> Result<Json<SearchResponse>, (StatusCode, String)> {
    let results = search::execute_reverse(
        &state.scylla_client,
        &state.memdb,
        params.point_lon,
        params.point_lat,
        params.size.unwrap_or(10).min(40),
        params
            .layers
            .as_ref()
            .map(|l| l.split(',').map(String::from).collect()),
    )
    .await
    .map_err(|e| {
        tracing::error!("Reverse geocoding failed: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;

    Ok(Json(SearchResponse {
        features: results,
        took_ms: 0,
    }))
}

/// Reverse geocoding V2
async fn reverse_v2_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ReverseQueryParams>,
) -> Result<Json<SearchResponseV2>, (StatusCode, String)> {
    let results = search::execute_reverse_v2(
        &state.scylla_client,
        &state.memdb,
        params.point_lon,
        params.point_lat,
        params.size.unwrap_or(10).min(40),
        params
            .layers
            .as_ref()
            .map(|l| l.split(',').map(String::from).collect()),
        params.lang.clone(),
    )
    .await
    .map_err(|e| {
        tracing::error!("Reverse geocoding V2 failed: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;

    Ok(Json(SearchResponseV2 {
        features: results,
        took_ms: 0,
    }))
}

async fn place_details_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PlaceDetailsQueryParams>,
) -> Result<axum::response::Response, StatusCode> {
    let place_json = state
        .scylla_client
        .get_place(&params.id)
        .await
        .map_err(|e| {
            tracing::error!("ScyllaDB query failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    match place_json {
        Some(json_data) => Ok(axum::response::Response::builder()
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(json_data))
            .unwrap()),
        None => Err(StatusCode::NOT_FOUND),
    }
}

#[derive(Serialize)]
struct AutocompleteResponse {
    features: Vec<AutocompleteFeature>,
    memdb_took_ms: u128,
}

#[derive(Serialize)]
struct AutocompleteFeature {
    #[serde(rename = "type")]
    result_type: String,
    geometry: search::Geometry,
    properties: AutocompleteProperties,
}

#[derive(Serialize)]
struct AutocompleteProperties {
    id: String,
    name: String,
}

#[derive(Deserialize)]
struct PlaceDetailsQueryParams {
    id: String,
}

#[derive(Deserialize)]
struct SearchQueryParams {
    /// Search text
    text: String,
    /// Preferred language for results
    lang: Option<String>,
    /// Bounding box: "minLon,minLat,maxLon,maxLat"
    bbox: Option<String>,
    /// Focus point latitude
    #[serde(rename = "focus.point.lat")]
    focus_point_lat: Option<f64>,
    /// Focus point longitude
    #[serde(rename = "focus.point.lon")]
    focus_point_lon: Option<f64>,
    /// Focus point weight (defaults to 50.0)
    #[serde(rename = "focus.point.weight")]
    focus_point_weight: Option<f64>,
    /// Filter by layers (comma-separated)
    layers: Option<String>,
    /// Number of results
    size: Option<usize>,
}

#[derive(Deserialize)]
struct ReverseQueryParams {
    /// Point longitude
    #[serde(rename = "point.lon")]
    point_lon: f64,
    /// Point latitude
    #[serde(rename = "point.lat")]
    point_lat: f64,
    /// Preferred language for results
    lang: Option<String>,
    /// Filter by layers (comma-separated)
    layers: Option<String>,
    /// Number of results
    size: Option<usize>,
}

#[derive(Serialize)]
struct SearchResponse {
    features: Vec<SearchResult>,
    took_ms: u128,
}

#[derive(Serialize)]
struct SearchResponseV2 {
    features: Vec<SearchResultV2>,
    took_ms: u128,
}

/// Parse bbox string "minLon,minLat,maxLon,maxLat"
fn parse_bbox(bbox: &Option<String>) -> Option<[f64; 4]> {
    bbox.as_ref().and_then(|s| {
        let parts: Vec<f64> = s.split(',').filter_map(|p| p.trim().parse().ok()).collect();
        if parts.len() == 4 {
            Some([parts[0], parts[1], parts[2], parts[3]])
        } else {
            None
        }
    })
}
