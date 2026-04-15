//! Query server for geocoding searches.
//!
//! Provides HTTP API for forward and reverse geocoding with support for
//! bounding box bias, location bias, and multilingual results.

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

use cypress::elasticsearch::EsClient;
use cypress::scylla::ScyllaClient;

mod memdb;
mod search;
use memdb::Memdb;
use search::{execute_search, execute_search_v2, SearchParams, SearchResult, SearchResultV2};

#[derive(Parser, Debug)]
#[command(name = "query")]
#[command(about = "Geocoding query server")]
struct Args {
    /// Listen address
    #[arg(short, long, default_value = "0.0.0.0:3000")]
    listen: String,

    /// Elasticsearch URL
    #[arg(long, default_value = "http://localhost:9200")]
    es_url: String,

    /// Elasticsearch index name
    #[arg(long, default_value = "places")]
    index: String,

    /// ScyllaDB URL
    #[arg(long, default_value = "127.0.0.1")]
    scylla_url: String,

    /// Directory containing FST and memory-mapped files compiled by 'compiler'
    #[arg(long, default_value = "./data/compiled")]
    memdb_dir: String,
}

/// Application state shared across handlers
struct AppState {
    es_client: EsClient,
    scylla_client: ScyllaClient,
    memdb: Arc<Memdb>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();

    info!("Cypress Query Server");
    info!("Connecting to Elasticsearch at {}", args.es_url);

    // Connect to Elasticsearch
    let es_client = EsClient::new(&args.es_url, &args.index).await?;

    if !es_client.health_check().await? {
        anyhow::bail!("Elasticsearch cluster is not healthy");
    }

    let doc_count = es_client.doc_count().await?;
    info!(
        "Connected to index '{}' with {} documents",
        args.index, doc_count
    );

    // Connect to ScyllaDB
    info!("Connecting to ScyllaDB at {}", args.scylla_url);
    let scylla_client = ScyllaClient::new(&args.scylla_url).await?;

    info!("Initializing MemDB from {}", args.memdb_dir);
    let memdb = Memdb::new(&args.memdb_dir)?;

    let state = Arc::new(AppState {
        es_client,
        scylla_client,
        memdb,
    });

    // Build router
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

/// Health check endpoint
async fn health_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<HealthResponse>, StatusCode> {
    let healthy = state.es_client.health_check().await.unwrap_or(false);

    Ok(Json(HealthResponse {
        status: if healthy { "ok" } else { "degraded" },
        elasticsearch: healthy,
    }))
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    elasticsearch: bool,
}

/// Forward geocoding search
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

    let results = execute_search(&state.es_client, &state.scylla_client, search_params, false)
        .await
        .map_err(|e| {
            tracing::error!("Search execution failed: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    Ok(Json(SearchResponse {
        features: results.results,
        es_took_ms: results.es_took_ms,
        scylla_took_ms: results.scylla_took_ms,
    }))
}

/// Autocomplete endpoint (Tier 1 Hot Path)
async fn autocomplete_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchQueryParams>,
) -> Result<Json<AutocompleteResponse>, (StatusCode, String)> {
    let start = std::time::Instant::now();
    let text = params.text.to_lowercase();

    let mut features = Vec::new();

    if !text.is_empty() {
        let memdb_data = state.memdb.get_data();

        use fst::{Automaton, IntoStreamer};
        let prefix = fst::automaton::Str::new(&text).starts_with();
        let mut stream = memdb_data.map.search(prefix).into_stream();

        let size = params.size.unwrap_or(10).min(20);

        use fst::Streamer;

        struct ScoredFeature {
            feature: AutocompleteFeature,
            score: f64,
        }

        let mut scored_features = Vec::with_capacity(size);
        let mut count = 0;

        let focus_point =
            if let (Some(lat), Some(lon)) = (params.focus_point_lat, params.focus_point_lon) {
                Some((lat, lon))
            } else {
                None
            };
        let weight = params.focus_point_weight.unwrap_or(3.0);

        while let Some((_key, idx)) = stream.next() {
            if count >= 5000 {
                break;
            }
            count += 1;

            if let Some(summary) = state.memdb.get_summary(idx) {
                // parse zero-padded strings
                let parse_str = |b: &[u8]| {
                    let end = b.iter().position(|&x| x == 0).unwrap_or(b.len());
                    String::from_utf8_lossy(&b[..end]).into_owned()
                };

                let phrase_len = _key.iter().position(|&b| b == 0).unwrap_or(_key.len());
                let token_completeness = if phrase_len > 0 {
                    ((text.len() as f64) / (phrase_len as f64)).min(1.0)
                } else {
                    1.0
                };

                let importance = summary.importance as f64;

                let decay = if let Some(focus) = focus_point {
                    let distance_km = search::haversine_distance_km(
                        focus,
                        (summary.lat as f64, summary.lon as f64),
                    );
                    (-(distance_km * distance_km) / (2.0 * weight * weight)).exp()
                } else {
                    1.0
                };

                let final_score = token_completeness * importance * decay;

                scored_features.push(ScoredFeature {
                    feature: AutocompleteFeature {
                        result_type: "Feature".to_string(),
                        geometry: search::Geometry {
                            geo_type: "Point".to_string(),
                            coordinates: [summary.lon as f64, summary.lat as f64],
                        },
                        properties: AutocompleteProperties {
                            id: parse_str(&summary.source_id),
                            name: parse_str(&summary.name),
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
        features = scored_features
            .into_iter()
            .take(size)
            .map(|sf| sf.feature)
            .collect();
    }

    Ok(Json(AutocompleteResponse {
        features,
        memdb_took_ms: start.elapsed().as_millis(),
    }))
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
        Some(json_data) => {
            // Forward the raw JSON response directly from Scylla for maximum speed
            Ok(axum::response::Response::builder()
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(json_data))
                .unwrap())
        }
        None => Err(StatusCode::NOT_FOUND),
    }
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

    let results = execute_search_v2(&state.es_client, &state.scylla_client, search_params, false)
        .await
        .map_err(|e| {
            tracing::error!("Search V2 execution failed: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    Ok(Json(SearchResponseV2 {
        features: results.results,
        es_took_ms: results.es_took_ms,
        scylla_took_ms: results.scylla_took_ms,
    }))
}

/// Reverse geocoding
async fn reverse_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ReverseQueryParams>,
) -> Result<Json<SearchResponse>, (StatusCode, String)> {
    let results = search::execute_reverse(
        &state.es_client,
        &state.scylla_client,
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
        es_took_ms: 0,
        scylla_took_ms: 0,
    }))
}

/// Reverse geocoding V2
async fn reverse_v2_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ReverseQueryParams>,
) -> Result<Json<SearchResponseV2>, (StatusCode, String)> {
    let results = search::execute_reverse_v2(
        &state.es_client,
        &state.scylla_client,
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
        es_took_ms: 0,
        scylla_took_ms: 0,
    }))
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
    /// Focus point weight (defaults to 3.0)
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
    es_took_ms: u128,
    scylla_took_ms: u128,
}

#[derive(Serialize)]
struct SearchResponseV2 {
    features: Vec<SearchResultV2>,
    es_took_ms: u128,
    scylla_took_ms: u128,
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
