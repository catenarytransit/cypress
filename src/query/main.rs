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

mod search;
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
}

/// Application state shared across handlers
struct AppState {
    es_client: EsClient,
    scylla_client: ScyllaClient,
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

    let state = Arc::new(AppState {
        es_client,
        scylla_client,
    });

    // Build router
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/v1/search", get(search_handler))
        .route("/v2/search", get(search_v2_handler))
        .route("/v1/reverse", get(reverse_handler))
        .route("/v1/autocomplete", get(autocomplete_handler))
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

/// Autocomplete endpoint (uses edge n-grams)
async fn autocomplete_handler(
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
        size: params.size.unwrap_or(10).min(20),
    };

    let results = execute_search(&state.es_client, &state.scylla_client, search_params, true)
        .await
        .map_err(|e| {
            tracing::error!("Autocomplete execution failed: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    Ok(Json(SearchResponse {
        features: results.results,
        es_took_ms: results.es_took_ms,
        scylla_took_ms: results.scylla_took_ms,
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
