//! OSM PBF ingest pipeline.
//!
//! Parses OSM data, extracts places, performs PIP lookups,
//! and indexes into Elasticsearch.

mod batch;
mod config;
mod importance;
mod synonyms;
mod version;
mod way_merger;

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use geo::{BoundingRect, Centroid};
use indicatif::{ProgressBar, ProgressStyle};
use osmpbfreader::OsmPbfReader;
use tokio::sync::mpsc;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use cypress::discord::DiscordWebhook;
use cypress::elasticsearch::{create_index, BulkIndexer, EsClient};
use cypress::models::normalized::NormalizedPlace;
use cypress::models::{Address, AdminEntry, GeoBbox, GeoPoint, Layer, OsmType, Place};
use cypress::pip::{extract_admin_boundaries, AdminSpatialIndex, GeometryResolver, PipService};
use cypress::scylla::ScyllaClient;
use cypress::wikidata::WikidataFetcher;

use crate::importance::{calculate_default_importance, load_importance};
use crate::synonyms::SynonymService;
use crate::way_merger::WayMerger;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser, Debug)]
#[command(name = "ingest")]
#[command(about = "Ingest OSM PBF data into Elasticsearch")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Ingest a single PBF file
    Single(Args),
    /// Run batch ingest from config
    Batch {
        /// Path to TOML configuration file
        #[arg(long, default_value = "regions.toml")]
        config: PathBuf,

        /// Base arguments to apply to all regions (overridden by config where applicable)
        #[command(flatten)]
        args: Args,
    },
    /// Reset version history (forces re-import of all regions)
    ResetVersions {
        /// Elasticsearch URL
        #[arg(long, default_value = "http://localhost:9200")]
        es_url: String,
    },
}

#[derive(Parser, Debug, Clone)]
pub struct Args {
    /// OSM PBF file to import (required for single mode, ignored in batch mode)
    #[arg(short, long)]
    pub file: Option<PathBuf>,

    /// Optional pre-filtered admin boundary file
    #[arg(long)]
    pub admin_file: Option<PathBuf>,

    /// Elasticsearch URL
    #[arg(long, default_value = "http://localhost:9200")]
    pub es_url: String,

    /// Elasticsearch index name
    #[arg(long, default_value = "places")]
    pub index: String,

    /// ScyllaDB URL
    #[arg(long, default_value = "127.0.0.1")]
    pub scylla_url: String,

    /// Fetch supplemental labels from Wikidata
    #[arg(long)]
    pub wikidata: bool,

    /// Delete stale documents from previous import
    #[arg(long)]
    pub refresh: bool,

    /// Create/recreate index before import
    #[arg(long)]
    pub create_index: bool,

    /// Batch size for bulk indexing
    #[arg(long, default_value = "500")]
    pub batch_size: usize,

    /// Path to wikimedia-importance.csv (optional)
    #[arg(long)]
    pub importance_file: Option<PathBuf>,

    /// Discord webhook URL for notifications (optional)
    #[arg(long)]
    pub discord_webhook: Option<String>,

    /// Merge adjacent road ways with the same name to reduce disk space
    #[arg(long, default_value = "true")]
    pub merge_roads: bool,

    /// Force fresh download of PBF files even if they exist
    #[arg(long)]
    pub force_download: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    // Initialize synonym service
    let mut synonym_service = SynonymService::new();
    // Try to load from standard locations
    if Path::new("schema/synonyms").exists() {
        synonym_service.load_from_dir("schema/synonyms")?;
    } else if Path::new("../schema/synonyms").exists() {
        synonym_service.load_from_dir("../schema/synonyms")?;
    } else {
        warn!("Could not find schema/synonyms directory. Running without synonyms.");
    }
    let synonym_service = Arc::new(synonym_service);

    match cli.command {
        Commands::Single(args) => run_single(args, synonym_service).await,
        Commands::Batch { config, args } => {
            // TODO: Pass Scylla URL to batch if needed, currently batch likely calls run_single or similar logic.
            // Check batch.rs content. For now we assume batch uses its own config or args.
            // Actually batch usually calls run_single logic or re-implements it.
            // Let's check batch.rs but for now I'll just update Single.
            // Wait, I should probably check batch.rs.
            // But let's proceed with run_single first as per plan.
            batch::run_batch(config, args, synonym_service).await
        }
        Commands::ResetVersions { es_url } => run_reset(&es_url).await,
    }
}

pub async fn run_reset(es_url: &str) -> Result<()> {
    use crate::version::VersionManager;

    info!("Connecting to Elasticsearch at {}...", es_url);
    let version_manager = VersionManager::new(es_url).await?;

    info!("Resetting version history...");
    version_manager.reset().await?;

    info!("Version history reset complete.");
    Ok(())
}

pub async fn run_single(args: Args, synonyms: Arc<SynonymService>) -> Result<()> {
    let file_path = args
        .file
        .clone()
        .ok_or_else(|| anyhow::anyhow!("Input file is required for single import"))?;

    info!("Cypress Ingest Pipeline");
    info!("File: {}", file_path.display());

    // Connect to Elasticsearch
    let es_client = EsClient::new(&args.es_url, &args.index)
        .await
        .context("Failed to connect to Elasticsearch")?;

    if !es_client.health_check().await? {
        anyhow::bail!("Elasticsearch cluster is not healthy");
    }
    info!("Connected to Elasticsearch");

    // Connect to ScyllaDB
    let scylla_client = ScyllaClient::new(&args.scylla_url).await?;
    info!("Connected to ScyllaDB");
    let scylla_client = Arc::new(scylla_client);

    // Initialize Discord webhook
    let discord = args
        .discord_webhook
        .as_ref()
        .map(|url| DiscordWebhook::new(url.clone()));

    // Get source file name for tracking
    let source_file = file_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown.osm.pbf")
        .to_string();

    if let Some(ref dw) = discord {
        let _ = dw
            .send_notification(
                "Ingestion Started",
                &format!("Starting ingestion for: **{}**", source_file),
                true,
            )
            .await;
    }

    // Create index if requested
    if args.create_index {
        create_index(&es_client, true).await?;
    }

    let import_start = Utc::now();

    // Load importance data
    let importance_map = if let Some(path) = &args.importance_file {
        Some(load_importance(path)?)
    } else if Path::new("wikimedia-importance.csv").exists() {
        Some(load_importance(Path::new("wikimedia-importance.csv"))?)
    } else {
        warn!("No importance file found. Skipping importance ranking.");
        None
    };

    // Open PBF file
    // Build GeometryResolver(s)
    let (admin_resolver, _place_resolver_source) = if let Some(admin_path) = &args.admin_file {
        info!(
            "Building admin geometry index from: {}",
            admin_path.display()
        );
        let file = File::open(admin_path).context("Failed to open admin PBF file")?;
        let mut reader = OsmPbfReader::new(BufReader::new(file));
        let resolver = GeometryResolver::build(&mut reader, |_| true)?;
        (resolver, None)
    } else {
        // Use main file for both
        info!("Building geometry index from main file...");
        let file = File::open(&file_path).context("Failed to open PBF file")?;
        let mut reader = OsmPbfReader::new(BufReader::new(file));
        let resolver =
            GeometryResolver::build(&mut reader, |tags| determine_layer(tags).is_some())?;
        (resolver, Some(&file_path))
    };

    if let Some(ref dw) = discord {
        let _ = dw
            .send_notification(
                "Geometry Index Built",
                &format!("Geometry index building complete for: **{}**", source_file),
                true,
            )
            .await;
    }

    // Extract admin boundaries using admin_resolver
    // Create spatial index immediately to avoid holding Vec<AdminBoundary>
    let spatial_index = {
        let path = args.admin_file.as_ref().unwrap_or(&file_path);
        info!("Extracting admin boundaries from: {}", path.display());
        let file = File::open(path)?;
        let mut reader = OsmPbfReader::new(BufReader::new(file));
        let boundaries = extract_admin_boundaries(&mut reader, &admin_resolver)?;

        if let Some(ref dw) = discord {
            let _ = dw
                .send_notification(
                    "Admin Boundaries Extracted",
                    &format!(
                        "Extracted **{}** admin boundaries for: **{}**",
                        boundaries.len(),
                        source_file
                    ),
                    true,
                )
                .await;
        }

        AdminSpatialIndex::build(boundaries)
    };

    let pip_service = Arc::new(PipService::new(spatial_index));
    let spatial_index_ref = pip_service.index(); // Access underlying index

    info!(
        "PIP service ready with {} boundaries",
        spatial_index_ref.len()
    );

    // Initialize Wikidata fetcher if enabled
    let wikidata = if args.wikidata {
        Some(WikidataFetcher::new())
    } else {
        None
    };

    // Prepare resolver for places
    let place_resolver = if args.admin_file.is_some() {
        info!("Building place geometry index from main file...");
        let file = File::open(&file_path)?;
        let mut reader = OsmPbfReader::new(BufReader::new(file));
        Arc::new(GeometryResolver::build(&mut reader, |tags| {
            determine_layer(tags).is_some()
        })?)
    } else {
        Arc::new(admin_resolver)
    };

    // Re-open file for place extraction (count first)
    // Note: Counting is expensive on large files, maybe skip?
    // User code had it, we'll keep it but it adds a pass.
    let file = File::open(&file_path)?;
    let mut reader = OsmPbfReader::new(BufReader::new(file));

    info!("Counting objects...");
    let mut total_count = 0u64;
    for obj in reader.iter() {
        if obj.is_ok() {
            total_count += 1;
        }
    }
    info!("Total OSM objects: {}", total_count);

    // Re-open for processing
    let file = File::open(&file_path)?;
    let mut reader = OsmPbfReader::new(BufReader::new(file));

    // Create progress bar
    let pb = ProgressBar::new(total_count);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({per_sec})",
            )?
            .progress_chars("#>-"),
    );

    // Create bulk indexer (starts background task)
    let indexer = BulkIndexer::new(es_client.clone(), args.batch_size);

    // Create pipeline channel
    let (tx, rx) = mpsc::channel::<Place>(2000);

    // Spawn processing pipeline
    let pipeline_handle = tokio::spawn(run_processing_pipeline(
        rx,
        wikidata,
        scylla_client.clone(),
        indexer.sender_clone(),
        args.batch_size,
    ));

    // Index Admin Boundaries first
    info!(
        "Indexing {} administrative boundaries...",
        spatial_index_ref.len()
    );

    // Use iterator from spatial_index to avoid looking at "boundaries" Vec (which is gone)
    for boundary in spatial_index_ref.boundaries() {
        let center = boundary.geometry.centroid().map(|p| GeoPoint {
            lat: p.y(),
            lon: p.x(),
        });
        let bbox = boundary
            .bbox()
            .map(|(min_x, min_y, max_x, max_y)| GeoBbox::new(min_x, min_y, max_x, max_y));

        if let Some(center) = center {
            let mut place = Place::new(
                OsmType::Relation,
                boundary.area.osm_id,
                Layer::Admin, // Use Admin layer
                center,
                &source_file,
            );
            place.name = boundary.area.name.clone();
            place.wikidata_id = boundary.area.wikidata_id.clone();
            place.bbox = bbox;

            // PIP lookup for admin hierarchy (limit to higher levels)
            let hierarchy = pip_service.lookup(
                place.center_point.lon,
                place.center_point.lat,
                Some(boundary.area.level),
            );
            place.parent = hierarchy;

            // Assign importance if available
            if let Some(ref map) = importance_map {
                if let Some(ref qid) = place.wikidata_id {
                    place.importance = map.get(qid).copied();
                }
            }

            place.sanitize();

            if let Err(_) = tx.send(place).await {
                error!("Pipeline receiver dropped encountered during admin indexing");
                break;
            }
        }
    }

    info!("Processing OSM objects...");

    // First pass: collect road ways for merging if enabled
    let mut way_merger = if args.merge_roads {
        Some(WayMerger::new(Arc::clone(&place_resolver)))
    } else {
        None
    };

    // Process each OSM object
    for obj_result in reader.iter() {
        pb.inc(1);

        let obj = match obj_result {
            Ok(o) => o,
            Err(e) => {
                warn!("Error reading OSM object: {}", e);
                continue;
            }
        };

        // If merging enabled and this is a road way, collect it
        if let Some(ref mut merger) = way_merger {
            if let osmpbfreader::OsmObj::Way(ref way) = obj {
                if is_road_way(&way.tags) {
                    // Normalize name in tags before adding to merger
                    // This improves merger grouping
                    let mut tags = way.tags.clone();
                    if let Some(name) = tags.get("name") {
                        let normalized = synonyms.normalize(name);
                        tags.insert("name".into(), normalized.into());
                    }
                    merger.add_road(way.id, tags, way.nodes.iter().map(|n| n.0).collect());
                    continue; // Don't process this way now
                }
            }
        }

        // Try to extract a place from this object (non-roads or when merging disabled)
        if let Some(mut place) = extract_place(&obj, &source_file, &place_resolver, &synonyms)? {
            // PIP lookup for admin hierarchy
            let hierarchy =
                pip_service.lookup(place.center_point.lon, place.center_point.lat, None);
            place.parent = hierarchy;

            // Collect Wikidata ID is redundant here as we moved it to process_batch,
            // BUT we still need to assign importance if we have it locally?
            // Or can we move importance lookup to pipeline too?
            // Importance map is available here. Pipeline doesn't have it.
            // Let's keep importance assignment here.

            if let Some(ref qid) = place.wikidata_id {
                if let Some(ref map) = importance_map {
                    if let Some(score) = map.get(qid) {
                        place.importance = Some(*score);
                    }
                }
            }

            place.sanitize();

            if let Err(_) = tx.send(place).await {
                error!("Pipeline receiver dropped");
                break;
            }
        }
    }

    pb.finish_with_message("Processing complete");

    // Process merged roads if enabled
    if let Some(merger) = way_merger {
        info!("Processing merged roads...");
        let merged_roads = merger.merge();

        for merged_road in merged_roads {
            if let Some(mut place) = merged_road.to_place(&source_file) {
                // Extract tags
                extract_tags(&mut place, &merged_road.tags, &synonyms);

                // Calculate importance
                place.importance = Some(calculate_default_importance(&merged_road.tags));

                // PIP lookup for admin hierarchy
                let hierarchy =
                    pip_service.lookup(place.center_point.lon, place.center_point.lat, None);
                place.parent = hierarchy;

                if let Some(ref qid) = place.wikidata_id {
                    if let Some(ref map) = importance_map {
                        if let Some(score) = map.get(qid) {
                            place.importance = Some(*score);
                        }
                    }
                }

                place.sanitize();

                if let Err(_) = tx.send(place).await {
                    error!("Pipeline receiver dropped during merged roads");
                    break;
                }
            }
        }
    }

    // Close channel by dropping sender
    drop(tx);

    // Wait for pipeline to finish
    if let Err(e) = pipeline_handle.await {
        error!("Pipeline task failed: {}", e);
    }

    // Finish indexing
    let (indexed, errors) = indexer.finish().await?;

    info!("Indexed {} documents ({} errors)", indexed, errors);

    // Refresh: delete stale documents
    if args.refresh {
        info!("Deleting stale documents from previous import...");
        delete_stale_documents(&es_client, &source_file, import_start).await?;
    }

    // Final stats
    let doc_count = es_client.doc_count().await?;
    info!("Total documents in index: {}", doc_count);

    if let Some(ref dw) = discord {
        let _ = dw.send_notification(
            "Ingestion Complete",
            &format!("Successfully indexed **{}** documents (with **{}** errors) for **{}**.\nTotal documents in index: **{}**", indexed, errors, source_file, doc_count),
            true
        ).await;
    }

    Ok(())
}

async fn run_processing_pipeline(
    mut rx: mpsc::Receiver<Place>,
    wikidata: Option<WikidataFetcher>,
    scylla: Arc<ScyllaClient>,
    indexer_tx: mpsc::Sender<Place>,
    batch_size: usize,
) {
    let mut buffer = Vec::with_capacity(batch_size);

    while let Some(place) = rx.recv().await {
        buffer.push(place);

        if buffer.len() >= batch_size {
            if let Err(e) = process_buffer(&mut buffer, &wikidata, &scylla, &indexer_tx).await {
                error!("Error processing batch: {}", e);
            }
            buffer.clear();
        }
    }

    // Process remaining
    if !buffer.is_empty() {
        if let Err(e) = process_buffer(&mut buffer, &wikidata, &scylla, &indexer_tx).await {
            error!("Error processing final batch: {}", e);
        }
    }
}

async fn process_buffer(
    places: &mut [Place],
    wikidata: &Option<WikidataFetcher>,
    scylla: &ScyllaClient,
    indexer_tx: &mpsc::Sender<Place>,
) -> Result<()> {
    // 1. Fetch Wikidata
    if let Some(wd) = wikidata {
        let qids: Vec<String> = places
            .iter()
            .filter_map(|p| p.wikidata_id.clone())
            .collect();

        if !qids.is_empty() {
            wd.fetch_batch(&qids).await?;

            for place in places.iter_mut() {
                if let Some(ref qid) = place.wikidata_id {
                    wd.merge_labels(qid, &mut place.name);
                }
            }
        }
    }

    // 2. Scylla Upsert & Indexer Send
    for place in places.iter() {
        // Upsert Admin Areas (Scylla)
        upsert_admin_areas(place, scylla).await?;

        // Upsert Normalized Place (Scylla)
        let normalized = NormalizedPlace::from_place(place.clone());
        let json_data = serde_json::to_string(&normalized)?;
        scylla
            .upsert_place(&normalized.source_id, &json_data)
            .await?;

        // Send to Indexer
        indexer_tx.send(place.clone()).await?;
    }

    Ok(())
}

/// Check if an OSM way is a road that should be considered for merging
fn is_road_way(tags: &osmpbfreader::Tags) -> bool {
    if let Some(highway) = tags.get("highway") {
        // Only roads with names can be merged
        if !tags.contains_key("name") {
            return false;
        }

        // Check if this is a street-type highway (not motorways or links)
        matches!(
            highway.as_str(),
            "residential"
                | "primary"
                | "secondary"
                | "tertiary"
                | "unclassified"
                | "service"
                | "living_street"
                | "pedestrian"
                | "track"
                | "road"
                | "footway"
                | "cycleway"
                | "path"
        )
    } else {
        false
    }
}

/// Extract a Place from an OSM object if it's relevant
fn extract_place(
    obj: &osmpbfreader::OsmObj,
    source_file: &str,
    resolver: &Arc<GeometryResolver>,
    synonyms: &SynonymService,
) -> Result<Option<Place>> {
    use osmpbfreader::OsmObj;

    match obj {
        OsmObj::Node(node) => {
            if let Some(layer) = determine_layer(&node.tags) {
                let center = GeoPoint {
                    lat: node.lat(),
                    lon: node.lon(),
                };
                let mut place = Place::new(OsmType::Node, node.id.0, layer, center, source_file);
                place.importance = Some(calculate_default_importance(&node.tags));
                extract_tags(&mut place, &node.tags, synonyms);
                Ok(Some(place))
            } else {
                Ok(None)
            }
        }
        OsmObj::Way(way) => {
            if let Some(layer) = determine_layer(&way.tags) {
                // Resolve geometry
                if let Some((lon, lat)) = resolver.resolve_centroid(way.id) {
                    let center = GeoPoint { lat, lon };
                    let mut place = Place::new(OsmType::Way, way.id.0, layer, center, source_file);
                    place.importance = Some(calculate_default_importance(&way.tags));
                    extract_tags(&mut place, &way.tags, synonyms);

                    // Optional: Add Bbox
                    if let Some(poly) = resolver.resolve_way(way.id) {
                        if let Some(rect) = poly.bounding_rect() {
                            place.bbox = Some(GeoBbox::new(
                                rect.min().x,
                                rect.min().y,
                                rect.max().x,
                                rect.max().y,
                            ));
                        }
                    }

                    Ok(Some(place))
                } else {
                    // warn!("Could not resolve geometry for way {}", way.id.0);
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        }
        OsmObj::Relation(rel) => {
            // Check layers/relevance
            if let Some(layer) = determine_layer(&rel.tags) {
                // Skip admin boundaries (handled separately)
                if layer == Layer::Admin {
                    return Ok(None);
                }

                // Resolve multipolygon geometry
                if let Some(multi_poly) = resolver.resolve_relation(rel.id) {
                    // Calculate centroid
                    if let Some(centroid) = multi_poly.centroid() {
                        let center = GeoPoint {
                            lat: centroid.y(),
                            lon: centroid.x(),
                        };

                        let mut place =
                            Place::new(OsmType::Relation, rel.id.0, layer, center, source_file);
                        place.importance = Some(calculate_default_importance(&rel.tags));
                        extract_tags(&mut place, &rel.tags, synonyms);

                        // Calculate Bbox
                        if let Some(rect) = multi_poly.bounding_rect() {
                            place.bbox = Some(GeoBbox::new(
                                rect.min().x,
                                rect.min().y,
                                rect.max().x,
                                rect.max().y,
                            ));
                        }

                        Ok(Some(place))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        }
    }
}

// Skipping determine_layer...

/// Extract all relevant tags from OSM object
fn extract_tags(place: &mut Place, tags: &osmpbfreader::Tags, synonyms: &SynonymService) {
    for (key, value) in tags.iter() {
        let key_str = key.as_str();

        // Names
        if key_str == "name" {
            place.add_name("default", synonyms.normalize(value));
        } else if let Some(lang) = key_str.strip_prefix("name:") {
            if is_valid_lang_code(lang) {
                place.add_name(lang, value.to_string());
            }
        } else {
            // Check for alternate names
            let name_variants = [
                "alt_name",
                "old_name",
                "official_name",
                "short_name",
                "int_name",
                "nat_name",
                "reg_name",
                "loc_name",
            ];

            for variant in &name_variants {
                if key_str == *variant {
                    place.add_name(variant, value.to_string());
                    break;
                } else if let Some(suffix) = key_str
                    .strip_prefix(variant)
                    .and_then(|s| s.strip_prefix(':'))
                {
                    if is_valid_lang_code(suffix) {
                        place.add_name(key_str, value.to_string());
                    }
                    break;
                }
            }
        }

        // Wikidata
        if key_str == "wikidata" || key_str == "brand:wikidata" {
            place.wikidata_id = Some(value.to_string());
        }
        // Address components
        else if key_str == "addr:housenumber" {
            place
                .address
                .get_or_insert_with(Address::default)
                .housenumber = Some(value.to_string());
        } else if key_str == "addr:street" {
            place.address.get_or_insert_with(Address::default).street =
                Some(synonyms.normalize(value));
        } else if key_str == "addr:postcode" {
            place.address.get_or_insert_with(Address::default).postcode = Some(value.to_string());
        } else if key_str == "addr:city" {
            place.address.get_or_insert_with(Address::default).city =
                Some(synonyms.normalize(value));
        }
        // Categories (POI types)
        else if [
            "amenity", "shop", "tourism", "leisure", "cuisine", "building", "historic", "office",
        ]
        .contains(&key_str)
        {
            place.add_category(key_str, value);
        }
    }
}

fn is_valid_lang_code(lang: &str) -> bool {
    // Basic check for 2-3 letter codes or simple region codes
    lang.len() >= 2 && lang.len() <= 10 && lang.chars().all(|c| c.is_alphabetic() || c == '-')
}

fn determine_layer(tags: &osmpbfreader::Tags) -> Option<Layer> {
    if let Some(place) = tags.get("place") {
        match place.as_str() {
            "country" | "state" | "region" | "province" | "district" | "county"
            | "municipality" | "city" | "town" | "village" | "hamlet" | "borough" | "suburb"
            | "quarter" | "neighbourhood" => Some(Layer::Admin), // Or specific layer
            "island" | "archipelago" => Some(Layer::Admin), // Treat as admin for now
            _ => None,
        }
    } else if let Some(admin_level) = tags.get("admin_level") {
        // Only if boundary=administrative
        if tags.contains("boundary", "administrative") {
            Some(Layer::Admin)
        } else {
            None
        }
    } else {
        // Venues/Addresses
        if tags.contains_key("addr:housenumber") && tags.contains_key("addr:street") {
            Some(Layer::Address)
        } else if tags.contains_key("amenity")
            || tags.contains_key("shop")
            || tags.contains_key("tourism")
            || tags.contains_key("leisure")
        {
            Some(Layer::Venue)
        } else {
            None
        }
    }
}

async fn upsert_admin_areas(place: &Place, scylla: &ScyllaClient) -> Result<()> {
    let parents = [
        place.parent.country.as_ref(),
        place.parent.macro_region.as_ref(),
        place.parent.region.as_ref(),
        place.parent.macro_county.as_ref(),
        place.parent.county.as_ref(),
        place.parent.local_admin.as_ref(),
        place.parent.locality.as_ref(),
        place.parent.borough.as_ref(),
        place.parent.neighbourhood.as_ref(),
    ];

    for parent in parents.iter().flatten() {
        if let Some(id) = parent.id {
            let source_id = format!("relation/{}", id);
            let json_data = serde_json::to_string(parent)?;
            scylla.upsert_admin_area(&source_id, &json_data).await?;
        }
    }
    Ok(())
}

/// Delete documents from previous import of the same file
async fn delete_stale_documents(
    client: &EsClient,
    source_file: &str,
    import_start: chrono::DateTime<Utc>,
) -> Result<()> {
    let query = serde_json::json!({
        "query": {
            "bool": {
                "must": [
                    { "term": { "source_file": source_file } }
                ],
                "filter": [
                    { "range": { "import_timestamp": { "lt": import_start.to_rfc3339() } } }
                ]
            }
        }
    });

    let response = client
        .client()
        .delete_by_query(elasticsearch::DeleteByQueryParts::Index(&[
            &client.index_name
        ]))
        .body(query)
        .send()
        .await?;

    let body = response.json::<serde_json::Value>().await?;
    let deleted = body["deleted"].as_u64().unwrap_or(0);

    info!("Deleted {} stale documents", deleted);

    Ok(())
}
