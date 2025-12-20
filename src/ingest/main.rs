//! OSM PBF ingest pipeline.
//!
//! Parses OSM data, extracts places, performs PIP lookups,
//! and indexes into Elasticsearch.

mod importance;

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use geo::{BoundingRect, Centroid};
use indicatif::{ProgressBar, ProgressStyle};
use osmpbfreader::OsmPbfReader;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use cypress::discord::DiscordWebhook;
use cypress::elasticsearch::{create_index, BulkIndexer, EsClient};
use cypress::models::{Address, GeoBbox, GeoPoint, Layer, OsmType, Place};
use cypress::pip::{extract_admin_boundaries, AdminSpatialIndex, GeometryResolver, PipService};
use cypress::wikidata::WikidataFetcher;

use crate::importance::{calculate_default_importance, load_importance};

#[derive(Parser, Debug)]
#[command(name = "ingest")]
#[command(about = "Ingest OSM PBF data into Elasticsearch")]
struct Args {
    /// OSM PBF file to import
    #[arg(short, long)]
    file: PathBuf,

    /// Optional pre-filtered admin boundary file
    #[arg(long)]
    admin_file: Option<PathBuf>,

    /// Elasticsearch URL
    #[arg(long, default_value = "http://localhost:9200")]
    es_url: String,

    /// Elasticsearch index name
    #[arg(long, default_value = "places")]
    index: String,

    /// Fetch supplemental labels from Wikidata
    #[arg(long)]
    wikidata: bool,

    /// Delete stale documents from previous import
    #[arg(long)]
    refresh: bool,

    /// Create/recreate index before import
    #[arg(long)]
    create_index: bool,

    /// Batch size for bulk indexing
    #[arg(long, default_value = "5000")]
    batch_size: usize,

    /// Path to wikimedia-importance.csv (optional)
    #[arg(long)]
    importance_file: Option<PathBuf>,

    /// Discord webhook URL for notifications (optional)
    #[arg(long)]
    discord_webhook: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();

    info!("Cypress Ingest Pipeline");
    info!("File: {}", args.file.display());

    // Connect to Elasticsearch
    let es_client = EsClient::new(&args.es_url, &args.index)
        .await
        .context("Failed to connect to Elasticsearch")?;

    if !es_client.health_check().await? {
        anyhow::bail!("Elasticsearch cluster is not healthy");
    }
    info!("Connected to Elasticsearch");

    // Initialize Discord webhook
    let discord = args
        .discord_webhook
        .as_ref()
        .map(|url| DiscordWebhook::new(url.clone()));

    // Get source file name for tracking
    let source_file = args
        .file
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
        // No filter needed for pre-filtered file, or ensure we get relations/ways
        let resolver = GeometryResolver::build(&mut reader, |_| true)?;
        (resolver, None)
    } else {
        // Use main file for both
        info!("Building geometry index from main file...");
        let file = File::open(&args.file).context("Failed to open PBF file")?;
        let mut reader = OsmPbfReader::new(BufReader::new(file));
        let resolver =
            GeometryResolver::build(&mut reader, |tags| determine_layer(tags).is_some())?;
        // Clone/re-use strategy: GeometryResolver is read-only mostly, but expensive to clone if it owns DB?
        // Actually GeometryResolver uses sled::Db which is cloneable (handles).
        // BUT we need a separate one for places if we didn't build the admin one from the main file.
        // If we build admin from SMALL file, we can't use it for places (missing nodes).
        // So we need a second build for places later.
        (resolver, Some(&args.file))
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
    let boundaries = {
        let path = args.admin_file.as_ref().unwrap_or(&args.file);
        info!("Extracting admin boundaries from: {}", path.display());
        let file = File::open(path)?;
        let mut reader = OsmPbfReader::new(BufReader::new(file));
        extract_admin_boundaries(&mut reader, &admin_resolver)?
    };
    let spatial_index = AdminSpatialIndex::build(boundaries.clone());
    let pip_service = Arc::new(PipService::new(spatial_index));

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

    info!(
        "PIP service ready with {} boundaries",
        pip_service.index().len()
    );

    // Initialize Wikidata fetcher if enabled
    let mut wikidata = if args.wikidata {
        Some(WikidataFetcher::new())
    } else {
        None
    };

    // Prepare resolver for places
    // If we have an admin file, admin_resolver is ONLY for admins. We need a new one for places from MAIN file.
    // If we don't, admin_resolver IS the place resolver (built from main file).
    let place_resolver = if args.admin_file.is_some() {
        info!("Building place geometry index from main file...");
        let file = File::open(&args.file)?;
        let mut reader = OsmPbfReader::new(BufReader::new(file));
        GeometryResolver::build(&mut reader, |tags| determine_layer(tags).is_some())?
    } else {
        // We can reuse the admin_resolver since it WAS built from main file
        // But wait, in the `else` block above we returned `(resolver, Some(&args.file))`
        // actually `admin_resolver` ignores the second tuple item in this scope?
        // Let's refine the logic.
        // GeometryResolver contains a sled::Db. We can clone it?
        // Struct definition: struct GeometryResolver { node_db: Db, ... }
        // Db is thread-safe and cloneable.
        // But we shouldn't re-build it if we don't have to.
        // The simplistic approach above works: `admin_resolver` is used, we just need to alias it or clone it.
        // However, Rust ownership.
        // Let's make `place_resolver` a reference or move it?
        // We need `admin_resolver` for the PIP service? No, `admin_resolver` was used to Extract Boundaries.
        // The PIP service uses `boundaries` (Vec<AdminBoundary>).
        // So `admin_resolver` is DONE after extraction.
        // So we can move it or drop it.
        // If we didn't have admin file, `admin_resolver` should be reused as `place_resolver`.
        admin_resolver
    };

    // Re-open file for place extraction (count first)
    // Note: Counting is expensive on large files, maybe skip?
    // User code had it, we'll keep it but it adds a pass.
    let file = File::open(&args.file)?;
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
    let file = File::open(&args.file)?;
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

    // Create bulk indexer
    let mut indexer = BulkIndexer::new(es_client.clone(), args.batch_size);

    // Collect Wikidata IDs for batch fetching
    let mut wikidata_ids: Vec<String> = Vec::new();
    let mut places_buffer: Vec<Place> = Vec::new();

    // Index Admin Boundaries first
    info!("Indexing {} administrative boundaries...", boundaries.len());
    for boundary in boundaries {
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
            place.name = boundary.area.name;
            place.wikidata_id = boundary.area.wikidata_id;
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

            // We could also do PIP lookup for parent, but admin boundaries ARE the parents.
            // Usually admins form a hierarchy.
            // Ideally we index them appropriately.

            place.sanitize();
            indexer.add(place).await?;
        }
    }

    info!("Processing OSM objects...");

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

        // Try to extract a place from this object
        if let Some(mut place) = extract_place(&obj, &source_file, &place_resolver)? {
            // PIP lookup for admin hierarchy
            let hierarchy =
                pip_service.lookup(place.center_point.lon, place.center_point.lat, None);
            place.parent = hierarchy;

            // Collect Wikidata ID if present
            if let Some(ref qid) = place.wikidata_id {
                wikidata_ids.push(qid.clone());

                // Assign importance
                if let Some(ref map) = importance_map {
                    if let Some(score) = map.get(qid) {
                        place.importance = Some(*score);
                    }
                }
            }

            place.sanitize();
            places_buffer.push(place);

            // Batch Wikidata fetch every 1000 places
            if args.wikidata && wikidata_ids.len() >= 1000 {
                if let Some(ref mut wd) = wikidata {
                    wd.fetch_batch(&wikidata_ids).await?;

                    // Merge labels into buffered places
                    for p in &mut places_buffer {
                        if let Some(ref qid) = p.wikidata_id {
                            wd.merge_labels(qid, &mut p.name);
                        }
                    }
                }
                wikidata_ids.clear();
            }

            // Index when buffer is full
            if places_buffer.len() >= args.batch_size {
                for p in places_buffer.drain(..) {
                    indexer.add(p).await?;
                }
            }
        }
    }

    pb.finish_with_message("Processing complete");

    // Fetch remaining Wikidata labels
    if args.wikidata && !wikidata_ids.is_empty() {
        if let Some(ref mut wd) = wikidata {
            wd.fetch_batch(&wikidata_ids).await?;
            for p in &mut places_buffer {
                if let Some(ref qid) = p.wikidata_id {
                    wd.merge_labels(qid, &mut p.name);
                }
            }
        }
    }

    // Index remaining places
    for p in places_buffer {
        indexer.add(p).await?;
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

/// Extract a Place from an OSM object if it's relevant
fn extract_place(
    obj: &osmpbfreader::OsmObj,
    source_file: &str,
    resolver: &GeometryResolver,
) -> Result<Option<Place>> {
    use osmpbfreader::OsmObj;

    match obj {
        OsmObj::Node(node) => {
            if !has_relevant_tags(&node.tags) {
                return Ok(None);
            }
            if let Some(layer) = determine_layer(&node.tags) {
                let center = GeoPoint {
                    lat: node.lat(),
                    lon: node.lon(),
                };
                let mut place = Place::new(OsmType::Node, node.id.0, layer, center, source_file);
                place.importance = Some(calculate_default_importance(&node.tags));
                extract_tags(&mut place, &node.tags);
                Ok(Some(place))
            } else {
                Ok(None)
            }
        }
        OsmObj::Way(way) => {
            if !has_relevant_tags(&way.tags) {
                return Ok(None);
            }
            if let Some(layer) = determine_layer(&way.tags) {
                // Resolve geometry
                if let Some((lon, lat)) = resolver.resolve_centroid(way.id) {
                    let center = GeoPoint { lat, lon };
                    let mut place = Place::new(OsmType::Way, way.id.0, layer, center, source_file);
                    place.importance = Some(calculate_default_importance(&way.tags));
                    extract_tags(&mut place, &way.tags);

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
        OsmObj::Relation(_rel) => {
            // Relation handling is complex (multipolygon).
            // We handled Admin boundaries separately.
            // Generic multipolygons (POIs) can be handled if we extend GeometryResolver.
            // For now, we skip non-admin relations.
            Ok(None)
        }
    }
}

fn has_relevant_tags(tags: &osmpbfreader::Tags) -> bool {
    tags.contains_key("name")
}

/// Determine the layer/type from OSM tags
fn determine_layer(tags: &osmpbfreader::Tags) -> Option<Layer> {
    // Check for place tag first
    if let Some(place_type) = tags.get("place") {
        return match place_type.as_str() {
            "country" => Some(Layer::Country),
            "state" | "province" | "region" => Some(Layer::Region),
            "city" | "town" | "village" | "hamlet" => Some(Layer::Locality),
            "suburb" | "neighbourhood" | "quarter" => Some(Layer::Neighbourhood),
            _ => Some(Layer::Venue),
        };
    }

    // Check for boundary=administrative
    if tags
        .get("boundary")
        .map(|v| v == "administrative")
        .unwrap_or(false)
    {
        return Some(Layer::Admin);
    }

    // Check for address
    if tags.contains_key("addr:housenumber") && tags.contains_key("addr:street") {
        return Some(Layer::Address);
    }

    // Check for various POI types
    // Expanded list
    let poi_keys = [
        "amenity", "shop", "tourism", "leisure", "office", "building", "historic", "craft",
    ];
    for key in &poi_keys {
        if tags.contains_key(*key) {
            return Some(Layer::Venue);
        }
    }

    // Check for highway (streets)
    if tags
        .get("highway")
        .map(|v| v == "residential" || v == "primary" || v == "secondary" || v == "tertiary")
        .unwrap_or(false)
    {
        return Some(Layer::Street);
    }

    None
}

/// Extract all relevant tags from OSM object
fn extract_tags(place: &mut Place, tags: &osmpbfreader::Tags) {
    for (key, value) in tags.iter() {
        let key_str = key.as_str();

        // Names
        if key_str == "name" {
            place.add_name("default", value.to_string());
        } else if let Some(lang) = key_str.strip_prefix("name:") {
            // Validate language code to prevent field explosion (ES limit 1000)
            // Allow 2-10 chars, alphanumeric + -_
            if lang.len() >= 2
                && lang.len() <= 10
                && lang
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
            {
                place.add_name(lang, value.to_string());
            }
        }
        // Wikidata
        else if key_str == "wikidata" || key_str == "brand:wikidata" {
            place.wikidata_id = Some(value.to_string());
        }
        // Address components
        else if key_str == "addr:housenumber" {
            place
                .address
                .get_or_insert_with(Address::default)
                .housenumber = Some(value.to_string());
        } else if key_str == "addr:street" {
            place.address.get_or_insert_with(Address::default).street = Some(value.to_string());
        } else if key_str == "addr:postcode" {
            place.address.get_or_insert_with(Address::default).postcode = Some(value.to_string());
        } else if key_str == "addr:city" {
            place.address.get_or_insert_with(Address::default).city = Some(value.to_string());
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
