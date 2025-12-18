//! OSM PBF ingest pipeline.
//!
//! Parses OSM data, extracts places, performs PIP lookups,
//! and indexes into Elasticsearch.

use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use osmpbfreader::OsmPbfReader;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use cypress::elasticsearch::{create_index, BulkIndexer, EsClient};
use cypress::models::{Address, GeoPoint, Layer, OsmType, Place};
use cypress::pip::{extract_admin_boundaries, AdminSpatialIndex, PipService};
use cypress::wikidata::WikidataFetcher;

#[derive(Parser, Debug)]
#[command(name = "ingest")]
#[command(about = "Ingest OSM PBF data into Elasticsearch")]
struct Args {
    /// OSM PBF file to import
    #[arg(short, long)]
    file: PathBuf,

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

    // Create index if requested
    if args.create_index {
        create_index(&es_client, true).await?;
    }

    // Get source file name for tracking
    let source_file = args
        .file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown.osm.pbf")
        .to_string();

    let import_start = Utc::now();

    // Open PBF file
    let file = File::open(&args.file).context("Failed to open PBF file")?;
    let mut reader = OsmPbfReader::new(BufReader::new(file));

    // Extract admin boundaries and build PIP service
    info!("Building admin boundary index...");
    let boundaries = extract_admin_boundaries(&mut reader)?;
    let spatial_index = AdminSpatialIndex::build(boundaries);
    let pip_service = Arc::new(PipService::new(spatial_index));

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

    // Re-open file for place extraction
    let file = File::open(&args.file)?;
    let mut reader = OsmPbfReader::new(BufReader::new(file));

    // First pass: count objects for progress
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
        if let Some(mut place) = extract_place(&obj, &source_file)? {
            // PIP lookup for admin hierarchy
            let hierarchy = pip_service.lookup(place.center_point.lon, place.center_point.lat);
            place.parent = hierarchy;

            // Collect Wikidata ID if present
            if let Some(ref qid) = place.wikidata_id {
                wikidata_ids.push(qid.clone());
            }

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

    Ok(())
}

/// Extract a Place from an OSM object if it's relevant
fn extract_place(obj: &osmpbfreader::OsmObj, source_file: &str) -> Result<Option<Place>> {
    use osmpbfreader::OsmObj;

    match obj {
        OsmObj::Node(node) => {
            // Check if this node is interesting (has relevant tags)
            let tags = &node.tags;

            // Must have a name to be searchable
            if !tags.contains_key("name") {
                return Ok(None);
            }

            // Determine layer from tags
            let layer = determine_layer(tags);
            if layer.is_none() {
                return Ok(None);
            }

            let center = GeoPoint {
                lat: node.lat(),
                lon: node.lon(),
            };

            let mut place = Place::new(
                OsmType::Node,
                node.id.0,
                layer.unwrap(),
                center,
                source_file,
            );

            // Extract names and other tags
            extract_tags(&mut place, tags);

            Ok(Some(place))
        }
        OsmObj::Way(_way) => {
            // Ways need coordinate lookup - skip for now, handle in full pipeline
            // For now, we focus on nodes
            Ok(None)
        }
        OsmObj::Relation(_rel) => {
            // Relations are complex - mainly admin boundaries handled separately
            Ok(None)
        }
    }
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

    // Check for address
    if tags.contains_key("addr:housenumber") && tags.contains_key("addr:street") {
        return Some(Layer::Address);
    }

    // Check for various POI types
    let poi_keys = [
        "amenity", "shop", "tourism", "leisure", "office", "building",
    ];
    for key in &poi_keys {
        if tags.contains_key(*key) {
            return Some(Layer::Venue);
        }
    }

    // Check for highway (streets)
    if tags
        .get("highway")
        .map(|v| v == "residential" || v == "primary" || v == "secondary")
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
            place.add_name(lang, value.to_string());
        }
        // Wikidata
        else if key_str == "wikidata" {
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
            "amenity", "shop", "tourism", "leisure", "cuisine", "building",
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
