use anyhow::{Context, Result};
use clap::Parser;
use futures::stream::{self, StreamExt};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use cypress::models::memdb::{CypressMemDb, PlaceRecord};
use cypress::scylla::ScyllaClient;
use rkyv::ser::{serializers::AllocSerializer, Serializer};

#[derive(Parser, Debug)]
#[command(name = "compiler")]
#[command(
    about = "Compiles ScyllaDB places into zero-copy rkyv memory maps using parallel token-range scanning"
)]
struct Args {
    /// ScyllaDB URL
    #[arg(long, default_value = "127.0.0.1")]
    scylla_url: String,

    /// Output directory for compiled files
    #[arg(short, long, default_value = "./data/compiled")]
    out_dir: String,

    /// Number of parallel workers for token scanning
    #[arg(short, long, default_value = "32")]
    concurrency: usize,

    /// Max retries per token range
    #[arg(long, default_value = "5")]
    max_retries: u32,
}

fn get_layer_rank(layer: cypress::models::place::Layer) -> u8 {
    match layer {
        cypress::models::place::Layer::Country => 9,
        cypress::models::place::Layer::MacroRegion => 8,
        cypress::models::place::Layer::Region => 7,
        cypress::models::place::Layer::MacroCounty => 6,
        cypress::models::place::Layer::County => 5,
        cypress::models::place::Layer::LocalAdmin => 4,
        cypress::models::place::Layer::Locality => 3,
        cypress::models::place::Layer::Borough => 2,
        cypress::models::place::Layer::Neighbourhood => 1,
        _ => 0,
    }
}

/// Shared state to accumulate data from parallel workers
struct GlobalCompilationState {
    places: Vec<PlaceRecord>,
    string_id_map: HashMap<String, u32>,
    string_to_places_temp: Vec<Vec<u32>>,
    string_bigram_counts: Vec<u8>,
    bigrams_temp: Vec<Vec<u32>>,
    spatial_map: HashMap<u32, Vec<u32>>,
    processed_ranges: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();
    let out_dir = Path::new(&args.out_dir);
    fs::create_dir_all(out_dir).context("Failed to create output directory")?;

    info!("Connecting to ScyllaDB at {}", args.scylla_url);
    let scylla_client = ScyllaClient::new(&args.scylla_url).await?;
    let session = &scylla_client.session;

    // Retrieve the token ring from the ReplicaLocator
    let cluster_state = session.get_cluster_state();
    let ring = cluster_state.replica_locator().ring();

    // Extract and sort unique tokens to define range boundaries
    let mut tokens: Vec<i64> = ring.iter().map(|(t, _)| t.value()).collect();
    tokens.sort_unstable();
    tokens.dedup();

    let mut ranges = Vec::new();
    if tokens.is_empty() {
        // Fallback for single-node setups without explicit vnodes
        ranges.push((i64::MIN, i64::MAX));
    } else {
        // Create ranges: (MIN, T0), (T0, T1) ... (TN, MAX)
        ranges.push((i64::MIN, tokens[0]));
        for i in 0..tokens.len() - 1 {
            ranges.push((tokens[i], tokens[i + 1]));
        }
        ranges.push((tokens[tokens.len() - 1], i64::MAX));
    }

    let total_ranges = ranges.len();
    info!(
        "Generated {} token ranges from cluster metadata",
        total_ranges
    );

    let state = Arc::new(Mutex::new(GlobalCompilationState {
        places: Vec::new(),
        string_id_map: HashMap::new(),
        string_to_places_temp: Vec::new(),
        string_bigram_counts: Vec::new(),
        bigrams_temp: vec![Vec::new(); 65536],
        spatial_map: HashMap::new(),
        processed_ranges: 0,
    }));

    let scylla_client_ref = Arc::new(scylla_client);

    stream::iter(ranges)
        .for_each_concurrent(args.concurrency, |(start, end)| {
            let state = Arc::clone(&state);
            let client = Arc::clone(&scylla_client_ref);
            let max_retries = args.max_retries;

            async move {
                let mut attempt = 0;
                let query = format!(
                    "SELECT data FROM cypress.places WHERE token(id) >= {} AND token(id) < {}",
                    start, end
                );

                loop {
                    match client.session.query_iter(query.clone(), &[]).await {
                        Ok(it) => {
                            let mut rows = match it.rows_stream::<(String,)>() {
                                Ok(s) => s,
                                Err(e) => {
                                    error!("Type check error in range [{}, {}]: {}", start, end, e);
                                    break;
                                }
                            };

                            let mut local_results = Vec::new();

                            while let Some(row_result) = rows.next().await {
                                let (data,) = match row_result {
                                    Ok(r) => r,
                                    Err(e) => {
                                        error!(
                                            "Row fetch error in range [{}, {}]: {}",
                                            start, end, e
                                        );
                                        break;
                                    }
                                };

                                if let Ok(place) = serde_json::from_str::<
                                    cypress::models::normalized::NormalizedPlace,
                                >(&data)
                                {
                                    let display_name = place
                                        .phrase
                                        .clone()
                                        .or_else(|| place.name.get("default").cloned())
                                        .or_else(|| place.name.values().next().cloned())
                                        .unwrap_or_default();

                                    let mut record = PlaceRecord {
                                        source_id_bytes: [0; 64],
                                        source_id_len: 0,
                                        name_bytes: [0; 128],
                                        name_len: 0,
                                        lat: place.center_point.lat as f32,
                                        lon: place.center_point.lon as f32,
                                        importance: place.importance.unwrap_or(0.0) as f32,
                                        layer_rank: get_layer_rank(place.layer),
                                    };

                                    let src_id = place.source_id.as_bytes();
                                    let l1 = src_id.len().min(64);
                                    record.source_id_bytes[..l1].copy_from_slice(&src_id[..l1]);
                                    record.source_id_len = l1 as u8;

                                    let name_b = display_name.as_bytes();
                                    let l2 = name_b.len().min(128);
                                    record.name_bytes[..l2].copy_from_slice(&name_b[..l2]);
                                    record.name_len = l2 as u8;

                                    let mut phrases = HashSet::new();
                                    for p in place.name.values().chain(place.phrase.iter()) {
                                        let k = p.trim().to_lowercase();
                                        if !k.is_empty() {
                                            phrases.insert(k);
                                        }
                                    }
                                    local_results.push((
                                        record,
                                        phrases,
                                        place.center_point.lat,
                                        place.center_point.lon,
                                    ));
                                }
                            }

                            // Commit local findings to global state
                            {
                                let mut s = state.lock().unwrap();
                                for (record, phrases, lat, lon) in local_results {
                                    let place_id = s.places.len() as u32;
                                    s.places.push(record);

                                    let cell_id =
                                        CypressMemDb::coord_to_cell(lat as f32, lon as f32);
                                    s.spatial_map.entry(cell_id).or_default().push(place_id);

                                    for phrase in phrases {
                                        let str_id = if let Some(&id) = s.string_id_map.get(&phrase)
                                        {
                                            id
                                        } else {
                                            let new_id = s.string_to_places_temp.len() as u32;
                                            s.string_id_map.insert(phrase.clone(), new_id);
                                            s.string_to_places_temp.push(Vec::new());

                                            let bytes = phrase.as_bytes();
                                            let mut local_bigrams = Vec::new();
                                            for w in bytes.windows(2) {
                                                local_bigrams
                                                    .push(((w[0] as u16) << 8) | (w[1] as u16));
                                            }
                                            s.string_bigram_counts
                                                .push(local_bigrams.len().min(255) as u8);
                                            local_bigrams.dedup();
                                            for bg in local_bigrams {
                                                s.bigrams_temp[bg as usize].push(new_id);
                                            }
                                            new_id
                                        };
                                        s.string_to_places_temp[str_id as usize].push(place_id);
                                    }
                                }
                                s.processed_ranges += 1;
                                if s.processed_ranges % 50 == 0
                                    || s.processed_ranges == total_ranges as u32
                                {
                                    info!(
                                        "Progress: {}/{} ranges processed",
                                        s.processed_ranges, total_ranges
                                    );
                                }
                            }
                            break;
                        }
                        Err(e) => {
                            attempt += 1;
                            if attempt >= max_retries {
                                error!(
                                    "Range [{}, {}] aborted after {} attempts: {}",
                                    start, end, attempt, e
                                );
                                break;
                            }
                            let delay = Duration::from_secs(2u64.pow(attempt));
                            warn!(
                                "Range [{}, {}] failed ({}). Retrying in {:?}...",
                                start, end, e, delay
                            );
                            sleep(delay).await;
                        }
                    }
                }
            }
        })
        .await;

    info!("Finalising database structures...");
    let mut s = Arc::try_unwrap(state)
        .map_err(|_| anyhow::anyhow!("State lock error"))
        .unwrap()
        .into_inner()
        .unwrap();

    let mut bigram_offsets = Vec::with_capacity(65537);
    let mut bigram_data = Vec::new();
    let mut current_offset = 0;
    for list in s.bigrams_temp.iter_mut() {
        list.dedup();
        bigram_offsets.push(current_offset);
        bigram_data.extend_from_slice(list);
        current_offset = bigram_data.len() as u32;
    }
    bigram_offsets.push(current_offset);

    let mut string_to_places_offsets = Vec::with_capacity(s.string_to_places_temp.len() + 1);
    let mut string_to_places_data = Vec::new();
    current_offset = 0;
    for list in s.string_to_places_temp.iter_mut() {
        list.dedup();
        string_to_places_offsets.push(current_offset);
        string_to_places_data.extend_from_slice(list);
        current_offset = string_to_places_data.len() as u32;
    }
    string_to_places_offsets.push(current_offset);

    let mut active_cells = Vec::new();
    let mut cell_offsets = Vec::new();
    let mut cell_places = Vec::new();
    let mut cells: Vec<_> = s.spatial_map.into_iter().collect();
    cells.sort_unstable_by_key(|(k, _)| *k);
    current_offset = 0;
    for (cell_id, places_in_cell) in cells {
        active_cells.push(cell_id);
        cell_offsets.push(current_offset);
        cell_places.extend(places_in_cell);
        current_offset = cell_places.len() as u32;
    }
    cell_offsets.push(current_offset);

    let memdb = CypressMemDb {
        string_bigram_counts: s.string_bigram_counts,
        bigram_offsets,
        bigram_data,
        places: s.places,
        string_to_places_offsets,
        string_to_places_data,
        active_cells,
        cell_offsets,
        cell_places,
    };

    info!("Serialising to rkyv binary...");
    let mut serializer = AllocSerializer::<4096>::default();
    serializer
        .serialize_value(&memdb)
        .context("Rkyv serialization failed")?;
    let bytes = serializer.into_serializer().into_inner();

    let db_path = out_dir.join("cypress_memdb.bin");
    let mut file = File::create(&db_path)?;
    file.write_all(&bytes)?;
    file.flush()?;

    let version_path = out_dir.join("current_version.txt");
    fs::write(version_path, chrono::Utc::now().timestamp().to_string())?;

    info!(
        "Compilation successful. Total places: {}",
        memdb.places.len()
    );
    Ok(())
}
