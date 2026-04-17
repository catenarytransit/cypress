use anyhow::{Context, Result};
use clap::Parser;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use cypress::models::memdb::{CypressMemDb, PlaceRecord};
use cypress::scylla::ScyllaClient;
use rkyv::ser::{serializers::AllocSerializer, Serializer};

#[derive(Parser, Debug)]
#[command(name = "compiler")]
#[command(about = "Compiles ScyllaDB places into zero-copy rkyv memory maps")]
struct Args {
    /// ScyllaDB URL
    #[arg(long, default_value = "127.0.0.1")]
    scylla_url: String,

    /// Output directory for compiled files
    #[arg(short, long, default_value = "./data/compiled")]
    out_dir: String,
}

fn get_layer_rank(layer: cypress::models::place::Layer) -> u8 {
    // Basic assignment based on general importance.
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

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();
    let out_dir = Path::new(&args.out_dir);
    fs::create_dir_all(out_dir).context("Failed to create output directory")?;

    info!("Connecting to ScyllaDB at {}", args.scylla_url);
    let scylla_client = ScyllaClient::new(&args.scylla_url).await?;

    info!("Streaming all places and compiling rkyv DB...");

    let mut places = Vec::new();

    let mut string_id_map: HashMap<String, u32> = HashMap::new();
    let mut string_to_places_temp: Vec<Vec<u32>> = Vec::new();
    let mut string_bigram_counts = Vec::new();
    let mut bigrams_temp: Vec<Vec<u32>> = vec![Vec::new(); 65536];

    let mut spatial_map: HashMap<u32, Vec<u32>> = HashMap::new();

    let version_path = out_dir.join("current_version.txt");

    use futures::TryStreamExt;
    let mut rows_stream = scylla_client
        .session
        .query_iter("SELECT data FROM cypress.places", &[])
        .await?
        .rows_stream::<(String,)>()?;

    let mut place_count = 0u32;

    while let Some((data,)) = rows_stream.try_next().await? {
        let place: cypress::models::normalized::NormalizedPlace = match serde_json::from_str(&data)
        {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("Failed to parse place from DB: {}", e);
                continue;
            }
        };

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

        let src_id_bytes = place.source_id.as_bytes();
        let copy_len = src_id_bytes.len().min(64);
        record.source_id_bytes[..copy_len].copy_from_slice(&src_id_bytes[..copy_len]);
        record.source_id_len = copy_len as u8;

        let name_bytes = display_name.as_bytes();
        let copy_len = name_bytes.len().min(128);
        record.name_bytes[..copy_len].copy_from_slice(&name_bytes[..copy_len]);
        record.name_len = copy_len as u8;

        places.push(record);

        let place_id = place_count;

        // Populate spatial grid
        let cell_id = CypressMemDb::coord_to_cell(
            place.center_point.lat as f32,
            place.center_point.lon as f32,
        );
        spatial_map.entry(cell_id).or_default().push(place_id);

        let mut unique_phrases = HashSet::new();
        for phrase in place.name.values().chain(place.phrase.iter()) {
            let key = phrase.trim().to_lowercase();
            if !key.is_empty() {
                unique_phrases.insert(key);
            }
        }

        // Process bigrams
        for phrase in unique_phrases {
            let str_id = if let Some(&id) = string_id_map.get(&phrase) {
                id
            } else {
                let new_id = string_to_places_temp.len() as u32;
                string_id_map.insert(phrase.clone(), new_id);
                string_to_places_temp.push(Vec::new());

                let text_bytes = phrase.as_bytes();
                let mut count = 0;
                let mut local_bigrams = Vec::new();
                for window in text_bytes.windows(2) {
                    let bigram_key = ((window[0] as u16) << 8) | (window[1] as u16);
                    local_bigrams.push(bigram_key);
                    count += 1;
                }
                string_bigram_counts.push(count.min(255) as u8);

                local_bigrams.dedup();
                for bg in local_bigrams {
                    bigrams_temp[bg as usize].push(new_id);
                }

                new_id
            };

            string_to_places_temp[str_id as usize].push(place_id);
        }

        place_count += 1;
        if place_count % 100000 == 0 {
            info!("Processed {} places...", place_count);
        }
    }

    info!("Flattening structures...");

    // Flatten bigrams
    let mut bigram_offsets = Vec::with_capacity(65537);
    let mut bigram_data = Vec::new();
    let mut current_offset = 0;
    for list in bigrams_temp.iter_mut() {
        list.dedup();
        bigram_offsets.push(current_offset);
        bigram_data.extend_from_slice(list);
        current_offset = bigram_data.len() as u32;
    }
    bigram_offsets.push(current_offset);

    // Flatten strings_to_places
    let mut string_to_places_offsets = Vec::with_capacity(string_to_places_temp.len() + 1);
    let mut string_to_places_data = Vec::new();
    current_offset = 0;
    for list in string_to_places_temp.iter_mut() {
        list.dedup();
        string_to_places_offsets.push(current_offset);
        string_to_places_data.extend_from_slice(list);
        current_offset = string_to_places_data.len() as u32;
    }
    string_to_places_offsets.push(current_offset);

    // Flatten Spatial Grid
    let mut active_cells = Vec::new();
    let mut cell_offsets = Vec::new();
    let mut cell_places = Vec::new();

    let mut cells: Vec<_> = spatial_map.into_iter().collect();
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
        string_bigram_counts,
        bigram_offsets,
        bigram_data,
        places,
        string_to_places_offsets,
        string_to_places_data,
        active_cells,
        cell_offsets,
        cell_places,
    };

    info!("Serializing with rkyv...");
    let mut serializer = AllocSerializer::<4096>::default();
    serializer.serialize_value(&memdb).unwrap();
    let bytes = serializer.into_serializer().into_inner();

    let db_path = out_dir.join("cypress_memdb.bin");
    let mut file = File::create(&db_path)?;
    file.write_all(&bytes)?;
    file.flush()?;

    // Atomically write new version file
    info!("Writing current_version.txt...");
    let temp_version = out_dir.join("current_version.txt.tmp");
    let version_str = chrono::Utc::now().timestamp().to_string();
    fs::write(&temp_version, version_str)?;
    fs::rename(temp_version, version_path)?;

    info!("Compilation finished successfully in {:?}", out_dir);
    Ok(())
}
