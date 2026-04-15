use anyhow::{Context, Result};
use bytemuck::{Pod, Zeroable};
use clap::Parser;
use cypress::models::place::PlaceSummary;
use fst::MapBuilder;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use cypress::scylla::ScyllaClient;

#[derive(Parser, Debug)]
#[command(name = "compiler")]
#[command(about = "Compiles ScyllaDB places into FST and binary memory maps")]
struct Args {
    /// ScyllaDB URL
    #[arg(long, default_value = "127.0.0.1")]
    scylla_url: String,

    /// Output directory for compiled files
    #[arg(short, long, default_value = "./data/compiled")]
    out_dir: String,
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

    info!("Streaming all places and compiling FST...");

    // We will collect keys to sort in memory
    let mut fst_keys = Vec::new();
    let mut index = 0u64;

    let bin_path = out_dir.join("places_summary.bin");
    let fst_path = out_dir.join("typeahead.fst");
    let version_path = out_dir.join("current_version.txt");

    let bin_file = File::create(&bin_path)?;
    let mut bin_writer = BufWriter::new(bin_file);

    use futures::TryStreamExt;
    let mut rows_stream = scylla_client
        .session
        .query_iter("SELECT data FROM cypress.places", &[])
        .await?
        .rows_stream::<(String,)>()?;

    while let Some((data,)) = rows_stream.try_next().await? {
        let place: cypress::models::normalized::NormalizedPlace = match serde_json::from_str(&data)
        {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("Failed to parse place from DB: {}", e);
                continue;
            }
        };

        let mut summary = PlaceSummary {
            source_id: [0; 64],
            name: [0; 128],
            lat: place.center_point.lat as f32,
            lon: place.center_point.lon as f32,
        };

        // Copy source ID bytes
        let src_id_bytes = place.source_id.as_bytes();
        let copy_len = src_id_bytes.len().min(64);
        summary.source_id[..copy_len].copy_from_slice(&src_id_bytes[..copy_len]);

        // Choose best name to display
        let display_name = place
            .phrase
            .clone()
            .or_else(|| place.name.get("default").cloned())
            .or_else(|| place.name.values().next().cloned())
            .unwrap_or_default();

        let name_bytes = display_name.as_bytes();
        let copy_len = name_bytes.len().min(128);
        summary.name[..copy_len].copy_from_slice(&name_bytes[..copy_len]);

        // Write binary struct
        bin_writer.write_all(bytemuck::bytes_of(&summary))?;

        // Harvest keys for FST
        for phrase in place.name.values().chain(place.phrase.iter()) {
            let mut key = phrase.trim().to_lowercase();
            if key.is_empty() {
                continue;
            }
            key.push('\0');
            key.push_str(&place.source_id);
            fst_keys.push((key, index));
        }

        index += 1;
        if index % 100000 == 0 {
            info!("Processed {} places...", index);
        }
    }
    bin_writer.flush()?;

    info!(
        "Total places: {}. Sorting {} FST keys...",
        index,
        fst_keys.len()
    );
    fst_keys.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    fst_keys.dedup_by(|a, b| a.0 == b.0);

    info!("Building FST...");
    let fst_file = File::create(&fst_path)?;
    let mut fst_writer = BufWriter::new(fst_file);
    let mut build = MapBuilder::new(&mut fst_writer)?;
    for (k, v) in fst_keys {
        build.insert(k, v)?;
    }
    build.finish()?;

    // Atomically write new version file
    info!("Writing current_version.txt...");
    let temp_version = out_dir.join("current_version.txt.tmp");
    let version_str = chrono::Utc::now().timestamp().to_string();
    fs::write(&temp_version, version_str)?;
    fs::rename(temp_version, version_path)?;

    info!("Compilation finished successfully in {:?}", out_dir);
    Ok(())
}
