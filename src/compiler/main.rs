use anyhow::{anyhow, Context, Result};
use clap::Parser;
use futures::stream::{self, StreamExt, TryStreamExt};
use memmap2::MmapOptions;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;
use xxhash_rust::xxh64::xxh64;

use cypress::models::memdb::{CypressMemDb, PlaceRecord, PLACE_RECORD_DISK_BYTES};
use cypress::models::normalized::NormalizedPlace;
use cypress::models::population::decompress_population;
use cypress::scylla::ScyllaClient;
use rkyv::ser::{
    serializers::{AllocScratch, CompositeSerializer, SharedSerializeMap, WriteSerializer},
    Serializer,
};

const TERM_BUCKETS: usize = 128;
const BIGRAM_SHARDS: usize = 256;
const HIERARCHY_PAIR_BUCKETS: usize = 512;

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

    /// Temporary directory used for intermediate worker spill files
    #[arg(long, default_value = "./data/compiled/tmp")]
    tmp_dir: String,
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

fn admin_level_from_field(field: &str) -> u8 {
    match field {
        "country" => 2,
        "macro_region" => 3,
        "region" => 4,
        "macro_county" => 5,
        "county" => 6,
        "local_admin" => 7,
        "locality" => 8,
        "borough" => 9,
        "neighbourhood" => 10,
        _ => 0,
    }
}

#[derive(Debug, Clone)]
struct WorkerSpill {
    worker_id: u32,
    place_file: PathBuf,
    term_file: PathBuf,
    parent_file: PathBuf,
    place_count: u32,
}

fn read_exact_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<bool> {
    let mut read_bytes = 0usize;
    while read_bytes < buf.len() {
        let n = reader.read(&mut buf[read_bytes..])?;
        if n == 0 {
            if read_bytes == 0 {
                return Ok(false);
            }
            return Err(anyhow!("Unexpected EOF while reading spill file"));
        }
        read_bytes += n;
    }
    Ok(true)
}

fn write_place_record<W: Write>(writer: &mut W, record: &PlaceRecord) -> Result<()> {
    writer.write_all(&record.source_id_bytes)?;
    writer.write_all(&[record.source_id_len])?;
    writer.write_all(&record.name_bytes)?;
    writer.write_all(&[record.name_len])?;
    writer.write_all(&record.lat.to_le_bytes())?;
    writer.write_all(&record.lon.to_le_bytes())?;
    writer.write_all(&record.importance.to_le_bytes())?;
    writer.write_all(&[record.layer_rank])?;
    writer.write_all(&record.population.to_le_bytes())?;
    Ok(())
}

fn decode_place_record(bytes: &[u8]) -> Result<PlaceRecord> {
    PlaceRecord::from_disk_bytes(bytes)
        .ok_or_else(|| anyhow!("Invalid place record byte width: {}", bytes.len()))
}

fn write_term_entry<W: Write>(writer: &mut W, place_id: u32, phrase: &str) -> Result<()> {
    writer.write_all(&place_id.to_le_bytes())?;
    let phrase_bytes = phrase.as_bytes();
    let len = phrase_bytes.len() as u32;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(phrase_bytes)?;
    Ok(())
}

fn read_term_entry<R: Read>(reader: &mut R) -> Result<Option<(u32, String)>> {
    let mut place_buf = [0u8; 4];
    if !read_exact_or_eof(reader, &mut place_buf)? {
        return Ok(None);
    }
    let place_id = u32::from_le_bytes(place_buf);

    let mut len_buf = [0u8; 4];
    if !read_exact_or_eof(reader, &mut len_buf)? {
        return Err(anyhow!("Unexpected EOF reading term length"));
    }
    let phrase_len = u32::from_le_bytes(len_buf) as usize;

    let mut phrase_bytes = vec![0u8; phrase_len];
    if phrase_len > 0 && !read_exact_or_eof(reader, &mut phrase_bytes)? {
        return Err(anyhow!("Unexpected EOF reading term phrase bytes"));
    }

    let phrase = String::from_utf8(phrase_bytes).context("Invalid UTF-8 in term spill")?;
    Ok(Some((place_id, phrase)))
}

fn write_bigram_pair<W: Write>(writer: &mut W, bigram: u16, string_id: u32) -> Result<()> {
    writer.write_all(&bigram.to_le_bytes())?;
    writer.write_all(&string_id.to_le_bytes())?;
    Ok(())
}

fn read_bigram_pair<R: Read>(reader: &mut R) -> Result<Option<(u16, u32)>> {
    let mut bg_buf = [0u8; 2];
    if !read_exact_or_eof(reader, &mut bg_buf)? {
        return Ok(None);
    }

    let mut sid_buf = [0u8; 4];
    if !read_exact_or_eof(reader, &mut sid_buf)? {
        return Err(anyhow!("Unexpected EOF reading bigram shard"));
    }

    Ok(Some((
        u16::from_le_bytes(bg_buf),
        u32::from_le_bytes(sid_buf),
    )))
}

/// Parent spill entry format:
///   local_place_id: u32
///   admin_level: u8
///   id_len: u16
///   id_bytes: [u8; id_len]
fn write_parent_entry<W: Write>(
    writer: &mut W,
    local_place_id: u32,
    admin_level: u8,
    area_id: &str,
) -> Result<()> {
    writer.write_all(&local_place_id.to_le_bytes())?;
    writer.write_all(&[admin_level])?;
    let id_bytes = area_id.as_bytes();
    let id_len = id_bytes.len() as u16;
    writer.write_all(&id_len.to_le_bytes())?;
    writer.write_all(id_bytes)?;
    Ok(())
}

fn read_parent_entry<R: Read>(reader: &mut R) -> Result<Option<(u32, u8, String)>> {
    let mut pid_buf = [0u8; 4];
    if !read_exact_or_eof(reader, &mut pid_buf)? {
        return Ok(None);
    }
    let place_id = u32::from_le_bytes(pid_buf);

    let mut level_buf = [0u8; 1];
    if !read_exact_or_eof(reader, &mut level_buf)? {
        return Err(anyhow!("Unexpected EOF reading parent admin level"));
    }

    let mut len_buf = [0u8; 2];
    if !read_exact_or_eof(reader, &mut len_buf)? {
        return Err(anyhow!("Unexpected EOF reading parent id length"));
    }
    let id_len = u16::from_le_bytes(len_buf) as usize;

    let mut id_bytes = vec![0u8; id_len];
    if id_len > 0 && !read_exact_or_eof(reader, &mut id_bytes)? {
        return Err(anyhow!("Unexpected EOF reading parent id bytes"));
    }

    let area_id = String::from_utf8(id_bytes).context("Invalid UTF-8 in parent spill")?;
    Ok(Some((place_id, level_buf[0], area_id)))
}

fn write_u32_pair<W: Write>(writer: &mut W, left: u32, right: u32) -> Result<()> {
    writer.write_all(&left.to_le_bytes())?;
    writer.write_all(&right.to_le_bytes())?;
    Ok(())
}

fn read_u32_pair<R: Read>(reader: &mut R) -> Result<Option<(u32, u32)>> {
    let mut left_buf = [0u8; 4];
    if !read_exact_or_eof(reader, &mut left_buf)? {
        return Ok(None);
    }

    let mut right_buf = [0u8; 4];
    if !read_exact_or_eof(reader, &mut right_buf)? {
        return Err(anyhow!("Unexpected EOF reading u32 pair spill"));
    }

    Ok(Some((
        u32::from_le_bytes(left_buf),
        u32::from_le_bytes(right_buf),
    )))
}

fn get_or_create_writer<'a>(
    writers: &'a mut HashMap<usize, BufWriter<File>>,
    dir: &Path,
    prefix: &str,
    bucket: usize,
) -> Result<&'a mut BufWriter<File>> {
    if !writers.contains_key(&bucket) {
        let path = dir.join(format!("{}_{:03}.bin", prefix, bucket));
        let file = File::create(path)?;
        writers.insert(bucket, BufWriter::new(file));
    }
    writers
        .get_mut(&bucket)
        .ok_or_else(|| anyhow!("Failed to fetch writer bucket {}", bucket))
}

async fn fetch_range_to_spill(
    client: Arc<ScyllaClient>,
    start: i64,
    end: i64,
    place_file: &Path,
    term_file: &Path,
    parent_file: &Path,
) -> Result<u32> {
    let query = format!(
        "SELECT data, population FROM cypress.places WHERE token(id) >= {} AND token(id) < {}",
        start, end
    );

    let mut place_writer = BufWriter::new(File::create(place_file).with_context(|| {
        format!(
            "Failed to create place spill file: {}",
            place_file.display()
        )
    })?);
    let mut term_writer =
        BufWriter::new(File::create(term_file).with_context(|| {
            format!("Failed to create term spill file: {}", term_file.display())
        })?);
    let mut parent_writer = BufWriter::new(File::create(parent_file).with_context(|| {
        format!(
            "Failed to create parent spill file: {}",
            parent_file.display()
        )
    })?);

    let it = client
        .session
        .query_iter(query, &[])
        .await
        .context("Scylla query_iter failed")?;

    let mut rows = it
        .rows_stream::<(String, Option<i32>)>()
        .context("rows_stream type conversion failed")?;

    let mut local_place_id = 0u32;

    while let Some(row_result) = rows.next().await {
        let (data, compressed_population) = row_result.context("Row fetch failure")?;

        let Ok(place) = serde_json::from_str::<NormalizedPlace>(&data) else {
            continue;
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
            population: decompress_population(
                compressed_population.and_then(|p| u32::try_from(p).ok()),
            )
            .or(place.population)
            .unwrap_or(0),
        };

        let src_id = place.source_id.as_bytes();
        let src_len = src_id.len().min(64);
        record.source_id_bytes[..src_len].copy_from_slice(&src_id[..src_len]);
        record.source_id_len = src_len as u8;

        let name_b = display_name.as_bytes();
        let name_len = name_b.len().min(128);
        record.name_bytes[..name_len].copy_from_slice(&name_b[..name_len]);
        record.name_len = name_len as u8;

        write_place_record(&mut place_writer, &record)?;

        let mut phrases = HashSet::new();
        for p in place.name.values().chain(place.phrase.iter()) {
            let key = p.trim().to_lowercase();
            if !key.is_empty() {
                phrases.insert(key);
            }
        }

        for phrase in phrases {
            write_term_entry(&mut term_writer, local_place_id, &phrase)?;
        }

        // Emit parent hierarchy entries
        let parent_fields: [(&str, &Option<String>); 9] = [
            ("country", &place.parent.country),
            ("macro_region", &place.parent.macro_region),
            ("region", &place.parent.region),
            ("macro_county", &place.parent.macro_county),
            ("county", &place.parent.county),
            ("local_admin", &place.parent.local_admin),
            ("locality", &place.parent.locality),
            ("borough", &place.parent.borough),
            ("neighbourhood", &place.parent.neighbourhood),
        ];

        for (field, id_opt) in &parent_fields {
            if let Some(id) = id_opt {
                let level = admin_level_from_field(field);
                write_parent_entry(&mut parent_writer, local_place_id, level, id)?;
            }
        }

        local_place_id += 1;
    }

    place_writer.flush()?;
    term_writer.flush()?;
    parent_writer.flush()?;
    Ok(local_place_id)
}

/// ADR-aligned little-endian bigram encoding: char[0] in low byte, char[1] in high byte.
fn compress_bigram(b0: u8, b1: u8) -> u16 {
    (b0 as u16) | ((b1 as u16) << 8)
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

    let tmp_root = Path::new(&args.tmp_dir);
    fs::create_dir_all(tmp_root).context("Failed to create temporary root directory")?;
    let run_tmp_dir = tmp_root.join(format!("compile-{}", chrono::Utc::now().timestamp_millis()));
    fs::create_dir_all(&run_tmp_dir).context("Failed to create temporary run directory")?;

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
        ranges.push((i64::MIN, i64::MAX));
    } else {
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

    let spills = Arc::new(Mutex::new(Vec::<WorkerSpill>::new()));
    let worker_counter = Arc::new(AtomicU32::new(0));
    let processed_ranges = Arc::new(AtomicU32::new(0));

    let scylla_client_ref = Arc::new(scylla_client);

    stream::iter(ranges.into_iter().map(Ok::<(i64, i64), anyhow::Error>))
        .try_for_each_concurrent(args.concurrency, |(start, end)| {
            let client = Arc::clone(&scylla_client_ref);
            let spills = Arc::clone(&spills);
            let worker_counter = Arc::clone(&worker_counter);
            let processed_ranges = Arc::clone(&processed_ranges);
            let run_tmp_dir = run_tmp_dir.clone();
            let max_retries = args.max_retries;

            async move {
                let worker_id = worker_counter.fetch_add(1, Ordering::Relaxed);
                let place_file = run_tmp_dir.join(format!("worker_{:08}.places.bin", worker_id));
                let term_file = run_tmp_dir.join(format!("worker_{:08}.terms.bin", worker_id));
                let parent_file = run_tmp_dir.join(format!("worker_{:08}.parents.bin", worker_id));

                let mut attempt = 0u32;
                let place_count = loop {
                    attempt += 1;
                    match fetch_range_to_spill(
                        Arc::clone(&client),
                        start,
                        end,
                        &place_file,
                        &term_file,
                        &parent_file,
                    )
                    .await
                    {
                        Ok(count) => break count,
                        Err(e) => {
                            if attempt >= max_retries {
                                error!(
                                    "Range [{}, {}] aborted after {} attempts: {}",
                                    start, end, attempt, e
                                );
                                return Err(anyhow!(
                                    "Range [{}, {}] failed permanently: {}",
                                    start,
                                    end,
                                    e
                                ));
                            }

                            let delay = Duration::from_secs(2u64.saturating_pow(attempt));
                            warn!(
                                "Range [{}, {}] failed ({}). Retrying in {:?}...",
                                start, end, e, delay
                            );
                            sleep(delay).await;
                        }
                    }
                };

                spills.lock().unwrap().push(WorkerSpill {
                    worker_id,
                    place_file,
                    term_file,
                    parent_file,
                    place_count,
                });

                let done = processed_ranges.fetch_add(1, Ordering::Relaxed) + 1;
                if done % 5 == 0 || done == total_ranges as u32 {
                    info!("Progress: {}/{} ranges processed", done, total_ranges);
                }

                Ok(())
            }
        })
        .await?;

    info!("Pass 1 complete. Building pass-2 index structures...");

    let mut spills = Arc::try_unwrap(spills)
        .map_err(|_| anyhow!("Failed to unwrap worker spill state"))?
        .into_inner()
        .map_err(|_| anyhow!("Failed to lock worker spill state"))?;
    spills.sort_unstable_by_key(|s| s.worker_id);

    let mut worker_offsets = Vec::with_capacity(spills.len());
    let mut total_places = 0u32;
    for spill in &spills {
        worker_offsets.push((spill.clone(), total_places));
        total_places = total_places.saturating_add(spill.place_count);
    }

    info!(
        "Merging {} worker place files into mmap-backed storage ({} records)",
        worker_offsets.len(),
        total_places
    );

    let merged_places_path = run_tmp_dir.join("places_merged.bin");
    let merged_places_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&merged_places_path)
        .with_context(|| {
            format!(
                "Failed to create merged place mmap file: {}",
                merged_places_path.display()
            )
        })?;
    merged_places_file.set_len((total_places as u64) * (PLACE_RECORD_DISK_BYTES as u64))?;

    {
        let mut mmap = unsafe {
            MmapOptions::new()
                .map_mut(&merged_places_file)
                .context("Failed to mmap merged place file")?
        };

        for (spill, global_offset) in &worker_offsets {
            let expected_bytes = spill.place_count as usize * PLACE_RECORD_DISK_BYTES;
            let start = *global_offset as usize * PLACE_RECORD_DISK_BYTES;
            let end = start + expected_bytes;

            let mut reader = BufReader::new(File::open(&spill.place_file).with_context(|| {
                format!(
                    "Failed to open spill place file: {}",
                    spill.place_file.display()
                )
            })?);
            reader.read_exact(&mut mmap[start..end]).with_context(|| {
                format!(
                    "Failed to read spill bytes into merged mmap: {}",
                    spill.place_file.display()
                )
            })?;
        }

        mmap.flush().context("Failed to flush merged place mmap")?;
    }

    let places_mmap = unsafe {
        MmapOptions::new()
            .map(&merged_places_file)
            .context("Failed to reopen merged place mmap as readonly")?
    };

    info!("Processing places for spatial index");
    let mut cell_pairs = Vec::with_capacity(total_places as usize);
    let mut place_latitudes = Vec::with_capacity(total_places as usize);
    let mut place_longitudes = Vec::with_capacity(total_places as usize);
    let mut place_importances = Vec::with_capacity(total_places as usize);
    let mut place_source_id_offsets = Vec::with_capacity(total_places as usize + 1);
    let mut place_source_id_bytes = Vec::new();
    let mut place_layer_ranks = Vec::with_capacity(total_places as usize);
    let mut place_populations = Vec::with_capacity(total_places as usize);

    place_source_id_offsets.push(0);

    for (idx, chunk) in places_mmap
        .chunks_exact(PLACE_RECORD_DISK_BYTES)
        .enumerate()
    {
        let place = decode_place_record(chunk)?;
        let place_id = idx as u32;
        let cell_id = CypressMemDb::coord_to_cell(place.lat, place.lon);
        cell_pairs.push((cell_id, place_id));

        place_latitudes.push(place.lat);
        place_longitudes.push(place.lon);
        place_importances.push(place.importance);
        place_layer_ranks.push(place.layer_rank);
        place_populations.push(place.population);

        let id_bytes = &place.source_id_bytes[..place.source_id_len as usize];
        place_source_id_bytes.extend_from_slice(id_bytes);
        place_source_id_offsets.push(place_source_id_bytes.len() as u32);
    }

    // === Pass 1.5a: Process parent hierarchy spills (collect unique area ids only) ===
    info!("Pass 1.5a: collecting unique parent area IDs");

    let mut all_area_ids = HashSet::new();
    let mut area_level_by_id: HashMap<String, u8> = HashMap::new();

    for (spill, _) in &worker_offsets {
        let parent_path = &spill.parent_file;
        if !parent_path.exists() || fs::metadata(parent_path)?.len() == 0 {
            continue;
        }

        let mut reader = BufReader::new(File::open(parent_path).with_context(|| {
            format!(
                "Failed to open parent spill file: {}",
                parent_path.display()
            )
        })?);

        while let Some((_, admin_level, area_id)) = read_parent_entry(&mut reader)? {
            if let Some(existing_level) = area_level_by_id.get(&area_id).copied() {
                if existing_level != admin_level {
                    warn!(
                        "Area {} encountered with inconsistent admin levels {} and {}; keeping first",
                        area_id, existing_level, admin_level
                    );
                }
            } else {
                area_level_by_id.insert(area_id.clone(), admin_level);
            }

            all_area_ids.insert(area_id);
        }
    }

    info!("Collected {} unique admin area IDs", all_area_ids.len());

    // Batch-fetch admin areas from ScyllaDB
    info!("Fetching admin area names from ScyllaDB...");
    let all_area_ids_vec: Vec<String> = all_area_ids.into_iter().collect();

    // Process in batches (ScyllaDB IN clause has limits)
    let mut admin_area_data: HashMap<String, (String, u32)> = HashMap::new();
    let batch_size = 10;

    for chunk in all_area_ids_vec.chunks(batch_size) {
        let chunk_vec: Vec<String> = chunk.to_vec();
        match scylla_client_ref
            .get_admin_areas_with_population(&chunk_vec)
            .await
        {
            Ok(map) => {
                for (id, (json_data, compressed_population)) in map {
                    if let Ok(entry) =
                        serde_json::from_str::<cypress::models::admin::AdminEntryScylla>(&json_data)
                    {
                        let admin_entry = cypress::models::admin::AdminEntry::from_scylla(entry);
                        let name = admin_entry
                            .name
                            .clone()
                            .or_else(|| admin_entry.names.get("default").cloned())
                            .or_else(|| admin_entry.names.values().next().cloned())
                            .unwrap_or_default()
                            .trim()
                            .to_lowercase();
                        let population = decompress_population(compressed_population)
                            .or(admin_entry.population)
                            .unwrap_or(0);
                        admin_area_data.insert(id, (name, population));
                    }
                }
            }
            Err(e) => {
                warn!("Failed to fetch admin area batch: {}", e);
            }
        }
    }

    info!(
        "Resolved {} admin area names from ScyllaDB",
        admin_area_data.len()
    );

    // Build area index: assign area_idx to each unique admin area
    let mut area_id_to_idx: HashMap<String, u32> = HashMap::new();
    let mut area_name_offsets: Vec<u32> = Vec::new();
    let mut area_name_bytes: Vec<u8> = Vec::new();
    let mut area_admin_levels: Vec<u8> = Vec::new();
    let mut area_populations: Vec<u32> = Vec::new();

    area_name_offsets.push(0);

    // Sorted for deterministic output
    let mut sorted_area_ids: Vec<String> = admin_area_data.keys().cloned().collect();
    sorted_area_ids.sort();

    for area_id in &sorted_area_ids {
        let (name, population) = admin_area_data.get(area_id).unwrap();
        let area_idx = area_id_to_idx.len() as u32;
        area_id_to_idx.insert(area_id.clone(), area_idx);

        area_name_bytes.extend_from_slice(name.as_bytes());
        area_name_offsets.push(area_name_bytes.len() as u32);
        area_populations.push(*population);

        let level = area_level_by_id.get(area_id).copied().unwrap_or(0);
        area_admin_levels.push(level);
    }

    info!("Built {} area entries", area_id_to_idx.len());

    // === Pass 1.5b: Spill compact (global_place_id, area_idx) pairs ===
    info!(
        "Pass 1.5b: spilling place-area pairs into {} buckets",
        HIERARCHY_PAIR_BUCKETS
    );

    let hierarchy_bucket_dir = run_tmp_dir.join("hierarchy_pair_buckets");
    fs::create_dir_all(&hierarchy_bucket_dir)?;
    let mut hierarchy_bucket_writers: HashMap<usize, BufWriter<File>> = HashMap::new();
    let mut hierarchy_pair_count = 0u64;

    for (spill, global_offset) in &worker_offsets {
        let parent_path = &spill.parent_file;
        if !parent_path.exists() || fs::metadata(parent_path)?.len() == 0 {
            continue;
        }

        let mut reader = BufReader::new(File::open(parent_path).with_context(|| {
            format!(
                "Failed to open parent spill file: {}",
                parent_path.display()
            )
        })?);

        while let Some((local_place_id, _, area_id)) = read_parent_entry(&mut reader)? {
            if local_place_id >= spill.place_count {
                return Err(anyhow!(
                    "Corrupt parent spill {}: local place id {} >= {}",
                    parent_path.display(),
                    local_place_id,
                    spill.place_count
                ));
            }

            let Some(&area_idx) = area_id_to_idx.get(&area_id) else {
                continue;
            };

            let global_place_id = global_offset + local_place_id;
            let bucket = (global_place_id as usize) % HIERARCHY_PAIR_BUCKETS;
            let writer = get_or_create_writer(
                &mut hierarchy_bucket_writers,
                &hierarchy_bucket_dir,
                "pairs",
                bucket,
            )?;
            write_u32_pair(writer, global_place_id, area_idx)?;
            hierarchy_pair_count += 1;
        }
    }

    for writer in hierarchy_bucket_writers.values_mut() {
        writer.flush()?;
    }

    info!(
        "Pass 1.5b complete: spilled {} place-area pairs",
        hierarchy_pair_count
    );

    // === Pass 1.5c: Build area sets from bucketed compact pair spills ===
    let sentinel_area_set = u32::MAX;
    let mut area_set_map: HashMap<Vec<u32>, u32> = HashMap::new();
    let mut area_set_offsets: Vec<u32> = Vec::new();
    let mut area_set_data: Vec<u32> = Vec::new();
    let mut place_area_sets = vec![sentinel_area_set; total_places as usize];

    area_set_offsets.push(0);

    info!(
        "Pass 1.5c: building deduplicated area sets from {} pair buckets",
        HIERARCHY_PAIR_BUCKETS
    );

    for bucket in 0..HIERARCHY_PAIR_BUCKETS {
        let bucket_path = hierarchy_bucket_dir.join(format!("pairs_{:03}.bin", bucket));
        if !bucket_path.exists() {
            continue;
        }
        let bucket_len = fs::metadata(&bucket_path)?.len();
        if bucket_len == 0 {
            continue;
        }
        if bucket_len % 8 != 0 {
            return Err(anyhow!(
                "Corrupt hierarchy pair bucket {}: byte length {} is not divisible by 8",
                bucket_path.display(),
                bucket_len
            ));
        }

        let mut pairs = Vec::<(u32, u32)>::with_capacity((bucket_len / 8) as usize);
        let mut reader = BufReader::new(File::open(&bucket_path).with_context(|| {
            format!(
                "Failed to open hierarchy pair bucket: {}",
                bucket_path.display()
            )
        })?);

        while let Some(pair) = read_u32_pair(&mut reader)? {
            pairs.push(pair);
        }

        pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        let mut i = 0usize;
        while i < pairs.len() {
            let place_id = pairs[i].0;
            if place_id >= total_places {
                return Err(anyhow!(
                    "Corrupt hierarchy pair bucket {}: place id {} >= {}",
                    bucket_path.display(),
                    place_id,
                    total_places
                ));
            }

            let mut area_indices = Vec::<u32>::new();
            let mut last_area = None::<u32>;

            while i < pairs.len() && pairs[i].0 == place_id {
                let area_idx = pairs[i].1;
                if last_area != Some(area_idx) {
                    area_indices.push(area_idx);
                    last_area = Some(area_idx);
                }
                i += 1;
            }

            if area_indices.is_empty() {
                continue;
            }

            let area_set_idx = if let Some(&existing_idx) = area_set_map.get(&area_indices) {
                existing_idx
            } else {
                let idx = area_set_map.len() as u32;
                area_set_data.extend_from_slice(&area_indices);
                area_set_offsets.push(area_set_data.len() as u32);
                area_set_map.insert(area_indices, idx);
                idx
            };

            place_area_sets[place_id as usize] = area_set_idx;
        }
    }

    info!(
        "Pass 1.5 complete: built {} deduplicated area sets for {} places",
        area_set_map.len(),
        place_area_sets
            .iter()
            .filter(|&&v| v != sentinel_area_set)
            .count()
    );

    info!(
        "Distributing worker term spills into {} hash buckets",
        TERM_BUCKETS
    );
    let term_bucket_dir = run_tmp_dir.join("term_buckets");
    fs::create_dir_all(&term_bucket_dir)?;
    let mut term_bucket_writers: HashMap<usize, BufWriter<File>> = HashMap::new();

    for (spill, global_offset) in &worker_offsets {
        let mut reader = BufReader::new(File::open(&spill.term_file).with_context(|| {
            format!(
                "Failed to open spill term file: {}",
                spill.term_file.display()
            )
        })?);

        while let Some((local_place_id, phrase)) = read_term_entry(&mut reader)? {
            if local_place_id >= spill.place_count {
                return Err(anyhow!(
                    "Corrupt spill term file {}: local place id {} >= {}",
                    spill.term_file.display(),
                    local_place_id,
                    spill.place_count
                ));
            }

            let global_place_id = global_offset + local_place_id;
            let bucket = (xxh64(phrase.as_bytes(), 0) as usize) % TERM_BUCKETS;
            let writer =
                get_or_create_writer(&mut term_bucket_writers, &term_bucket_dir, "terms", bucket)?;
            write_term_entry(writer, global_place_id, &phrase)?;
        }
    }

    for writer in term_bucket_writers.values_mut() {
        writer.flush()?;
    }

    info!("Pass 2a: merge-sorting term buckets and generating string index");

    let bigram_shard_dir = run_tmp_dir.join("bigram_shards");
    fs::create_dir_all(&bigram_shard_dir)?;

    let mut bigram_shard_writers: HashMap<usize, BufWriter<File>> = HashMap::new();

    let mut string_bigram_counts = Vec::new();
    let mut string_to_places_offsets = Vec::new();
    let mut string_to_places_data = Vec::new();
    let mut string_name_offsets: Vec<u32> = Vec::new();
    let mut string_name_bytes: Vec<u8> = Vec::new();
    let mut next_string_id = 0u32;
    string_name_offsets.push(0);

    for bucket in 0..TERM_BUCKETS {
        let bucket_path = term_bucket_dir.join(format!("terms_{:03}.bin", bucket));
        if !bucket_path.exists() {
            continue;
        }
        if fs::metadata(&bucket_path)?.len() == 0 {
            continue;
        }

        let mut entries = Vec::<(String, u32)>::new();
        let mut reader = BufReader::new(File::open(&bucket_path)?);
        while let Some((place_id, phrase)) = read_term_entry(&mut reader)? {
            entries.push((phrase, place_id));
        }

        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        let mut i = 0usize;
        while i < entries.len() {
            let phrase = entries[i].0.clone();
            let _string_id = next_string_id;
            next_string_id += 1;

            string_to_places_offsets.push(string_to_places_data.len() as u32);

            string_name_bytes.extend_from_slice(phrase.as_bytes());
            string_name_offsets.push(string_name_bytes.len() as u32);

            let mut last_place = None::<u32>;
            while i < entries.len() && entries[i].0 == phrase {
                let place_id = entries[i].1;
                if last_place != Some(place_id) {
                    let lat2 = place_latitudes[place_id as usize];
                    let lon2 = place_longitudes[place_id as usize];
                    let mut duplicate = false;

                    let start_idx = *string_to_places_offsets.last().unwrap() as usize;
                    for &existing_id in &string_to_places_data[start_idx..] {
                        let lat1 = place_latitudes[existing_id as usize];
                        let lon1 = place_longitudes[existing_id as usize];

                        let d_lat = (lat2 - lat1).to_radians();
                        let d_lon = (lon2 - lon1).to_radians();
                        let a = (d_lat / 2.0).sin().powi(2)
                            + lat1.to_radians().cos()
                                * lat2.to_radians().cos()
                                * (d_lon / 2.0).sin().powi(2);
                        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
                        let dist_km = 6371.0 * c;

                        if dist_km < 1.5 {
                            duplicate = true;
                            break;
                        }
                    }

                    if !duplicate {
                        string_to_places_data.push(place_id);
                    }
                    last_place = Some(place_id);
                }
                i += 1;
            }

            // ADR-aligned little-endian bigram encoding
            let mut local_bigrams: Vec<u16> = phrase
                .as_bytes()
                .windows(2)
                .filter(|w| w[0] != b' ' && w[1] != b' ')
                .map(|w| compress_bigram(w[0], w[1]))
                .collect();
            local_bigrams.sort_unstable();
            local_bigrams.dedup();

            string_bigram_counts.push(local_bigrams.len().min(255) as u8);

            for bg in local_bigrams {
                let shard = (bg >> 8) as usize; // high byte = second char
                let writer = get_or_create_writer(
                    &mut bigram_shard_writers,
                    &bigram_shard_dir,
                    "bg",
                    shard,
                )?;
                write_bigram_pair(writer, bg, _string_id)?;
            }
        }
    }

    string_to_places_offsets.push(string_to_places_data.len() as u32);

    for writer in bigram_shard_writers.values_mut() {
        writer.flush()?;
    }

    info!("Pass 2b: building flattened bigram offset/data arrays");

    let mut bigram_offsets = Vec::with_capacity(65537);
    let mut bigram_data = Vec::<u32>::new();
    let mut current_offset = 0u32;
    let mut current_bigram = 0u32;
    bigram_offsets.push(0);

    for shard in 0..BIGRAM_SHARDS {
        let shard_path = bigram_shard_dir.join(format!("bg_{:03}.bin", shard));
        if !shard_path.exists() {
            continue;
        }
        if fs::metadata(&shard_path)?.len() == 0 {
            continue;
        }

        let mut pairs = Vec::<(u16, u32)>::new();
        let mut reader = BufReader::new(File::open(&shard_path)?);
        while let Some(pair) = read_bigram_pair(&mut reader)? {
            pairs.push(pair);
        }

        pairs.sort_unstable();
        pairs.dedup();

        let mut i = 0usize;
        while i < pairs.len() {
            let bg = pairs[i].0 as u32;
            while current_bigram < bg {
                bigram_offsets.push(current_offset);
                current_bigram += 1;
            }

            while i < pairs.len() && pairs[i].0 as u32 == bg {
                bigram_data.push(pairs[i].1);
                i += 1;
            }

            current_offset = bigram_data.len() as u32;
            bigram_offsets.push(current_offset);
            current_bigram += 1;
        }
    }

    while current_bigram < 65536 {
        bigram_offsets.push(current_offset);
        current_bigram += 1;
    }

    if bigram_offsets.len() != 65537 {
        return Err(anyhow!(
            "Invalid bigram_offsets length: {}",
            bigram_offsets.len()
        ));
    }

    info!("Building sparse spatial grid arrays");

    cell_pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    let mut active_cells = Vec::new();
    let mut cell_offsets = Vec::new();
    let mut cell_places = Vec::new();

    let mut i = 0usize;
    while i < cell_pairs.len() {
        let cell_id = cell_pairs[i].0;
        active_cells.push(cell_id);
        cell_offsets.push(cell_places.len() as u32);

        while i < cell_pairs.len() && cell_pairs[i].0 == cell_id {
            cell_places.push(cell_pairs[i].1);
            i += 1;
        }
    }
    cell_offsets.push(cell_places.len() as u32);

    let memdb = CypressMemDb {
        string_bigram_counts,
        bigram_offsets,
        bigram_data,
        string_to_places_offsets,
        string_to_places_data,
        place_latitudes,
        place_longitudes,
        place_importances,
        place_source_id_offsets,
        place_source_id_bytes,
        place_layer_ranks,
        place_populations,
        string_name_offsets,
        string_name_bytes,
        area_name_offsets,
        area_name_bytes,
        area_admin_levels,
        area_populations,
        area_set_offsets,
        area_set_data,
        place_area_sets,
        active_cells,
        cell_offsets,
        cell_places,
    };

    info!("Serialising index to rkyv binary (streaming writer)...");

    let db_path = out_dir.join("cypress_index.bin");
    let output_file = File::create(&db_path)
        .with_context(|| format!("Failed to create output file: {}", db_path.display()))?;
    let writer = BufWriter::new(output_file);
    let mut serializer = CompositeSerializer::new(
        WriteSerializer::new(writer),
        AllocScratch::default(),
        SharedSerializeMap::default(),
    );
    serializer
        .serialize_value(&memdb)
        .context("Rkyv streaming serialization failed")?;
    let mut writer = serializer.into_serializer().into_inner();
    writer.flush()?;

    drop(places_mmap);
    drop(merged_places_file);

    let final_places_path = out_dir.join("cypress_places.bin");
    fs::rename(&merged_places_path, &final_places_path).with_context(|| {
        format!(
            "Failed to move merged places file {} -> {}",
            merged_places_path.display(),
            final_places_path.display()
        )
    })?;

    let version_path = out_dir.join("current_version.txt");
    fs::write(version_path, chrono::Utc::now().timestamp().to_string())?;

    if let Err(e) = fs::remove_dir_all(&run_tmp_dir) {
        warn!(
            "Compilation succeeded but temporary run directory could not be removed ({}): {}",
            run_tmp_dir.display(),
            e
        );
    }

    info!("Compilation successful. Total places: {}", total_places);
    Ok(())
}
