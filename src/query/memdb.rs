use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use memmap2::Mmap;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{info, warn};

use cypress::models::memdb::{
    ArchivedCypressMemDb, CypressMemDb, PlaceRecord, PLACE_RECORD_DISK_BYTES,
};

pub struct CosSimMatch {
    pub string_idx: u32,
    pub cos_sim: f32,
}

const QUERY_CACHE_MAX_ENTRIES: usize = 128;
const QUERY_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Sparse cached ScanCount state. The old cache stored one byte for every
/// searchable string per entry, even when only a small fraction of IDs had
/// been touched. These parallel arrays use approximately five payload bytes
/// per touched string and can be restored without scanning the corpus.
pub struct SparseNgramCounts {
    pub string_ids: Vec<u32>,
    pub counts: Vec<u8>,
}

impl SparseNgramCounts {
    fn approx_bytes(&self) -> usize {
        self.string_ids
            .len()
            .saturating_mul(std::mem::size_of::<u32>())
            .saturating_add(self.counts.len())
    }
}

fn is_subset(subset: &[u16], superset: &[u16]) -> bool {
    if subset.len() > superset.len() {
        return false;
    }

    let mut i = 0usize;
    let mut j = 0usize;
    while i < subset.len() && j < superset.len() {
        match subset[i].cmp(&superset[j]) {
            std::cmp::Ordering::Less => return false,
            std::cmp::Ordering::Equal => {
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Greater => {
                j += 1;
            }
        }
    }

    i == subset.len()
}

fn missing_elements(subset: &[u16], superset: &[u16], out: &mut Vec<u16>) {
    out.clear();

    let mut i = 0usize;
    let mut j = 0usize;
    while j < superset.len() {
        if i >= subset.len() {
            out.extend_from_slice(&superset[j..]);
            break;
        }

        match subset[i].cmp(&superset[j]) {
            std::cmp::Ordering::Equal => {
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(superset[j]);
                j += 1;
            }
            std::cmp::Ordering::Less => {
                i += 1;
            }
        }
    }
}

pub struct QueryNgramCache {
    max_entries: usize,
    max_bytes: usize,
    bytes: usize,
    insert_order: VecDeque<Vec<u16>>,
    entries: HashMap<Vec<u16>, Arc<SparseNgramCounts>>,
}

impl QueryNgramCache {
    pub fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            max_entries,
            max_bytes,
            bytes: 0,
            insert_order: VecDeque::new(),
            entries: HashMap::new(),
        }
    }

    pub fn has_exact(&self, key: &[u16]) -> bool {
        self.entries.contains_key(key)
    }

    pub fn get_closest_ref(
        &self,
        key: &[u16],
        missing: &mut Vec<u16>,
    ) -> Option<Arc<SparseNgramCounts>> {
        if let Some(exact) = self.entries.get(key) {
            missing.clear();
            return Some(exact.clone());
        }

        let mut best_subset: Option<&Vec<u16>> = None;
        for cached_key in self.entries.keys() {
            if is_subset(cached_key, key)
                && best_subset
                    .map(|existing| cached_key.len() > existing.len())
                    .unwrap_or(true)
            {
                best_subset = Some(cached_key);
            }
        }

        if let Some(best_key) = best_subset {
            missing_elements(best_key, key, missing);
            return self.entries.get(best_key).cloned();
        }

        missing.clear();
        missing.extend_from_slice(key);
        None
    }

    pub fn put(&mut self, key: &[u16], counts: Arc<SparseNgramCounts>) {
        let entry_bytes = counts.approx_bytes();
        if self.max_entries == 0
            || self.max_bytes == 0
            || entry_bytes > self.max_bytes
            || self.entries.contains_key(key)
        {
            return;
        }

        let owned_key = key.to_vec();
        self.insert_order.push_back(owned_key.clone());
        self.entries.insert(owned_key, counts);
        self.bytes = self.bytes.saturating_add(entry_bytes);

        while self.entries.len() > self.max_entries || self.bytes > self.max_bytes {
            let Some(oldest) = self.insert_order.pop_front() else {
                break;
            };
            if let Some(removed) = self.entries.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(removed.approx_bytes());
            }
        }
    }

    pub fn clear(&mut self) {
        self.insert_order.clear();
        self.entries.clear();
        self.bytes = 0;
    }
}

pub struct GuessContext {
    pub string_match_counts: Vec<u8>,
    pub string_match_epochs: Vec<u32>,
    pub touched_string_indices: Vec<u32>,
    pub string_matches: Vec<CosSimMatch>,
    pub query_bigrams: Vec<u16>,
    pub query_cache: QueryNgramCache,
    pub query_cache_version: String,
    pub query_epoch: u32,
    pub area_match_cache: Vec<(f32, u8, u8, u32)>,
    pub sift4_offset_arr: Vec<cypress::models::sift4::SiftOffset>,
    pub place_scores: Vec<(u32, f32)>,
}

impl GuessContext {
    pub fn new(string_count: usize, _place_count: usize) -> Self {
        Self {
            string_match_counts: vec![0; string_count],
            string_match_epochs: vec![0; string_count],
            touched_string_indices: Vec::with_capacity(16_384),
            string_matches: Vec::with_capacity(6000),
            query_bigrams: Vec::with_capacity(64),
            query_cache: QueryNgramCache::new(QUERY_CACHE_MAX_ENTRIES, QUERY_CACHE_MAX_BYTES),
            query_cache_version: String::new(),
            query_epoch: 1,
            area_match_cache: Vec::new(),
            sift4_offset_arr: Vec::new(),
            place_scores: Vec::new(),
        }
    }

    pub fn clear(
        &mut self,
        needed_string_count: usize,
        _needed_place_count: usize,
        index_version: &str,
    ) {
        if self.query_cache_version != index_version {
            self.query_cache.clear();
            self.query_cache_version.clear();
            self.query_cache_version.push_str(index_version);
        }

        if self.string_match_counts.len() != needed_string_count {
            self.string_match_counts.resize(needed_string_count, 0);
            self.string_match_epochs.resize(needed_string_count, 0);
        }

        self.touched_string_indices.clear();
        self.string_matches.clear();
        self.query_bigrams.clear();
        self.place_scores.clear();
        self.sift4_offset_arr.clear();

        self.query_epoch = self.query_epoch.wrapping_add(1);
        if self.query_epoch == 0 {
            // Epoch zero means "never touched". A wrap is extraordinarily rare,
            // but reset stamp-backed caches so stale entries cannot become visible.
            self.string_match_epochs.fill(0);
            self.area_match_cache.fill((0.0, 0, 0, 0));
            self.query_epoch = 1;
        }
    }

    #[inline]
    pub fn increment_string_match(&mut self, string_idx: usize) {
        if self.string_match_epochs[string_idx] != self.query_epoch {
            self.string_match_epochs[string_idx] = self.query_epoch;
            self.string_match_counts[string_idx] = 1;
            self.touched_string_indices.push(string_idx as u32);
        } else {
            self.string_match_counts[string_idx] =
                self.string_match_counts[string_idx].saturating_add(1);
        }
    }

    #[inline]
    pub fn string_match_count(&self, string_idx: usize) -> u8 {
        if self.string_match_epochs[string_idx] == self.query_epoch {
            self.string_match_counts[string_idx]
        } else {
            0
        }
    }

    pub fn restore_sparse_counts(&mut self, cached: &SparseNgramCounts) {
        let len = cached.string_ids.len().min(cached.counts.len());
        for i in 0..len {
            let string_idx = cached.string_ids[i] as usize;
            let count = cached.counts[i];
            if string_idx >= self.string_match_counts.len() || count == 0 {
                continue;
            }
            self.string_match_epochs[string_idx] = self.query_epoch;
            self.string_match_counts[string_idx] = count;
            self.touched_string_indices.push(string_idx as u32);
        }
    }

    pub fn snapshot_sparse_counts(&self) -> SparseNgramCounts {
        let mut string_ids = Vec::with_capacity(self.touched_string_indices.len());
        let mut counts = Vec::with_capacity(self.touched_string_indices.len());
        for &string_id in &self.touched_string_indices {
            let string_idx = string_id as usize;
            let count = self.string_match_count(string_idx);
            if count != 0 {
                string_ids.push(string_id);
                counts.push(count);
            }
        }
        SparseNgramCounts { string_ids, counts }
    }
}

thread_local! {
    pub static GUESS_CONTEXT: std::cell::RefCell<GuessContext> = std::cell::RefCell::new(GuessContext::new(0, 0));
}

pub struct MemdbData {
    pub index_mmap: Mmap,
    pub places_mmap: Mmap,
    pub version: String,
}

impl MemdbData {
    pub fn get_archived(&self) -> &ArchivedCypressMemDb {
        unsafe { rkyv::archived_root::<CypressMemDb>(&self.index_mmap) }
    }

    pub fn get_place(&self, place_id: usize) -> Option<PlaceRecord> {
        let start = place_id.checked_mul(PLACE_RECORD_DISK_BYTES)?;
        let end = start.checked_add(PLACE_RECORD_DISK_BYTES)?;
        if end > self.places_mmap.len() {
            return None;
        }

        PlaceRecord::from_disk_bytes(&self.places_mmap[start..end])
    }

    pub fn get_place_source_id(&self, place_id: usize) -> Option<String> {
        let db = self.get_archived();
        let start = *db.place_source_id_offsets.get(place_id)? as usize;
        let end = *db.place_source_id_offsets.get(place_id + 1)? as usize;

        if start > end || end > db.place_source_id_bytes.len() {
            return None;
        }

        Some(String::from_utf8_lossy(&db.place_source_id_bytes[start..end]).into_owned())
    }

    pub fn get_string_name(&self, string_id: usize) -> Option<&[u8]> {
        let db = self.get_archived();
        let start = *db.string_name_offsets.get(string_id)? as usize;
        let end = *db.string_name_offsets.get(string_id + 1)? as usize;

        if start > end || end > db.string_name_bytes.len() {
            return None;
        }

        Some(&db.string_name_bytes[start..end])
    }
}

pub struct Memdb {
    data: ArcSwap<MemdbData>,
    out_dir: PathBuf,
}

impl Memdb {
    pub fn new(out_dir: impl AsRef<Path>) -> Result<Arc<Self>> {
        let out_dir = out_dir.as_ref().to_path_buf();

        let memdb = Arc::new(Self {
            data: ArcSwap::from_pointee(Self::load_data(&out_dir, String::new())?),
            out_dir,
        });

        // Start background hot-reload watcher
        let memdb_clone = memdb.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(5));
            loop {
                ticker.tick().await;
                if let Err(e) = memdb_clone.check_reload().await {
                    warn!("Memdb reload check failed: {}", e);
                }
            }
        });

        Ok(memdb)
    }

    fn load_data(out_dir: &Path, version: String) -> Result<MemdbData> {
        info!("Loading memdb version: {}", version);
        let index_file = File::open(out_dir.join("cypress_index.bin"))
            .context("Failed to open cypress_index.bin")?;
        let places_file = File::open(out_dir.join("cypress_places.bin"))
            .context("Failed to open cypress_places.bin")?;

        let index_mmap = unsafe { Mmap::map(&index_file)? };
        let places_mmap = unsafe { Mmap::map(&places_file)? };

        if places_mmap.len() % PLACE_RECORD_DISK_BYTES != 0 {
            return Err(anyhow::anyhow!(
                "Invalid cypress_places.bin length: {} bytes is not divisible by {}",
                places_mmap.len(),
                PLACE_RECORD_DISK_BYTES
            ));
        }

        Ok(MemdbData {
            index_mmap,
            places_mmap,
            version,
        })
    }

    async fn check_reload(&self) -> Result<()> {
        let version_file = self.out_dir.join("current_version.txt");
        if !version_file.exists() {
            return Ok(());
        }

        let current_version = tokio::fs::read_to_string(&version_file)
            .await?
            .trim()
            .to_string();
        let active_version = self.data.load().version.clone();

        if current_version != active_version {
            info!(
                "Detected new memdb version: {} -> {}",
                active_version, current_version
            );

            // Do the blocking map file reads in spawn_blocking
            let out_dir = self.out_dir.clone();
            let new_data =
                tokio::task::spawn_blocking(move || Self::load_data(&out_dir, current_version))
                    .await??;

            self.data.store(Arc::new(new_data));
            info!("Hot reload complete!");
        }

        Ok(())
    }

    /// Read the active database instance
    pub fn get_data(&self) -> arc_swap::Guard<Arc<MemdbData>> {
        self.data.load()
    }
}
