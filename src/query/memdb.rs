use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use memmap2::Mmap;
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

pub struct GuessContext {
    pub string_match_counts: Vec<u16>,
    pub string_matches: Vec<CosSimMatch>,
}

impl GuessContext {
    pub fn new(size: usize) -> Self {
        Self {
            string_match_counts: vec![0; size],
            string_matches: Vec::with_capacity(6000),
        }
    }

    pub fn clear(&mut self, needed_size: usize) {
        if self.string_match_counts.len() < needed_size {
            self.string_match_counts.resize(needed_size, 0);
        } else {
            self.string_match_counts.fill(0);
        }
        self.string_matches.clear();
    }
}

thread_local! {
    pub static GUESS_CONTEXT: std::cell::RefCell<GuessContext> = std::cell::RefCell::new(GuessContext::new(0));
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
