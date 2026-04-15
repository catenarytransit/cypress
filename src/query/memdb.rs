use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use bytemuck::cast_slice;
use fst::Map;
use memmap2::Mmap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{info, warn};

use cypress::models::place::PlaceSummary;

pub struct MemdbData {
    pub map: Map<Mmap>,
    pub mmap_data: Mmap,
    pub version: String,
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
        let fst_file =
            File::open(out_dir.join("typeahead.fst")).context("Failed to open typeahead.fst")?;
        let fst_mmap = unsafe { Mmap::map(&fst_file)? };
        let map = Map::new(fst_mmap).context("Failed to parse FST map")?;

        let bin_file = File::open(out_dir.join("places_summary.bin"))
            .context("Failed to open places_summary.bin")?;
        let mmap_data = unsafe { Mmap::map(&bin_file)? };

        Ok(MemdbData {
            map,
            mmap_data,
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

    /// Helper to get a place by its array index
    pub fn get_summary(&self, index: u64) -> Option<PlaceSummary> {
        let data = self.get_data();
        let summaries: &[PlaceSummary] = cast_slice(&data.mmap_data);
        summaries.get(index as usize).copied()
    }
}
