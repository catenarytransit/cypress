use anyhow::{Context, Result};
use cypress::scylla::ScyllaClient;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use tracing::info;
use xxhash_rust::xxh64::Xxh64;

#[derive(Debug, Serialize, Deserialize)]
pub struct VersionDoc {
    pub region_name: String,
    pub filename: String,
    pub hash: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

pub struct VersionManager {
    scylla_client: Arc<ScyllaClient>,
}

impl VersionManager {
    pub async fn new(scylla_url: &str) -> Result<Self> {
        let scylla_client = ScyllaClient::new(scylla_url).await?;
        let manager = Self {
            scylla_client: Arc::new(scylla_client),
        };

        manager.ensure_table().await?;
        Ok(manager)
    }

    async fn ensure_table(&self) -> Result<()> {
        let session = &self.scylla_client.session;
        session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS cypress.cypress_versions (
                region_name text,
                filename text,
                hash text,
                timestamp timestamp,
                PRIMARY KEY ((region_name, filename), hash)
            );",
                &[],
            )
            .await
            .context("Failed to ensure ScyllaDB version tracking table")?;
        session.query_unpaged(
            "CREATE TABLE IF NOT EXISTS cypress.latest_region_versions_v2 (
                region_name text, filename text, hash text, timestamp timestamp,
                PRIMARY KEY ((region_name), filename)
            )", &[],
        ).await?;
        Ok(())
    }

    pub async fn has_active_place_source(&self, source: &str) -> Result<bool> {
        Ok(self.scylla_client.active_place_sources().await?
            .iter().any(|entry| entry.source_file == source))
    }

    pub async fn is_latest(&self, region_name: &str, filename: &str, hash: &str) -> Result<bool> {
        let result = self.scylla_client.session.query_unpaged(
            "SELECT hash FROM cypress.latest_region_versions_v2 WHERE region_name = ? AND filename = ?",
            (region_name, filename),
        ).await?;
        let current = result.into_rows_result()?.maybe_first_row::<(String,)>()?;
        Ok(current.is_some_and(|(value,)| value == hash))
    }

    pub async fn save_version(&self, version: VersionDoc) -> Result<()> {
        let session = &self.scylla_client.session;
        let statement =
            "INSERT INTO cypress.cypress_versions (region_name, filename, hash, timestamp) \
                         VALUES (?, ?, ?, ?);";

        session
            .query_unpaged(
                statement,
                (
                    &version.region_name,
                    &version.filename,
                    &version.hash,
                    version.timestamp,
                ),
            )
            .await
            .context("Failed persisting processing metadata state verification sequence")?;
        session.query_unpaged(
            "INSERT INTO cypress.latest_region_versions_v2 (region_name, filename, hash, timestamp) VALUES (?, ?, ?, ?)",
            (&version.region_name, &version.filename, &version.hash, version.timestamp),
        ).await?;
        Ok(())
    }

    pub async fn reset(&self) -> Result<()> {
        let session = &self.scylla_client.session;
        info!("Truncating historical record layers stored inside cypress_versions");
        session
            .query_unpaged("TRUNCATE cypress.cypress_versions;", &[])
            .await
            .context("Failed to safely prune persistent storage elements")?;
        session.query_unpaged("TRUNCATE cypress.latest_region_versions_v2", &[]).await?;
        Ok(())
    }
}

pub fn calculate_file_hash<P: AsRef<Path>>(path: P) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Xxh64::new(0);
    let mut buffer = [0; 8192];

    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }

    Ok(format!("{:x}", hasher.digest()))
}
