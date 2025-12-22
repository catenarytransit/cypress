use anyhow::{Context, Result};
use cypress::elasticsearch::EsClient;
use elasticsearch::indices::{IndicesCreateParts, IndicesExistsParts};
use elasticsearch::SearchParts;
use serde::{Deserialize, Serialize};
use std::fs::File;

use std::io::Read;
use std::path::Path;
use tracing::info;
use xxhash_rust::xxh64::Xxh64;

#[derive(Debug, Serialize, Deserialize)]
pub struct VersionDoc {
    pub region_name: String,
    pub filename: String,
    pub hash: String,
    pub timestamp: String,
}

pub struct VersionManager {
    es_client: EsClient,
    index_name: String,
}

impl VersionManager {
    pub async fn new(es_url: &str) -> Result<Self> {
        let index_name = "cypress_versions".to_string();
        let es_client = EsClient::new(es_url, &index_name)
            .await
            .context("Failed to connect to ES for version manager")?;

        let manager = Self {
            es_client,
            index_name,
        };

        manager.ensure_index().await?;

        Ok(manager)
    }

    async fn ensure_index(&self) -> Result<()> {
        let es = self.es_client.client();
        let exists = es
            .indices()
            .exists(IndicesExistsParts::Index(&[&self.index_name]))
            .send()
            .await?
            .status_code()
            .is_success();

        if !exists {
            info!("Creating version index: {}", self.index_name);
            let mapping = serde_json::json!({
                "mappings": {
                    "properties": {
                        "region_name": { "type": "keyword" },
                        "filename": { "type": "keyword" },
                        "hash": { "type": "keyword" },
                        "timestamp": { "type": "date" }
                    }
                }
            });

            es.indices()
                .create(IndicesCreateParts::Index(&self.index_name))
                .body(mapping)
                .send()
                .await
                .context("Failed to create version index")?;
        }
        Ok(())
    }

    pub async fn is_latest(&self, region_name: &str, filename: &str, hash: &str) -> Result<bool> {
        let query = serde_json::json!({
            "query": {
                "bool": {
                    "must": [
                        { "term": { "region_name": region_name } },
                        { "term": { "filename": filename } },
                        { "term": { "hash": hash } }
                    ]
                }
            }
        });

        let response = self
            .es_client
            .client()
            .search(SearchParts::Index(&[&self.index_name]))
            .body(query)
            .send()
            .await?;

        let body = response.json::<serde_json::Value>().await?;
        let hits = body["hits"]["total"]["value"].as_u64().unwrap_or(0);
        Ok(hits > 0)
    }

    pub async fn save_version(&self, version: VersionDoc) -> Result<()> {
        self.es_client
            .client()
            .index(elasticsearch::IndexParts::Index(&self.index_name))
            .body(version)
            .send()
            .await?;
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
