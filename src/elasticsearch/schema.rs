//! Elasticsearch index schema management.

use anyhow::{Context, Result};
use elasticsearch::indices::{IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts};
use tracing::info;

use super::EsClient;

/// Schema JSON embedded at compile time
const PLACES_MAPPING: &str = include_str!("../../schema/places_mapping.json");

/// Create the places index with proper mapping
pub async fn create_index(client: &EsClient, delete_existing: bool) -> Result<()> {
    let es = client.client();
    let index_name = &client.index_name;

    // Check if index exists
    let exists = es
        .indices()
        .exists(IndicesExistsParts::Index(&[index_name]))
        .send()
        .await?
        .status_code()
        .is_success();

    if exists {
        if delete_existing {
            info!("Deleting existing index: {}", index_name);
            es.indices()
                .delete(IndicesDeleteParts::Index(&[index_name]))
                .send()
                .await
                .context("Failed to delete existing index")?;
        } else {
            info!("Index {} already exists, skipping creation", index_name);
            return Ok(());
        }
    }

    // Parse the mapping JSON
    let mapping: serde_json::Value =
        serde_json::from_str(PLACES_MAPPING).context("Failed to parse places_mapping.json")?;

    // Create the index
    info!("Creating index: {}", index_name);
    let response = es
        .indices()
        .create(IndicesCreateParts::Index(index_name))
        .body(mapping)
        .send()
        .await
        .context("Failed to create index")?;

    if !response.status_code().is_success() {
        let error_body = response.text().await?;
        anyhow::bail!("Failed to create index: {}", error_body);
    }

    info!("Index {} created successfully", index_name);
    Ok(())
}
