//! Bulk indexing operations for Elasticsearch.

use anyhow::{Context, Result};
use elasticsearch::http::request::JsonBody;
use elasticsearch::BulkParts;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::EsClient;
use crate::models::Place;

/// Bulk indexer that runs in a background task
pub struct BulkIndexer {
    sender: mpsc::Sender<Place>,
    handle: JoinHandle<Result<(usize, usize)>>,
}

impl BulkIndexer {
    /// Create a new bulk indexer
    pub fn new(client: EsClient, batch_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(batch_size * 2);

        let handle = tokio::spawn(run_indexer(client, rx, batch_size));

        Self { sender: tx, handle }
    }

    /// Add a document to the indexing queue
    pub async fn add(&self, place: Place) -> Result<()> {
        self.sender
            .send(place)
            .await
            .map_err(|_| anyhow::anyhow!("Indexer task closed unexpectedly"))
    }

    /// Get a clone of the sender to send from other tasks
    pub fn sender_clone(&self) -> mpsc::Sender<Place> {
        self.sender.clone()
    }

    /// Finish indexing and return statistics
    pub async fn finish(self) -> Result<(usize, usize)> {
        // Drop sender to signal completion
        drop(self.sender);

        // Wait for background task
        self.handle.await?
    }
}

/// Background task loop
async fn run_indexer(
    client: EsClient,
    mut rx: mpsc::Receiver<Place>,
    batch_size: usize,
) -> Result<(usize, usize)> {
    let mut buffer = Vec::with_capacity(batch_size);
    let mut total_indexed = 0;
    let mut total_errors = 0;

    while let Some(place) = rx.recv().await {
        buffer.push(place);

        if buffer.len() >= batch_size {
            match flush(&client, &mut buffer).await {
                Ok((indexed, errors)) => {
                    total_indexed += indexed;
                    total_errors += errors;
                }
                Err(e) => {
                    error!("Failed to flush batch: {}", e);
                    // Decide whether to abort or continue. Usually we want to continue?
                    // But if ES is down, we might loop errors.
                    // For now, let's just log and continue, but maybe count them as errors?
                    // Actually flush returns Result. If it fails (network), we might want to retry or bail.
                    return Err(e);
                }
            }
        }
    }

    // Flush remaining
    if !buffer.is_empty() {
        let (indexed, errors) = flush(&client, &mut buffer).await?;
        total_indexed += indexed;
        total_errors += errors;
    }

    info!(
        "Bulk indexer finished. Total: {}, Errors: {}",
        total_indexed, total_errors
    );
    Ok((total_indexed, total_errors))
}

/// Flush the buffer to Elasticsearch
async fn flush(client: &EsClient, buffer: &mut Vec<Place>) -> Result<(usize, usize)> {
    if buffer.is_empty() {
        return Ok((0, 0));
    }

    let docs = std::mem::take(buffer);
    let count = docs.len();

    debug!("Flushing {} documents to Elasticsearch", count);

    // Build bulk request body as Vec of JsonBody
    let mut body: Vec<JsonBody<serde_json::Value>> = Vec::with_capacity(count * 2);

    for doc in &docs {
        // Action line
        body.push(
            serde_json::json!({
                "index": {
                    "_id": &doc.source_id
                }
            })
            .into(),
        );
        // Document line
        body.push(serde_json::to_value(doc)?.into());
    }

    // Send bulk request
    let response = client
        .client()
        .bulk(BulkParts::Index(&client.index_name))
        .body(body)
        .send()
        .await
        .context("Bulk request failed")?;

    let response_body = response.json::<serde_json::Value>().await?;
    let mut batch_errors = 0;

    // Check for errors
    if response_body["errors"].as_bool().unwrap_or(false) {
        let items = response_body["items"].as_array();
        if let Some(items) = items {
            let errors: Vec<_> = items
                .iter()
                .filter(|item| item["index"]["error"].is_object())
                .collect();

            batch_errors = errors.len();
            warn!(
                "Bulk request had {} errors out of {} documents",
                batch_errors, count
            );

            // Log the first 5 errors to help debugging
            for (i, error_item) in errors.iter().take(5).enumerate() {
                let error = &error_item["index"]["error"];
                let doc_id = &error_item["index"]["_id"];
                warn!(
                    "Error #{}: Doc ID: {:?}, Reason: {:?}",
                    i + 1,
                    doc_id,
                    error
                );
            }
        }
    }

    // Prepare buffer for next batch
    // We moved it out, so buffer is empty (std::mem::take).
    // Just reserve capacity again to avoid reallocations on next push?
    // Actually run_indexer recreates it or we reuse it?
    // run_indexer has `buffer`. We passed `&mut buffer`. `take` replaced it with empty vec.
    // So buffer is empty now.
    buffer.reserve(count); // Optimistic preallocation

    Ok((count, batch_errors))
}
