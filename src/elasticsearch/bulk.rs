//! Bulk indexing operations for Elasticsearch.

use anyhow::{Context, Result};
use elasticsearch::http::request::JsonBody;
use elasticsearch::BulkParts;
use tracing::{debug, warn};

use super::EsClient;
use crate::models::Place;

/// Bulk indexer for efficient document insertion
pub struct BulkIndexer {
    client: EsClient,
    batch_size: usize,
    buffer: Vec<Place>,
    total_indexed: usize,
    total_errors: usize,
}

impl BulkIndexer {
    /// Create a new bulk indexer
    pub fn new(client: EsClient, batch_size: usize) -> Self {
        Self {
            client,
            batch_size,
            buffer: Vec::with_capacity(batch_size),
            total_indexed: 0,
            total_errors: 0,
        }
    }

    /// Add a document to the buffer, flushing if batch is full
    pub async fn add(&mut self, place: Place) -> Result<()> {
        self.buffer.push(place);

        if self.buffer.len() >= self.batch_size {
            self.flush().await?;
        }

        Ok(())
    }

    /// Flush the buffer to Elasticsearch
    pub async fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let docs = std::mem::take(&mut self.buffer);
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
        let response = self
            .client
            .client()
            .bulk(BulkParts::Index(&self.client.index_name))
            .body(body)
            .send()
            .await
            .context("Bulk request failed")?;

        let response_body = response.json::<serde_json::Value>().await?;

        // Check for errors
        if response_body["errors"].as_bool().unwrap_or(false) {
            let items = response_body["items"].as_array();
            if let Some(items) = items {
                let error_count = items
                    .iter()
                    .filter(|item| item["index"]["error"].is_object())
                    .count();
                self.total_errors += error_count;
                warn!(
                    "Bulk request had {} errors out of {} documents",
                    error_count, count
                );
            }
        }

        self.total_indexed += count;
        self.buffer = Vec::with_capacity(self.batch_size);

        Ok(())
    }

    /// Finish indexing and return statistics
    pub async fn finish(mut self) -> Result<(usize, usize)> {
        self.flush().await?;
        Ok((self.total_indexed, self.total_errors))
    }

    /// Get current statistics
    pub fn stats(&self) -> (usize, usize) {
        (self.total_indexed, self.total_errors)
    }
}
