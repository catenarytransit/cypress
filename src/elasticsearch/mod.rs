//! Elasticsearch client and operations.

mod bulk;
mod client;
mod schema;

pub use bulk::BulkIndexer;
pub use client::EsClient;
pub use schema::create_index;

use crate::models::Place;

/// Trait for documents that can be indexed into Elasticsearch
pub trait EsDocument: serde::Serialize + Send + Sync + 'static {
    /// Get the unique document ID
    fn id(&self) -> &str;
}

impl EsDocument for Place {
    fn id(&self) -> &str {
        &self.source_id
    }
}
