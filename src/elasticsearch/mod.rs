//! Elasticsearch client and operations.

mod bulk;
mod client;
mod schema;

pub use bulk::BulkIndexer;
pub use client::EsClient;
pub use schema::create_index;
