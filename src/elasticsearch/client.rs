//! Elasticsearch client wrapper.

use anyhow::Result;
use elasticsearch::{
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    Elasticsearch,
};
use url::Url;

/// Elasticsearch client wrapper with connection configuration
#[derive(Clone)]
pub struct EsClient {
    client: Elasticsearch,
    pub index_name: String,
}

impl EsClient {
    /// Create a new Elasticsearch client
    pub async fn new(es_url: &str, index_name: &str) -> Result<Self> {
        let url = Url::parse(es_url)?;
        let conn_pool = SingleNodeConnectionPool::new(url);
        let transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;

        let client = Elasticsearch::new(transport);

        Ok(Self {
            client,
            index_name: index_name.to_string(),
        })
    }

    /// Get the underlying Elasticsearch client
    pub fn client(&self) -> &Elasticsearch {
        &self.client
    }

    /// Check if cluster is healthy
    pub async fn health_check(&self) -> Result<bool> {
        let response = self
            .client
            .cluster()
            .health(elasticsearch::cluster::ClusterHealthParts::None)
            .send()
            .await?;

        Ok(response.status_code().is_success())
    }

    /// Get document count in index
    pub async fn doc_count(&self) -> Result<u64> {
        let response = self
            .client
            .count(elasticsearch::CountParts::Index(&[&self.index_name]))
            .send()
            .await?;

        let body = response.json::<serde_json::Value>().await?;
        Ok(body["count"].as_u64().unwrap_or(0))
    }
}
