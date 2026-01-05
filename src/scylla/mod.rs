use anyhow::{Context, Result};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
// use scylla::IntoTypedRows; - Removed broken import
use scylla::response::query_result::QueryResult;
use std::sync::Arc;
use tracing::info;

#[derive(Clone)]
pub struct ScyllaClient {
    session: Arc<Session>,
}

impl ScyllaClient {
    pub async fn new(uri: &str) -> Result<Self> {
        info!("Connecting to ScyllaDB at {}...", uri);
        let session: Session = SessionBuilder::new()
            .known_node(uri)
            .build()
            .await
            .context("Failed to connect to ScyllaDB")?;

        let client = Self {
            session: Arc::new(session),
        };

        client.init_schema().await?;
        Ok(client)
    }

    async fn init_schema(&self) -> Result<()> {
        // Create keyspace if not exists
        self.session
            .query_unpaged(
                "CREATE KEYSPACE IF NOT EXISTS cypress 
                 WITH REPLICATION = { 
                    'class' : 'SimpleStrategy', 
                    'replication_factor' : 1 
                 }",
                &[],
            )
            .await?;

        // Create places table
        self.session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS cypress.places (
                    id text PRIMARY KEY,
                    data text
                )",
                &[],
            )
            .await?;

        // Create admin_areas table
        self.session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS cypress.admin_areas (
                    id text PRIMARY KEY,
                    data text
                )",
                &[],
            )
            .await?;

        Ok(())
    }

    pub async fn upsert_place(&self, id: &str, data: &str) -> Result<()> {
        self.session
            .query_unpaged(
                "INSERT INTO cypress.places (id, data) VALUES (?, ?)",
                (id, data),
            )
            .await?;
        Ok(())
    }

    pub async fn upsert_admin_area(&self, id: &str, data: &str) -> Result<()> {
        self.session
            .query_unpaged(
                "INSERT INTO cypress.admin_areas (id, data) VALUES (?, ?)",
                (id, data),
            )
            .await?;
        Ok(())
    }

    pub async fn get_place(&self, id: &str) -> Result<Option<String>> {
        let result: QueryResult = self
            .session
            .query_unpaged("SELECT data FROM cypress.places WHERE id = ?", (id,))
            .await?;

        if let Ok(rows_result) = result.into_rows_result() {
            if let Some((data,)) = rows_result.maybe_first_row::<(String,)>()? {
                return Ok(Some(data));
            }
        }

        Ok(None)
    }

    pub async fn get_admin_areas(
        &self,
        ids: &[String],
    ) -> Result<std::collections::HashMap<String, String>> {
        if ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        // Prepare query with IN clause
        // Note: Scylla/Cassandra IN clause has limits, but for admin hierarchy (max 10 items) it's fine.
        let placeholders: Vec<String> = ids.iter().map(|_| "?".to_string()).collect();
        let query = format!(
            "SELECT id, data FROM cypress.admin_areas WHERE id IN ({})",
            placeholders.join(", ")
        );

        let result = self.session.query_unpaged(query, ids).await?;
        let mut map = std::collections::HashMap::new();

        if let Ok(rows_result) = result.into_rows_result() {
            for row_res in rows_result.rows::<(String, String)>()? {
                if let Ok((id, data)) = row_res {
                    map.insert(id, data);
                }
            }
        }

        Ok(map)
    }
}
