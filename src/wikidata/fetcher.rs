//! Wikidata label fetcher using SPARQL queries.

use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::{debug, info, warn};

const WIKIDATA_SPARQL_ENDPOINT: &str = "https://query.wikidata.org/sparql";

/// Fetches multilingual labels from Wikidata
pub struct WikidataFetcher {
    client: Client,
    /// Cache of Q-ID → language → label
    cache: HashMap<String, HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct SparqlResponse {
    results: SparqlResults,
}

#[derive(Debug, Deserialize)]
struct SparqlResults {
    bindings: Vec<SparqlBinding>,
}

#[derive(Debug, Deserialize)]
struct SparqlBinding {
    item: SparqlValue,
    #[serde(rename = "itemLabel")]
    label: SparqlValue,
}

#[derive(Debug, Deserialize)]
struct SparqlValue {
    value: String,
    #[serde(rename = "xml:lang")]
    lang: Option<String>,
}

impl WikidataFetcher {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .user_agent("Cypress/0.1 (geocoder; https://github.com/example)")
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .expect("Failed to create HTTP client"),
            cache: HashMap::new(),
        }
    }

    /// Fetch labels for a batch of Wikidata Q-IDs
    pub async fn fetch_batch(&mut self, qids: &[String]) -> Result<()> {
        if qids.is_empty() {
            return Ok(());
        }

        // Filter out already cached
        let to_fetch: Vec<&String> = qids
            .iter()
            .filter(|q| !self.cache.contains_key(*q))
            .collect();

        if to_fetch.is_empty() {
            return Ok(());
        }

        info!("Fetching labels for {} Wikidata items...", to_fetch.len());

        // Batch into chunks of 50 to avoid query limits
        for chunk in to_fetch.chunks(50) {
            self.fetch_chunk(chunk).await?;
            // Small delay to be nice to Wikidata
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        Ok(())
    }

    async fn fetch_chunk(&mut self, qids: &[&String]) -> Result<()> {
        // Build VALUES clause
        let values: String = qids
            .iter()
            .map(|q| format!("wd:{}", q))
            .collect::<Vec<_>>()
            .join(" ");

        let query = format!(
            r#"
            SELECT ?item ?itemLabel WHERE {{
                VALUES ?item {{ {} }}
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_ALL]". }}
            }}
        "#,
            values
        );

        let mut attempts = 0;
        let max_attempts = 2;

        while attempts < max_attempts {
            attempts += 1;

            let response = match self
                .client
                .get(WIKIDATA_SPARQL_ENDPOINT)
                .query(&[("query", &query), ("format", &"json".to_string())])
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "Wikidata SPARQL request failed (attempt {}/{}): {}",
                        attempts, max_attempts, e
                    );
                    if attempts < max_attempts {
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        continue;
                    }
                    return Ok(());
                }
            };

            if !response.status().is_success() {
                warn!(
                    "Wikidata query failed with status {} (attempt {}/{}): {:?}",
                    response.status(),
                    attempts,
                    max_attempts,
                    response.text().await.ok()
                );
                if attempts < max_attempts {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    continue;
                }
                return Ok(());
            }

            let data: SparqlResponse = match response.json().await {
                Ok(d) => d,
                Err(e) => {
                    warn!("Failed to parse Wikidata response: {}", e);
                    return Ok(());
                }
            };

            // Process results
            for binding in data.results.bindings {
                // Extract Q-ID from URI
                let qid = binding
                    .item
                    .value
                    .rsplit('/')
                    .next()
                    .unwrap_or("")
                    .to_string();

                if qid.is_empty() {
                    continue;
                }

                let lang = binding.label.lang.unwrap_or_else(|| "default".to_string());

                self.cache
                    .entry(qid)
                    .or_insert_with(HashMap::new)
                    .insert(lang, binding.label.value);
            }

            debug!("Fetched labels for {} items", qids.len());
            return Ok(());
        }

        Ok(())
    }

    /// Get cached labels for a Q-ID
    pub fn get_labels(&self, qid: &str) -> Option<&HashMap<String, String>> {
        self.cache.get(qid)
    }

    /// Merge Wikidata labels into existing name map (Wikidata fills gaps, doesn't override)
    pub fn merge_labels(&self, qid: &str, names: &mut HashMap<String, String>) {
        if let Some(labels) = self.cache.get(qid) {
            for (lang, label) in labels {
                // Only add if not already present from OSM
                names.entry(lang.clone()).or_insert_with(|| label.clone());
            }
        }
    }

    /// Get cache size
    pub fn cache_size(&self) -> usize {
        self.cache.len()
    }
}

impl Default for WikidataFetcher {
    fn default() -> Self {
        Self::new()
    }
}
