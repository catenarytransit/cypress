//! Source-scoped immutable place generations. Importers write only into
//! inactive generations; compilers read a snapshot of active source pointers.
use super::ScyllaClient;
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use futures::{stream, StreamExt, TryStreamExt};
use scylla::response::query_result::QueryResult;
use scylla::value::{CqlValue, Row};
use tracing::{info, warn};
use uuid::Uuid;
use xxhash_rust::xxh64::xxh64;

pub const V2_BUCKETS: i16 = 256;
/// Compiled place ID -> source generation lookup, aligned with cypress_places.bin.
pub const V2_LOOKUP_FILE: &str = "cypress_v2_lookup.bin";
pub const V2_LOOKUP_MAGIC: &[u8; 8] = b"CYV2LOOK";
const GC_CONCURRENCY: usize = 8;
const ABANDONED_AFTER_HOURS: i64 = 2;
const GC_GRACE_HOURS: i64 = 48;

#[derive(Debug, Clone)]
pub struct ActiveSource {
    pub source_file: String,
    pub generation: Uuid,
}

#[derive(Debug, Clone)]
pub struct V2Import {
    pub source_file: String,
    pub generation: Uuid,
}

pub fn bucket_for_id(id: &str) -> i16 {
    (xxh64(id.as_bytes(), 0) % V2_BUCKETS as u64) as i16
}

fn was_applied(result: QueryResult) -> Result<bool> {
    let row = result.into_rows_result()?.first_row::<Row>()?;
    row.columns
        .first()
        .and_then(Option::as_ref)
        .and_then(CqlValue::as_boolean)
        .context("Conditional source update did not return [applied]")
}

impl ScyllaClient {
    /// Direct, partition-key-complete read against a published generation.
    pub async fn get_place_v2_from_source(
        &self,
        id: &str,
        source: &ActiveSource,
    ) -> Result<Option<String>> {
        let result = self.session.query_unpaged(
            "SELECT data FROM cypress.places_v2
             WHERE source_file = ? AND generation = ? AND bucket = ? AND id = ?",
            (&source.source_file, source.generation, bucket_for_id(id), id),
        ).await?;
        Ok(result.into_rows_result()?.maybe_first_row::<(String,)>()?.map(|row| row.0))
    }

    pub(crate) async fn init_places_v2_schema(&self) -> Result<()> {
        for ddl in [
            "CREATE TABLE IF NOT EXISTS cypress.places_v2 (
                source_file text, generation uuid, bucket smallint, id text,
                data text, population int,
                PRIMARY KEY ((source_file, generation, bucket), id)
            ) WITH compression = { 'sstable_compression': 'ZstdCompressor', 'chunk_length_in_kb': 64 }",
            "CREATE TABLE IF NOT EXISTS cypress.place_sources_v2 (
                source_file text PRIMARY KEY, active_generation uuid,
                pending_generation uuid, heartbeat timestamp
            )",
            "CREATE TABLE IF NOT EXISTS cypress.place_import_runs_v2 (
                source_file text, generation uuid, state text,
                started_at timestamp, finished_at timestamp, written bigint,
                PRIMARY KEY ((source_file), generation)
            )",
        ] {
            self.session.query_unpaged(ddl, &[]).await?;
        }
        Ok(())
    }

    async fn source_state(
        &self,
        source: &str,
    ) -> Result<Option<(Option<Uuid>, Option<Uuid>, Option<DateTime<Utc>>)>> {
        let result = self.session.query_unpaged(
            "SELECT active_generation, pending_generation, heartbeat
             FROM cypress.place_sources_v2 WHERE source_file = ?",
            (source,),
        ).await?;
        Ok(result.into_rows_result()?.maybe_first_row::<(
            Option<Uuid>, Option<Uuid>, Option<DateTime<Utc>>,
        )>()?)
    }

    /// Acquires a per-source fenced import slot. Stale slots are reclaimed only
    /// after their heartbeat expires, and the abandoned generation is never
    /// eligible for publication by its old worker.
    pub async fn begin_place_import(&self, source: &str) -> Result<V2Import> {
        let generation = Uuid::new_v4();
        let now = Utc::now();
        let created = was_applied(self.session.query_unpaged(
            "INSERT INTO cypress.place_sources_v2
             (source_file, pending_generation, heartbeat)
             VALUES (?, ?, ?) IF NOT EXISTS",
            (source, generation, now),
        ).await?)?;
        if !created {
            if let Some((_, Some(pending), heartbeat)) = self.source_state(source).await? {
                let expired = heartbeat
                    .map(|at| at < now - Duration::hours(ABANDONED_AFTER_HOURS))
                    .unwrap_or(true);
                if !expired {
                    bail!("An import is already running for {source} ({pending})");
                }
                let reclaimed = was_applied(self.session.query_unpaged(
                    "UPDATE cypress.place_sources_v2
                     SET pending_generation = null, heartbeat = null
                     WHERE source_file = ? IF pending_generation = ?",
                    (source, pending),
                ).await?)?;
                if !reclaimed {
                    bail!("Import ownership changed while recovering {source}");
                }
                self.session.query_unpaged(
                    "UPDATE cypress.place_import_runs_v2
                     SET state = 'failed', finished_at = ?
                     WHERE source_file = ? AND generation = ?",
                    (now, source, pending),
                ).await?;
                warn!(source, %pending, "Recovered abandoned place import");
            }
            let acquired = was_applied(self.session.query_unpaged(
                "UPDATE cypress.place_sources_v2
                 SET pending_generation = ?, heartbeat = ?
                 WHERE source_file = ? IF pending_generation = null",
                (generation, now, source),
            ).await?)?;
            if !acquired {
                bail!("Import lock is held for {source}");
            }
        }
        self.session.query_unpaged(
            "INSERT INTO cypress.place_import_runs_v2
             (source_file, generation, state, started_at, written)
             VALUES (?, ?, 'importing', ?, 0)",
            (source, generation, now),
        ).await?;
        Ok(V2Import { source_file: source.to_owned(), generation })
    }

    /// Called periodically even while expensive geometry/road processing runs.
    pub async fn heartbeat_place_import(&self, run: &V2Import) -> Result<()> {
        let applied = was_applied(self.session.query_unpaged(
            "UPDATE cypress.place_sources_v2 SET heartbeat = ?
             WHERE source_file = ? IF pending_generation = ?",
            (Utc::now(), &run.source_file, run.generation),
        ).await?)?;
        if !applied {
            bail!("Lost ownership of import {} ({})", run.source_file, run.generation);
        }
        Ok(())
    }

    pub async fn upsert_place_v2(
        &self, run: &V2Import, id: &str, data: &str, population: Option<u32>,
    ) -> Result<()> {
        let population_i32 = population.and_then(|value| i32::try_from(value).ok());
        self.session.query_unpaged(
            "INSERT INTO cypress.places_v2
             (source_file, generation, bucket, id, data, population)
             VALUES (?, ?, ?, ?, ?, ?)",
            (&run.source_file, run.generation, bucket_for_id(id), id, data, population_i32),
        ).await?;
        Ok(())
    }

    /// Publish only after all writes succeed; a failed importer cannot publish
    /// after losing its lock. Old generations remain for readers and GC.
    pub async fn publish_place_import(&self, run: &V2Import, written: u64) -> Result<()> {
        self.heartbeat_place_import(run).await?;
        let now = Utc::now();
        self.session.query_unpaged(
            "UPDATE cypress.place_import_runs_v2
             SET state = 'complete', finished_at = ?, written = ?
             WHERE source_file = ? AND generation = ?",
            (now, written as i64, &run.source_file, run.generation),
        ).await?;
        let applied = was_applied(self.session.query_unpaged(
            "UPDATE cypress.place_sources_v2
             SET active_generation = ?, pending_generation = null, heartbeat = null
             WHERE source_file = ? IF pending_generation = ?",
            (run.generation, &run.source_file, run.generation),
        ).await?)?;
        if !applied {
            bail!("Failed to publish {}: import slot changed", run.source_file);
        }
        info!(source = %run.source_file, generation = %run.generation, written,
            "Published places_v2 generation");
        Ok(())
    }

    pub async fn fail_place_import(&self, run: &V2Import) -> Result<()> {
        self.session.query_unpaged(
            "UPDATE cypress.place_import_runs_v2
             SET state = 'failed', finished_at = ?
             WHERE source_file = ? AND generation = ?",
            (Utc::now(), &run.source_file, run.generation),
        ).await?;
        // Do not release the slot if another worker has already reclaimed it.
        let _ = was_applied(self.session.query_unpaged(
            "UPDATE cypress.place_sources_v2
             SET pending_generation = null, heartbeat = null
             WHERE source_file = ? IF pending_generation = ?",
            (&run.source_file, run.generation),
        ).await?)?;
        Ok(())
    }

    pub async fn active_place_sources(&self) -> Result<Vec<ActiveSource>> {
        let mut rows = self.session.query_iter(
            "SELECT source_file, active_generation FROM cypress.place_sources_v2", &[],
        ).await?.rows_stream::<(String, Option<Uuid>)>()?;
        let mut sources = Vec::new();
        while let Some(row) = rows.next().await {
            let (source_file, generation) = row?;
            if let Some(generation) = generation {
                sources.push(ActiveSource { source_file, generation });
            }
        }
        // Deterministic duplicate resolution for shared OSM IDs across extracts.
        sources.sort_unstable_by(|a, b| a.source_file.cmp(&b.source_file));
        Ok(sources)
    }

    /// Deletes inactive generations via partition tombstones, not row-level LWTs.
    /// A 48-hour grace period protects compilations holding an earlier snapshot.
    /// Repeatable after partial deletion or power loss.
    pub async fn garbage_collect_places_v2(&self) -> Result<u64> {
        let mut rows = self.session.query_iter(
            "SELECT source_file, generation, state, started_at, finished_at
             FROM cypress.place_import_runs_v2", &[],
        ).await?.rows_stream::<(
            String, Uuid, String, DateTime<Utc>, Option<DateTime<Utc>>,
        )>()?;
        let cutoff = Utc::now() - Duration::hours(GC_GRACE_HOURS);
        let mut old = Vec::new();
        while let Some(row) = rows.next().await {
            let (source, generation, state, started, finished) = row?;
            if state == "importing" && finished.is_none() {
                let current = self.source_state(&source).await?;
                if let Some((_, Some(pending), heartbeat)) = current {
                    if pending == generation && heartbeat.is_some_and(|t| t > Utc::now() - Duration::hours(ABANDONED_AFTER_HOURS)) {
                        continue;
                    }
                    if pending == generation {
                        let reclaimed = was_applied(self.session.query_unpaged(
                            "UPDATE cypress.place_sources_v2 SET pending_generation = null, heartbeat = null WHERE source_file = ? IF pending_generation = ?",
                            (&source, generation),
                        ).await?)?;
                        if !reclaimed { continue; }
                    }
                }
                self.session.query_unpaged(
                    "UPDATE cypress.place_import_runs_v2 SET state = 'failed', finished_at = ? WHERE source_file = ? AND generation = ?",
                    (Utc::now(), &source, generation),
                ).await?;
                // Retain for the same grace period as other inactive generations.
                continue;
            }
            if finished.unwrap_or(started) <= cutoff {
                old.push((source, generation));
            }
        }
        let mut removed = 0u64;
        for (source, generation) in old {
            let state = self.source_state(&source).await?;
            if state.is_some_and(|(active, pending, _)| {
                active == Some(generation) || pending == Some(generation)
            }) {
                continue;
            }
            stream::iter(0..V2_BUCKETS)
                .map(|bucket| {
                    let source = &source;
                    async move {
                    self.session.query_unpaged(
                        "DELETE FROM cypress.places_v2
                         WHERE source_file = ? AND generation = ? AND bucket = ?",
                        (&source, generation, bucket),
                    ).await.context("Failed to delete obsolete places_v2 partition")?;
                    Ok::<_, anyhow::Error>(())
                    }
                })
                .buffer_unordered(GC_CONCURRENCY)
                .try_collect::<Vec<_>>().await?;
            self.session.query_unpaged(
                "DELETE FROM cypress.place_import_runs_v2
                 WHERE source_file = ? AND generation = ?",
                (&source, generation),
            ).await?;
            removed += 1;
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bucket_is_stable_and_bounded() {
        let first = bucket_for_id("node/123456789");
        assert_eq!(first, bucket_for_id("node/123456789"));
        assert!((0..V2_BUCKETS).contains(&first));
    }
}
