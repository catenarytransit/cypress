//! Integration test: CYPRESS_TEST_SCYLLA_URI=127.0.0.1:9042
//! cargo test --test places_v2 -- --ignored
use anyhow::Result;
use cypress::scylla::ScyllaClient;
use cypress::scylla::places_v2::bucket_for_id;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires dedicated Scylla and CYPRESS_TEST_SCYLLA_URI"]
async fn failed_import_does_not_change_active_generation() -> Result<()> {
    let uri = std::env::var("CYPRESS_TEST_SCYLLA_URI")?;
    let db = ScyllaClient::new(&uri).await?;
    let source = format!("v2-test-{}", Uuid::new_v4());
    let first = db.begin_place_import(&source).await?;
    db.upsert_place_v2(&first, "node/1", "first", None).await?;
    db.publish_place_import(&first, 1).await?;

    let failed = db.begin_place_import(&source).await?;
    db.upsert_place_v2(&failed, "node/2", "incomplete", None).await?;
    db.fail_place_import(&failed).await?;
    let active = db.active_place_sources().await?;
    assert_eq!(active.iter().find(|s| s.source_file == source).unwrap().generation, first.generation);

    let next = db.begin_place_import(&source).await?;
    db.upsert_place_v2(&next, "node/3", "next", None).await?;
    db.publish_place_import(&next, 1).await?;
    let active = db.active_place_sources().await?;
    assert_eq!(active.iter().find(|s| s.source_file == source).unwrap().generation, next.generation);

    let old = db.session.query_unpaged(
        "SELECT data FROM cypress.places_v2 WHERE source_file = ? AND generation = ? AND bucket = ? AND id = ?",
        (&source, first.generation, bucket_for_id("node/1"), "node/1"),
    ).await?.into_rows_result()?.maybe_first_row::<(String,)>()?;
    assert_eq!(old.map(|v| v.0).as_deref(), Some("first"));

    // Clean up this test namespace, independent of age-based production GC.
    for generation in [first.generation, failed.generation, next.generation] {
        for bucket in 0..256i16 {
            db.session.query_unpaged(
                "DELETE FROM cypress.places_v2 WHERE source_file = ? AND generation = ? AND bucket = ?",
                (&source, generation, bucket),
            ).await?;
        }
        db.session.query_unpaged(
            "DELETE FROM cypress.place_import_runs_v2 WHERE source_file = ? AND generation = ?",
            (&source, generation),
        ).await?;
    }
    db.session.query_unpaged(
        "DELETE FROM cypress.place_sources_v2 WHERE source_file = ?", (&source,),
    ).await?;
    Ok(())
}
