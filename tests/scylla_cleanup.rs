//! Run against a dedicated ScyllaDB instance:
//! CYPRESS_TEST_SCYLLA_URI=127.0.0.1:9042 cargo test --test scylla_cleanup -- --ignored
use anyhow::Result;
use chrono::{Duration, Utc};
use cypress::scylla::ScyllaClient;
use futures::{stream, StreamExt, TryStreamExt};

#[tokio::test]
#[ignore = "requires a dedicated ScyllaDB instance and CYPRESS_TEST_SCYLLA_URI"]
async fn cleanup_pages_preserves_current_places_and_is_repeatable() -> Result<()> {
    let uri = std::env::var("CYPRESS_TEST_SCYLLA_URI")?;
    let client = ScyllaClient::new(&uri).await?;
    let source = format!("cleanup-test-{}", uuid::Uuid::new_v4());
    let other_source = format!("{source}-other");
    // CQL timestamps have millisecond precision.
    let cutoff = chrono::DateTime::from_timestamp_millis(Utc::now().timestamp_millis()).unwrap();
    let old = cutoff - Duration::days(2);
    let newer = cutoff + Duration::seconds(1);
    let stale_ids: Vec<_> = (0..1_003).map(|i| format!("{source}-{i:04}")).collect();
    stream::iter(&stale_ids)
        .map(|id| client.upsert_place(id, "stale", None, &source, old))
        .buffer_unordered(16)
        .try_collect::<Vec<_>>()
        .await?;

    // Several historical index entries can point to one place. Count a place
    // only once, even if its historical entries straddle cleanup pages.
    client
        .upsert_place(
            &stale_ids[0],
            "stale",
            None,
            &source,
            old + Duration::seconds(1),
        )
        .await?;
    let refreshed = format!("{source}-refreshed");
    client
        .upsert_place(&refreshed, "old", None, &source, old)
        .await?;
    client
        .upsert_place(&refreshed, "refreshed", None, &source, cutoff)
        .await?;
    let future = format!("{source}-newer");
    client
        .upsert_place(&future, "old", None, &source, old)
        .await?;
    client
        .upsert_place(&future, "newer", None, &source, newer)
        .await?;
    let reassigned = format!("{source}-reassigned");
    client
        .upsert_place(&reassigned, "old", None, &source, old)
        .await?;
    client
        .upsert_place(&reassigned, "other source", None, &other_source, old)
        .await?;
    let missing = format!("{source}-missing");
    client
        .upsert_place(&missing, "gone", None, &source, old)
        .await?;
    client
        .session
        .query_unpaged("DELETE FROM cypress.places WHERE id = ?", (&missing,))
        .await?;

    assert_eq!(
        client.delete_stale_places(&source, cutoff).await?,
        stale_ids.len() as u64
    );
    for id in &stale_ids {
        assert_eq!(
            client.get_place(id).await?,
            None,
            "stale place {id} survived"
        );
    }
    assert_eq!(
        client.get_place(&refreshed).await?.as_deref(),
        Some("refreshed")
    );
    assert_eq!(client.get_place(&future).await?.as_deref(), Some("newer"));
    assert_eq!(
        client.get_place(&reassigned).await?.as_deref(),
        Some("other source")
    );
    let remaining = client
        .session
        .query_unpaged(
            "SELECT id, import_timestamp FROM cypress.places_by_source WHERE source_file = ?",
            (&source,),
        )
        .await?
        .into_rows_result()?;
    let remaining: Vec<_> = remaining
        .rows::<(String, chrono::DateTime<Utc>)>()?
        .collect::<Result<_, _>>()?;
    assert_eq!(
        remaining.len(),
        2,
        "all obsolete index entries must be removed"
    );
    assert!(remaining.iter().all(|(_, timestamp)| *timestamp >= cutoff));
    assert_eq!(client.delete_stale_places(&source, cutoff).await?, 0);
    assert_eq!(
        client
            .delete_stale_places(&format!("{source}-empty"), cutoff)
            .await?,
        0
    );

    // Remove only this test's namespaced fixtures.
    for id in [&refreshed, &future, &reassigned] {
        client
            .session
            .query_unpaged("DELETE FROM cypress.places WHERE id = ?", (id,))
            .await?;
    }
    for name in [&source, &other_source] {
        client
            .session
            .query_unpaged(
                "DELETE FROM cypress.places_by_source WHERE source_file = ?",
                (name,),
            )
            .await?;
    }
    Ok(())
}
