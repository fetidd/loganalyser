mod useful;
use useful::*;
use event_storage::{Filter, event_filter::Cmp};
use shared::event::Event;
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn basic_functionality() {
    let env = setup(BASIC_CONFIG).await;
    let timeout = Duration::from_secs(5);
    // -- single event --
    append(&env.log_file_path, "2026-01-01 12:00:00 LOG hello world");
    let ok = wait_until(
        || {
            let storage = Arc::clone(&env.storage);
            async move { storage.load(Filter::new().with_name(Cmp::Eq("single_event"))).await.unwrap().len() >= 1 }
        },
        timeout,
    )
    .await;
    assert!(ok, "timed out waiting for single event");
    let singles = env.storage.load(Filter::new().with_name(Cmp::Eq("single_event"))).await.unwrap();
    assert_eq!(singles.len(), 1);
    let Event::Single { data, parent_id, .. } = &singles[0] else {
        panic!("expected Single event, got {:?}", singles[0]);
    };
    assert_eq!(data.get("data").map(String::as_str), Some("hello world"));
    assert!(parent_id.is_none());
    // -- span with nested --
    // Write all three lines before the next poll tick so the span state is
    // maintained within a single parse batch.
    append(&env.log_file_path, "2026-01-01 12:00:01 ABC START");
    append(&env.log_file_path, "2026-01-01 12:00:02 ABC NOTE something happened");
    append(&env.log_file_path, "2026-01-01 12:00:03 ABC END");
    let ok = wait_until(
        || {
            let storage = Arc::clone(&env.storage);
            async move { storage.load(Filter::new().with_name(Cmp::Eq("span_event"))).await.unwrap().len() >= 1 }
        },
        timeout,
    )
    .await;
    assert!(ok, "timed out waiting for span event");
    let spans = env.storage.load(Filter::new().with_name(Cmp::Eq("span_event"))).await.unwrap();
    assert_eq!(spans.len(), 1);
    let Event::Span { id: span_id, data: span_data, .. } = &spans[0] else {
        panic!("expected Span event, got {:?}", spans[0]);
    };
    assert_eq!(span_data.get("ref").map(String::as_str), Some("ABC"));
    assert!(
        wait_until(
            || {
                let storage = Arc::clone(&env.storage);
                async move { storage.load(Filter::new().with_name(Cmp::Eq("span_inner"))).await.unwrap().len() >= 1 }
            },
            timeout,
        )
        .await,
        "timed out waiting for nested event"
    );
    let inner = env.storage.load(Filter::new().with_name(Cmp::Eq("span_inner"))).await.unwrap();
    assert_eq!(inner.len(), 1);
    let Event::Single { parent_id, data: inner_data, .. } = &inner[0] else {
        panic!("expected Single nested event, got {:?}", inner[0]);
    };
    assert_eq!(*parent_id, Some(*span_id), "nested event should link to its parent span");
    assert_eq!(inner_data.get("note").map(String::as_str), Some("something happened"));
}

#[tokio::test]
async fn span_across_poll_boundaries() {
    let env = setup(BASIC_CONFIG).await;
    let timeout = Duration::from_secs(5);
    // Write START and wait for it to be picked up by a poll cycle
    append(&env.log_file_path, "2026-01-01 12:00:00 ABC START");
    tokio::time::sleep(Duration::from_millis(1500)).await;
    // Write END in a separate poll cycle — this is the cross-poll case
    append(&env.log_file_path, "2026-01-01 12:00:05 ABC END");
    let ok = wait_until(
        || {
            let storage = Arc::clone(&env.storage);
            async move { storage.load(Filter::new().with_name(Cmp::Eq("span_event"))).await.unwrap().len() >= 1 }
        },
        timeout,
    )
    .await;
    assert!(ok, "timed out waiting for cross-poll span event");
    let spans = env.storage.load(Filter::new().with_name(Cmp::Eq("span_event"))).await.unwrap();
    assert_eq!(spans.len(), 1);
    let Event::Span { data, .. } = &spans[0] else {
        panic!("expected Span event, got {:?}", spans[0]);
    };
    assert_eq!(data.get("ref").map(String::as_str), Some("ABC"));
}

// ---------------------------------------------------------------------------
// Restart / cursor bug tests
// ---------------------------------------------------------------------------

/// Baseline: pending span state is restored after a restart, and if END arrives
/// *after* the restart the span completes correctly.
#[tokio::test]
async fn pending_span_completes_when_end_written_after_restart() {
    let mut env = setup(BASIC_CONFIG).await;
    let timeout = Duration::from_secs(5);
    // Write START and wait for it to be picked up and saved as a pending span.
    append(&env.log_file_path, "2026-01-01 12:00:00 ABC START");
    let ok = wait_until(
        || {
            let storage = Arc::clone(&env.storage);
            async move { !storage.load_pending().await.unwrap().is_empty() }
        },
        timeout,
    )
    .await;
    assert!(ok, "timed out waiting for pending span to be persisted");
    // Restart the watcher — pending span should be restored from DB.
    env.restart().await;
    // Write END after the restart; the new watcher's cursor is at the current
    // file end, so it will pick this up on the next poll.
    append(&env.log_file_path, "2026-01-01 12:00:05 ABC END");
    let ok = wait_until(
        || {
            let storage = Arc::clone(&env.storage);
            async move { storage.load(Filter::new().with_name(Cmp::Eq("span_event"))).await.unwrap().len() >= 1 }
        },
        timeout,
    )
    .await;
    assert!(ok, "timed out waiting for span to complete after restart");
}

/// Bug: when END is written to the log *while the watcher is down*, the
/// restarted watcher sets its cursor to the current end-of-file and never
/// reads the END line, leaving the span permanently incomplete.
///
/// This test documents the desired behaviour (span should complete).
#[tokio::test]
async fn pending_span_completes_when_end_written_during_downtime() {
    let mut env = setup(BASIC_CONFIG).await;
    let timeout = Duration::from_secs(5);
    // Write START and wait for the pending span to be persisted.
    append(&env.log_file_path, "2026-01-01 12:00:00 ABC START");
    let ok = wait_until(
        || {
            let storage = Arc::clone(&env.storage);
            async move { !storage.load_pending().await.unwrap().is_empty() }
        },
        timeout,
    )
    .await;
    assert!(ok, "timed out waiting for pending span to be persisted");
    // Kill the watcher to simulate a crash.
    env.kill();
    tokio::time::sleep(Duration::from_millis(200)).await; // let abort propagate
    // Write END while the watcher is down — this is the problematic case.
    append(&env.log_file_path, "2026-01-01 12:00:05 ABC END");
    // Restart: FileWatcher::new sets cursor = current file length, which is
    // *past* the END line. The END is therefore never read.
    env.restart().await;
    // Wait several poll cycles to give the watcher a fair chance.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let spans = env.storage.load(Filter::new().with_name(Cmp::Eq("span_event"))).await.unwrap();
    assert_eq!(spans.len(), 1, "span should have completed — END was written while watcher was down");
}
