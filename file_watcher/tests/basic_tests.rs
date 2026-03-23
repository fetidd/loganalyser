mod useful; 

use std::{sync::Arc, time::Duration};

use event_storage::{Filter, event_filter::Cmp};
use shared::event::Event;
use crate::useful::*;


#[tokio::test]
async fn basic_functionality() {
    let env = setup(BASIC_CONFIG).await;

    let timeout = Duration::from_secs(5);

    // -- single event --
    append(&env.log_file_path, "2026-01-01 12:00:00 LOG hello world");

    let ok = wait_until(
        || {
            let storage = Arc::clone(&env.storage);
            async move {
                storage
                    .load(Filter::new().with_name(Cmp::Eq("single_event")))
                    .await
                    .unwrap()
                    .len()
                    >= 1
            }
        },
        timeout,
    )
    .await;
    assert!(ok, "timed out waiting for single event");

    let singles = env
        .storage
        .load(Filter::new().with_name(Cmp::Eq("single_event")))
        .await
        .unwrap();
    assert_eq!(singles.len(), 1);
    let Event::Single {
        data, parent_id, ..
    } = &singles[0]
    else {
        panic!("expected Single event, got {:?}", singles[0]);
    };
    assert_eq!(data.get("data").map(String::as_str), Some("hello world"));
    assert!(parent_id.is_none());

    // -- span with nested --
    // Write all three lines before the next poll tick so the span state is
    // maintained within a single parse batch.
    append(&env.log_file_path, "2026-01-01 12:00:01 ABC START");
    append(
        &env.log_file_path,
        "2026-01-01 12:00:02 ABC NOTE something happened",
    );
    append(&env.log_file_path, "2026-01-01 12:00:03 ABC END");

    let ok = wait_until(
        || {
            let storage = Arc::clone(&env.storage);
            async move {
                storage
                    .load(Filter::new().with_name(Cmp::Eq("span_event")))
                    .await
                    .unwrap()
                    .len()
                    >= 1
            }
        },
        timeout,
    )
    .await;
    assert!(ok, "timed out waiting for span event");

    let spans = env
        .storage
        .load(Filter::new().with_name(Cmp::Eq("span_event")))
        .await
        .unwrap();
    assert_eq!(spans.len(), 1);
    let Event::Span {
        id: span_id,
        data: span_data,
        ..
    } = &spans[0]
    else {
        panic!("expected Span event, got {:?}", spans[0]);
    };
    assert_eq!(span_data.get("ref").map(String::as_str), Some("ABC"));

    let ok = wait_until(
        || {
            let storage = Arc::clone(&env.storage);
            async move {
                storage
                    .load(Filter::new().with_name(Cmp::Eq("span_inner")))
                    .await
                    .unwrap()
                    .len()
                    >= 1
            }
        },
        timeout,
    )
    .await;
    assert!(ok, "timed out waiting for nested event");

    let inner = env
        .storage
        .load(Filter::new().with_name(Cmp::Eq("span_inner")))
        .await
        .unwrap();
    assert_eq!(inner.len(), 1);
    let Event::Single {
        parent_id,
        data: inner_data,
        ..
    } = &inner[0]
    else {
        panic!("expected Single nested event, got {:?}", inner[0]);
    };
    assert_eq!(
        *parent_id,
        Some(*span_id),
        "nested event should link to its parent span"
    );
    assert_eq!(
        inner_data.get("note").map(String::as_str),
        Some("something happened")
    );
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
            async move {
                storage
                    .load(Filter::new().with_name(Cmp::Eq("span_event")))
                    .await
                    .unwrap()
                    .len()
                    >= 1
            }
        },
        timeout,
    )
    .await;
    assert!(ok, "timed out waiting for cross-poll span event");

    let spans = env
        .storage
        .load(Filter::new().with_name(Cmp::Eq("span_event")))
        .await
        .unwrap();
    assert_eq!(spans.len(), 1);
    let Event::Span { data, .. } = &spans[0] else {
        panic!("expected Span event, got {:?}", spans[0]);
    };
    assert_eq!(data.get("ref").map(String::as_str), Some("ABC"));
}
