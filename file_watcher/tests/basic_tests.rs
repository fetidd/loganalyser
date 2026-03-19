use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use event_storage::event_filter::Cmp;
use event_storage::{Filter, StorageConfig, StorageType, make_storage};
use file_watcher::FileWatcher;
use shared::event::Event;
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

struct TestEnv {
    _temp_dir: tempfile::TempDir, // kept alive so the directory isn't deleted mid-test
    pub log_file_path: PathBuf,
    pub storage: Arc<dyn event_storage::EventStorage>,
    jh: JoinHandle<()>,
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        self.jh.abort();
    }
}

/// Create a temp dir, write `config` (with DB_PATH / LOG_PATH substituted),
/// start a FileWatcher, and open a read-side storage connection.
async fn setup(config: &str) -> TestEnv {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let config_file_path = temp_dir.path().join("test_config.toml");
    let log_file_path = temp_dir.path().join("test.log");
    let db_file_path = temp_dir.path().join("test.db");
    let config = config
        .replace("DB_PATH", db_file_path.to_str().unwrap())
        .replace("LOG_PATH", log_file_path.to_str().unwrap());
    std::fs::write(&config_file_path, &config).unwrap();
    std::fs::write(&log_file_path, "").unwrap();
    let mut watcher = FileWatcher::new(std::fs::read(&config_file_path).unwrap())
        .await
        .unwrap();
    let jh = tokio::spawn(async move {
        watcher.run().await.expect("watcher run failed");
    });
    let storage = make_storage(&StorageConfig {
        storage_type: StorageType::Sqlite,
        connection_string: Some(db_file_path.to_str().unwrap().to_string()),
    })
    .await
    .unwrap();
    // The table is created asynchronously inside SqliteEventStore::new — wait
    // until a query succeeds before handing the env to the test.
    let ready = wait_until(
        || {
            let storage = Arc::clone(&storage);
            async move { storage.load(Filter::new()).await.is_ok() }
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(ready, "timed out waiting for events table to be created");
    TestEnv {
        _temp_dir: temp_dir,
        log_file_path,
        storage,
        jh,
    }
}

/// Poll `check` every 100ms until it returns true or `timeout` elapses.
async fn wait_until<F, Fut>(mut check: F, timeout: Duration) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if check().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn append(path: &std::path::Path, line: &str) {
    let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    writeln!(f, "{line}").unwrap();
}

// ---------------------------------------------------------------------------
// Configs
// ---------------------------------------------------------------------------

const BASIC_CONFIG: &str = r#"
[settings]
poll_interval_secs = 1

[storage]
storage_type = "sqlite"
connection_string = "DB_PATH"

[defaults]
timestamp_format = "%Y-%m-%d %H:%M:%S"

[components]
timestamp = '(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
ref = '(?P<ref>[A-Z]{3})'
__ = '\s+'

[[parsers]]
name = "single_event"
glob = "LOG_PATH"
type = "single"
pattern = '{{timestamp}}{{__}}LOG{{__}}(?P<data>.*)'

[[parsers]]
name = "span_event"
glob = "LOG_PATH"
type = "span"
start_pattern = '{{timestamp}}{{__}}{{ref}}{{__}}START'
end_pattern = '{{timestamp}}{{__}}{{ref}}{{__}}END'
reference_fields = ["ref"]
nested = [
    { name = "span_inner", type = "single", pattern = '{{timestamp}}{{__}}{{ref}}{{__}}NOTE{{__}}(?P<note>.*)' },
]
"#;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
