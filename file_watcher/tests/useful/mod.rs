#![allow(dead_code)]

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use event_storage::{Filter, StorageConfig, StorageType, make_storage};
use file_watcher::FileWatcher;
use shared::{datetime_from, event::Event};
use tokio::task::JoinHandle;

pub struct TestEnv {
    _temp_dir: tempfile::TempDir, // kept alive so the directory isn't deleted mid-test
    pub log_file_path: PathBuf,
    config_file_path: PathBuf,
    pub storage: Arc<event_storage::EventStorage>,
    jh: JoinHandle<()>,
    seconds: i64,
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        self.jh.abort();
    }
}

impl TestEnv {
    /// Kills the running watcher without restarting it.
    pub fn kill(&self) {
        self.jh.abort();
    }

    /// Polls storage until at least `min` events match `filter`, then returns
    /// them. Returns `Err` with a description if `timeout` elapses first.
    pub async fn wait_for(&self, filter: Filter, min: usize, timeout: Duration) -> Result<Vec<Event>, String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let events = self.storage.load(filter.clone()).await.unwrap();
            if events.len() >= min {
                return Ok(events);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!("timed out after {timeout:?}: {} event(s) matched, wanted {min}", events.len()));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Kills the running watcher and starts a fresh one from the same config.
    /// Sleeps briefly after the abort to let cancellation propagate before
    /// the new watcher is initialised (and sets its cursor).
    pub async fn restart(&mut self) {
        self.jh.abort();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut watcher = FileWatcher::new(std::fs::read(&self.config_file_path).unwrap()).await.unwrap();
        self.jh = tokio::spawn(async move {
            watcher.run().await.expect("watcher run failed");
        });
    }

    pub fn append_log(&mut self, lines: &[&str]) {
        for line in lines {
            let line = if line.starts_with("{{ts}}") {
                let dt = datetime_from("2026-01-01 12:00:00").unwrap().checked_add_signed(chrono::TimeDelta::seconds(self.seconds)).unwrap();
                self.seconds += 1;
                format!("{dt}{}", &line[6..])
            } else {
                line.to_string()
            };
            append(&self.log_file_path, &line);
        }
    }
}

/// Create a temp dir, write `config` (with DB_PATH / LOG_PATH substituted),
/// start a FileWatcher, and open a read-side storage connection.
pub async fn setup(config: &str) -> TestEnv {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let config_file_path = temp_dir.path().join("test_config.toml");
    let log_file_path = temp_dir.path().join("test.log");
    let db_file_path = temp_dir.path().join("test.db");
    let config = config.replace("DB_PATH", db_file_path.to_str().unwrap()).replace("LOG_PATH", log_file_path.to_str().unwrap());
    std::fs::write(&config_file_path, &config).unwrap();
    std::fs::write(&log_file_path, "").unwrap();
    let mut watcher = FileWatcher::new(std::fs::read(&config_file_path).unwrap()).await.unwrap();
    let jh = tokio::spawn(async move {
        watcher.run().await.expect("watcher run failed");
    });
    let storage = make_storage(&StorageConfig {
        storage_type: StorageType::Sqlite,
        connection_string: Some(db_file_path.to_str().unwrap().to_string()),
        state_db_path: None,
    })
    .await
    .unwrap();
    // Verify both tables are accessible before handing the env to the test.
    storage.load(Filter::new()).await.expect("events table not ready");
    storage.load_pending().await.expect("pending_spans table not ready");
    TestEnv {
        _temp_dir: temp_dir,
        log_file_path,
        config_file_path,
        storage: Arc::new(storage),
        jh,
        seconds: 0,
    }
}

/// Poll `check` every 100ms until it returns true or `timeout` elapses.
pub async fn wait_until<F, Fut>(mut check: F, timeout: Duration) -> bool
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

pub fn append(path: &std::path::Path, line: &str) {
    let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    writeln!(f, "{line}").unwrap();
}

// ---------------------------------------------------------------------------
// Configs
// ---------------------------------------------------------------------------

pub const BASIC_CONFIG: &str = r#"
[settings]
poll_interval_secs = 1

[storage]
storage_type = "sqlite"
connection_string = "DB_PATH"

[defaults]
timestamp_format = "%Y-%m-%d %H:%M:%S"

[components]
timestamp = '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
ref       = '[A-Z]{3}'

[[parsers]]
name    = "single_event"
glob    = "LOG_PATH"
type    = "single"
pattern = '(?P<timestamp>${timestamp}) LOG (?P<data>.*)'

[[parsers]]
name             = "span_event"
glob             = "LOG_PATH"
type             = "span"
start_pattern    = '(?P<timestamp>${timestamp}) (?P<ref>${ref})   START'
end_pattern      = '(?P<timestamp>${timestamp}) (?P<ref>${ref})   END'
reference_fields = ["ref"]

[[parsers.nested]]
name    = "span_inner"
type    = "single"
pattern = '(?P<timestamp>${timestamp}) (?P<ref>${ref}) NOTE (?P<note>.*)'
"#;
