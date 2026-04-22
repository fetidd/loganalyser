use std::collections::HashMap;

use async_trait::async_trait;
use shared::event::Event;
use thiserror::Error;

pub mod config;
pub mod event_filter;
pub(crate) mod memory;
pub(crate) mod mysql;
pub mod pending;
pub(crate) mod sql;
pub(crate) mod sqlite;

pub use config::{StorageConfig, StorageType, make_storage};
pub use event_filter::Filter;
pub use memory::MemoryEventStore;
pub use mysql::MySqlEventStore;
pub use pending::PendingSpanRecord;
pub use sqlite::SqliteEventStore;

#[derive(Debug, Error)]
pub enum Error {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("uuid error: {0}")]
    Uuid(#[from] uuid::Error),
    #[error("Storage error: {0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[async_trait]
pub trait EventStorage: Send + Sync + std::fmt::Debug {
    async fn store(&self, events: &[Event]) -> Result<()>;
    async fn load(&self, filter: Filter) -> Result<Vec<Event>>;

    async fn save_pending(&self, _file_path: &str, _parser_name: &str, _records: &[PendingSpanRecord]) -> Result<()> {
        Ok(())
    }

    async fn save_cursor(&self, _file_path: &str, _cursor: u64) -> Result<()> {
        Ok(())
    }

    async fn load_pending(&self) -> Result<Vec<PendingSpanRecord>> {
        Ok(vec![])
    }

    async fn load_file_cursors(&self) -> Result<HashMap<String, u64>> {
        Ok(HashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use shared::event::Event;

    use crate::{EventStorage, Filter, MemoryEventStore};

    #[tokio::test]
    async fn test_new_in_memory_storage() {
        let event = Event::new_single("single1", shared::datetime_from("2026-01-01").unwrap(), HashMap::new(), None);
        let store = MemoryEventStore::new_in_memory().await;
        store.store(&[event.clone()]).await.expect("failed to store");
        let read = store.load(Filter::new()).await.expect("failed to load");
        assert_eq!(event, read[0]);
    }
}
