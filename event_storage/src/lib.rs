use shared::event::Event;
use sqlx::{MySqlPool, SqlitePool};
use thiserror::Error;

pub mod config;
pub mod event_filter;
pub(crate) mod memory;
pub(crate) mod mysql;
pub(crate) mod sql;
pub(crate) mod sqlite;

pub use config::{StorageConfig, StorageType, make_storage};
pub use event_filter::Filter;
pub use memory::MemoryEventStore;
pub use mysql::MySqlEventStore;
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

#[derive(Debug)]
pub enum EventStorage {
    Sqlite(SqliteEventStore),
    MySql(MySqlEventStore),
    InMemory(SqliteEventStore),
}

impl EventStorage {
    pub fn new_mysql(pool: MySqlPool) -> Self {
        Self::MySql(MySqlEventStore::new(pool))
    }

    pub async fn new_sqlite(pool: SqlitePool) -> Self {
        Self::Sqlite(SqliteEventStore::from_pool(pool).await)
    }

    pub async fn new_in_memory() -> Self {
        Self::InMemory(SqliteEventStore::new_in_memory().await)
    }

    pub async fn store(&self, events: &[Event]) -> Result<()> {
        match self {
            EventStorage::Sqlite(s) | EventStorage::InMemory(s) => s.store(events).await,
            EventStorage::MySql(s) => s.store(events).await,
        }
    }

    pub async fn load(&self, filter: &Filter) -> Result<Vec<Event>> {
        match self {
            EventStorage::Sqlite(s) | EventStorage::InMemory(s) => s.load(filter).await,
            EventStorage::MySql(s) => s.load(filter).await,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use shared::event::Event;

    use crate::{EventStorage, Filter};

    #[tokio::test]
    async fn test_new_in_memory_storage() {
        let event = Event::new_single("single1", shared::datetime_from("2026-01-01").unwrap(), HashMap::new(), String::new());
        let store = EventStorage::new_in_memory().await;
        store.store(&[event.clone()]).await.expect("failed to store");
        let read = store.load(&Filter::new()).await.expect("failed to load");
        assert_eq!(event, read[0]);
    }
}
