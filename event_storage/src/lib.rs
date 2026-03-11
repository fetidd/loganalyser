use shared::event::Event;
use thiserror::Error;

pub mod event_filter;
pub mod memory;
pub mod mysql;

pub use memory::MemoryEventStore;
pub use mysql::MySqlEventStore;

use crate::event_filter::EventFilter;

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

pub trait EventStorage {
    fn store(&self, events: &[Event]) -> impl Future<Output = Result<()>> + Send;
    fn load(&self, filter: EventFilter) -> impl Future<Output = Result<Vec<Event>>> + Send;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use shared::event::Event;

    use crate::{EventFilter, EventStorage, MemoryEventStore};

    #[tokio::test]
    async fn test_new_in_memory_storage() {
        let event = Event::new_single(
            "single1",
            shared::datetime_from("2026-01-01").unwrap(),
            HashMap::new(),
        );
        let store = MemoryEventStore::new();
        store
            .store(&[event.clone()])
            .await
            .expect("failed to store");
        let read = store
            .load(EventFilter::new())
            .await
            .expect("failed to load");
        assert_eq!(event, read[0]);
    }
}
