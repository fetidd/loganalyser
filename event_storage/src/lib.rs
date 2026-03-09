use std::sync::{Arc, RwLock};

use shared::event::Event;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Storage error: {0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait EventStorage {
    fn store(&self, events: &[Event]) -> impl Future<Output = Result<()>> + Send;
    fn load(&self, filter: EventFilter) -> impl Future<Output = Result<Vec<Event>>> + Send;
}

#[derive(Debug)]
pub struct MemoryEventStore {
    events: Arc<RwLock<Vec<Event>>>,
}

impl MemoryEventStore {
    pub fn new() -> Self {
        Self {
            events: Arc::new(RwLock::new(vec![])),
        }
    }
}

impl Default for MemoryEventStore {
    fn default() -> Self {
        Self::new()
    }
}

impl EventStorage for MemoryEventStore {
    fn store(&self, events: &[Event]) -> impl Future<Output = Result<()>> + Send {
        let res = match self.events.write() {
            Ok(mut stored) => {
                let new_events: Vec<Event> = events.into_iter().map(|e| (*e).to_owned()).collect();
                stored.extend(new_events);
                Ok(())
            }
            Err(error) => Err(Error::Storage(error.to_string())),
        };
        std::future::ready(res)
    }

    fn load(&self, filter: EventFilter) -> impl Future<Output = Result<Vec<Event>>> + Send {
        let res = match self.events.read() {
            Ok(stored) => Ok(stored
                .iter()
                .filter(|ev| filter.apply(*ev))
                .cloned()
                .collect()),
            Err(e) => Err(Error::Storage(e.to_string())),
        };
        std::future::ready(res)
    }
}

pub struct EventFilter {}

impl EventFilter {
    fn apply(&self, _event: &Event) -> bool {
        true
    }

    pub fn new() -> Self {
        Self {}
    }
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
