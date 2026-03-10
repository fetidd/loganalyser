use std::sync::{Arc, RwLock};

use shared::event::Event;

use crate::{Error, EventFilter, EventStorage, Result};

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
