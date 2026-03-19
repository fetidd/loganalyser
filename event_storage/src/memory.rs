use std::sync::{Arc, RwLock};

use shared::event::Event;

use async_trait::async_trait;

use crate::{Error, Filter, EventStorage, Result};

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

#[async_trait]
impl EventStorage for MemoryEventStore {
    async fn store(&self, events: &[Event]) -> Result<()> {
        match self.events.write() {
            Ok(mut stored) => {
                stored.extend(events.iter().cloned());
                Ok(())
            }
            Err(e) => Err(Error::Storage(e.to_string())),
        }
    }

    async fn load(&self, filter: Filter) -> Result<Vec<Event>> {
        match self.events.read() {
            Ok(stored) => Ok(stored
                .iter()
                .filter(|ev| apply_filter(ev, &filter))
                .cloned()
                .collect()),
            Err(e) => Err(Error::Storage(e.to_string())),
        }
    }
}

fn apply_filter(_event: &Event, _filter: &Filter) -> bool {
    true
}
