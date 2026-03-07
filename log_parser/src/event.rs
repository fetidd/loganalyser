use std::collections::HashMap;

use chrono::{Duration, NaiveDateTime};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Span {
        id: String,
        name: String,
        timestamp: chrono::NaiveDateTime,
        data: HashMap<String, String>,
        duration: Duration,
    },
    Single {
        id: String,
        name: String,
        timestamp: chrono::NaiveDateTime,
        data: HashMap<String, String>,
    },
}

impl Event {
    pub fn id(&self) -> &str {
        match self {
            Event::Span { id, .. } => id.as_str(),
            Event::Single { id, .. } => id.as_str(),
        }
    }

    pub fn new_single(
        name: &str,
        timestamp: NaiveDateTime,
        data: HashMap<String, String>,
    ) -> Event {
        Event::Single {
            id: Uuid::new_v4().to_string(),
            name: name.into(),
            timestamp,
            data,
        }
    }

    pub fn new_span(
        name: &str,
        timestamp: NaiveDateTime,
        data: HashMap<String, String>,
        duration: Duration,
    ) -> Event {
        Event::Span {
            id: Uuid::new_v4().to_string(),
            name: name.into(),
            timestamp,
            data,
            duration,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_id(&mut self, new_id: &str) {
        match self {
            Event::Span { id, .. } => *id = new_id.to_owned(),
            Event::Single { id, .. } => *id = new_id.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    // use super::*;

    #[test]
    fn test() {}
}
