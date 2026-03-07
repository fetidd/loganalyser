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
    use std::collections::HashMap;

    use chrono::{Duration, NaiveDateTime};
    use rstest::rstest;

    use super::*;

    const TS_STR: &str = "2026-01-01 00:00:00";
    const TS_FMT: &str = "%Y-%m-%d %H:%M:%S";

    fn ts() -> NaiveDateTime {
        NaiveDateTime::parse_from_str(TS_STR, TS_FMT).unwrap()
    }

    fn make_single(id: &str) -> Event {
        Event::Single {
            id: id.to_owned(),
            name: "p".into(),
            timestamp: ts(),
            data: HashMap::new(),
        }
    }

    fn make_span(id: &str) -> Event {
        Event::Span {
            id: id.to_owned(),
            name: "p".into(),
            timestamp: ts(),
            data: HashMap::new(),
            duration: Duration::seconds(1),
        }
    }

    #[rstest]
    #[case(make_single("abc"), "abc")]
    #[case(make_span("def"), "def")]
    fn test_event_id(#[case] event: Event, #[case] expected_id: &str) {
        assert_eq!(event.id(), expected_id);
    }

    #[rstest]
    #[case(make_single("old"))]
    #[case(make_span("old"))]
    fn test_set_id(#[case] mut event: Event) {
        event.set_id("new_id");
        assert_eq!(event.id(), "new_id");
    }

    #[test]
    fn test_new_single_fields() {
        let timestamp = ts();
        let data = HashMap::from([("key".to_owned(), "value".to_owned())]);
        let event = Event::new_single("my_parser", timestamp, data.clone());
        let Event::Single { id, name, timestamp: actual_ts, data: actual_data } = event else {
            panic!("expected Single variant");
        };
        assert_eq!(name, "my_parser");
        assert_eq!(actual_ts, timestamp);
        assert_eq!(actual_data, data);
        assert_eq!(id.len(), 36); // UUID v4: 32 hex chars + 4 hyphens
    }

    #[test]
    fn test_new_single_unique_ids() {
        let ts = ts();
        let id1 = Event::new_single("x", ts, HashMap::new()).id().to_owned();
        let id2 = Event::new_single("x", ts, HashMap::new()).id().to_owned();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_new_span_fields() {
        let timestamp = ts();
        let data = HashMap::from([("key".to_owned(), "value".to_owned())]);
        let duration = Duration::seconds(42);
        let event = Event::new_span("my_parser", timestamp, data.clone(), duration);
        let Event::Span { id, name, timestamp: actual_ts, data: actual_data, duration: actual_duration } = event else {
            panic!("expected Span variant");
        };
        assert_eq!(name, "my_parser");
        assert_eq!(actual_ts, timestamp);
        assert_eq!(actual_data, data);
        assert_eq!(actual_duration, duration);
        assert_eq!(id.len(), 36);
    }

    #[test]
    fn test_new_span_unique_ids() {
        let ts = ts();
        let id1 = Event::new_span("x", ts, HashMap::new(), Duration::seconds(0)).id().to_owned();
        let id2 = Event::new_span("x", ts, HashMap::new(), Duration::seconds(0)).id().to_owned();
        assert_ne!(id1, id2);
    }
}
