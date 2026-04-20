use std::collections::HashMap;

use chrono::{Duration, NaiveDateTime};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Span {
        id: Uuid,
        name: String,
        timestamp: chrono::NaiveDateTime,
        data: HashMap<String, String>,
        duration: Duration,
        parent_id: Option<Uuid>,
        raw_lines: Option<(String, String)>,
    },
    Single {
        id: Uuid,
        name: String,
        timestamp: chrono::NaiveDateTime,
        data: HashMap<String, String>,
        parent_id: Option<Uuid>,
        raw_line: Option<String>,
    },
}

impl Event {
    pub fn id(&self) -> Uuid {
        match self {
            Event::Span { id, .. } => *id,
            Event::Single { id, .. } => *id,
        }
    }

    pub fn new_single(
        name: &str,
        timestamp: NaiveDateTime,
        data: HashMap<String, String>,
        raw_line: Option<String>,
    ) -> Event {
        Event::Single {
            id: Uuid::new_v4(),
            name: name.into(),
            timestamp,
            data,
            parent_id: None,
            raw_line,
        }
    }

    pub fn new_span(
        name: &str,
        timestamp: NaiveDateTime,
        data: HashMap<String, String>,
        duration: Duration,
        raw_lines: Option<(String, String)>,
    ) -> Event {
        Event::Span {
            id: Uuid::new_v4(),
            name: name.into(),
            timestamp,
            data,
            duration,
            parent_id: None,
            raw_lines,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Event::Span { name, .. } | Event::Single { name, .. } => name,
        }
    }

    pub fn parent_id(&self) -> Option<Uuid> {
        match self {
            Event::Span { parent_id, .. } | Event::Single { parent_id, .. } => *parent_id,
        }
    }

    pub fn data(&self) -> &HashMap<String, String> {
        match self {
            Event::Span { data, .. } | Event::Single { data, .. } => data,
        }
    }

    pub fn with_parent(mut self, parent_id: Uuid) -> Self {
        match &mut self {
            Event::Span { parent_id: p, .. } | Event::Single { parent_id: p, .. } => {
                *p = Some(parent_id);
            }
        }
        self
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
    const ID_A: Uuid = Uuid::from_u128(1);
    const ID_B: Uuid = Uuid::from_u128(2);

    fn ts() -> NaiveDateTime {
        NaiveDateTime::parse_from_str(TS_STR, TS_FMT).unwrap()
    }

    fn make_single(id: Uuid) -> Event {
        let mut e = Event::new_single("p", ts(), HashMap::new(), None);
        if let Event::Single { id: ref mut eid, .. } = e {
            *eid = id;
        }
        e
    }

    fn make_span(id: Uuid) -> Event {
        let mut e = Event::new_span("p", ts(), HashMap::new(), Duration::seconds(1), None);
        if let Event::Span { id: ref mut eid, .. } = e {
            *eid = id;
        }
        e
    }

    #[rstest]
    #[case(make_single(ID_A), ID_A)]
    #[case(make_span(ID_B), ID_B)]
    fn test_event_id(#[case] event: Event, #[case] expected_id: Uuid) {
        assert_eq!(event.id(), expected_id);
    }

    #[test]
    fn test_new_single_fields() {
        let timestamp = ts();
        let data = HashMap::from([("key".to_owned(), "value".to_owned())]);
        let event = Event::new_single("my_parser", timestamp, data.clone(), None);
        let Event::Single {
            id,
            name,
            timestamp: actual_ts,
            data: actual_data,
            ..
        } = event
        else {
            panic!("expected Single variant");
        };
        assert_eq!(name, "my_parser");
        assert_eq!(actual_ts, timestamp);
        assert_eq!(actual_data, data);
        assert_ne!(id, Uuid::nil());
    }

    #[test]
    fn test_new_single_unique_ids() {
        let ts = ts();
        let id1 = Event::new_single("x", ts, HashMap::new(), None).id();
        let id2 = Event::new_single("x", ts, HashMap::new(), None).id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_new_span_fields() {
        let timestamp = ts();
        let data = HashMap::from([("key".to_owned(), "value".to_owned())]);
        let duration = Duration::seconds(42);
        let event = Event::new_span("my_parser", timestamp, data.clone(), duration, None);
        let Event::Span {
            id,
            name,
            timestamp: actual_ts,
            data: actual_data,
            duration: actual_duration,
            ..
        } = event
        else {
            panic!("expected Span variant");
        };
        assert_eq!(name, "my_parser");
        assert_eq!(actual_ts, timestamp);
        assert_eq!(actual_data, data);
        assert_eq!(actual_duration, duration);
        assert_ne!(id, Uuid::nil());
    }

    #[test]
    fn test_new_span_unique_ids() {
        let ts = ts();
        let id1 = Event::new_span("x", ts, HashMap::new(), Duration::seconds(0), None).id();
        let id2 = Event::new_span("x", ts, HashMap::new(), Duration::seconds(0), None).id();
        assert_ne!(id1, id2);
    }
}
