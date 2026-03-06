use std::{cell::RefCell, collections::HashMap};

use chrono::NaiveDateTime;
use regex::Regex;

use crate::event::Event;

use super::Parser;

#[derive(Debug, Clone)]
pub struct InternalSpanParser {
    pub name: String,
    pub timestamp_format: String,
    pub start_pattern: Regex,
    pub end_pattern: Regex,
    pub nested: Vec<Parser>,
    pub reference_fields: Vec<String>,
    pending: PendingSpans,
}

impl InternalSpanParser {
    pub(super) fn new(
        name: String,
        timestamp_format: String,
        start_pattern: Regex,
        end_pattern: Regex,
        nested: Vec<Parser>,
        reference_fields: Vec<String>,
    ) -> Self {
        Self {
            name,
            timestamp_format,
            start_pattern,
            end_pattern,
            nested,
            reference_fields,
            pending: PendingSpans::default(),
        }
    }

    pub(super) fn parse(&self, _input: &str) -> Vec<Event> {
        todo!()
    }
}

#[derive(Debug, Clone)]
struct SpanReference(Vec<String>);

#[derive(Debug, Clone)]
struct PendingSpan {
    timestamp: NaiveDateTime,
    data: HashMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct PendingSpans(RefCell<HashMap<SpanReference, PendingSpan>>);

#[cfg(test)]
mod tests {
    use chrono::{Duration, NaiveDateTime};
    use regex::Regex;

    use crate::event::Event;

    use super::super::tests::common_test_data;
    use super::*;

    const TS_FMT: &str = "%Y-%m-%d %H:%M:%S";

    fn test_span(data: &[(&str, &str)], timestamp: &str, duration: i64) -> Event {
        let (ts, data_map) = common_test_data(data, timestamp);
        Event::Span {
            source: "test".into(),
            timestamp: NaiveDateTime::parse_from_str(&ts, TS_FMT).unwrap(),
            data: data_map,
            duration: Duration::new(duration, 0).unwrap(),
        }
    }

    #[test]
    fn test_span_parse() {
        for (start_pattern, end_pattern, log, expected) in [(
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s(?P<ref>[a-z0-9]{5})\s+START",
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s(?P<ref>[a-z0-9]{5})\s+END",
            "2026-01-01 00:00:00 abc01 START\n2026-01-01 00:00:05 abc01 END",
            vec![test_span(&[], "2026-01-01 00:00:00", 5)],
        )] {
            let parser = InternalSpanParser::new(
                "test".into(),
                TS_FMT.into(),
                Regex::new(start_pattern).unwrap(),
                Regex::new(end_pattern).unwrap(),
                vec![],
                vec!["ref".into()],
            );
            let actual = parser.parse(log);
            assert_eq!(actual, expected);
        }
    }
}
