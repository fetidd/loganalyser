use std::{cell::RefCell, collections::HashMap};

use chrono::{Duration, NaiveDateTime};
use regex::{CaptureNames, Captures, Regex};

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

    fn extract_timestamp(&self, ts: &str) -> Option<chrono::NaiveDateTime> {
        match chrono::NaiveDateTime::parse_from_str(ts, &self.timestamp_format) {
            Ok(timestamp) => Some(timestamp),
            Err(_) => None,
        }
    }

    fn extract_data(
        &self,
        capture_names: &mut CaptureNames,
        captures: &Captures,
    ) -> HashMap<String, String> {
        let mut data = HashMap::new();
        for field in capture_names {
            if let Some(field) = field
                && let Some(value) = captures.name(field)
            {
                data.insert(field.to_owned(), value.as_str().to_owned());
            }
        }
        data
    }

    fn extract_span_reference(&self, data: &HashMap<String, String>) -> SpanReference {
        SpanReference(
            self.reference_fields
                .iter()
                .map(|rf| data.get(rf).unwrap().clone()) // when the span parser is built we make sure the reference_fields are in the data
                .collect(),
        )
    }

    pub(super) fn parse(&self, input: &str) -> Vec<Event> {
        let mut events = vec![];
        for line in input.lines() {
            if let Some(start_captures) = self.start_pattern.captures(line) {
                let Some(timestamp) = self.extract_timestamp(&start_captures["timestamp"]) else {
                    // TODO do we want to log here? Error?
                    continue;
                };
                let mut capture_names = self.start_pattern.capture_names();
                let data = self.extract_data(&mut capture_names, &start_captures);
                let span_reference = self.extract_span_reference(&data);
                let pending_span = PendingSpan::new(timestamp, data);
                self.pending
                    .0
                    .borrow_mut()
                    .insert(span_reference, pending_span);
            } else if let Some(end_captures) = self.end_pattern.captures(line) {
                let mut capture_names = self.end_pattern.capture_names();
                let mut data = self.extract_data(&mut capture_names, &end_captures);
                let span_reference = self.extract_span_reference(&data);
                if let Some((_pending_reference, pending_span)) =
                    self.pending.0.borrow_mut().remove_entry(&span_reference)
                {
                    let Some(end_timestamp) = self.extract_timestamp(&end_captures["timestamp"])
                    else {
                        // TODO do we want to log here? Error?
                        continue;
                    };
                    data.extend(pending_span.data); // TODO chekcx if this ends up overwiritng and if we want to stop that - we want it to overwrite the timestamp because its the start timestamp we want
                    let duration = end_timestamp - pending_span.timestamp;
                    events.push(Event::new_span(
                        &self.name,
                        pending_span.timestamp,
                        data,
                        duration,
                    ))
                } else {
                    panic!("FOUND AN END LINE WITHOUT A PENDING SPAN FOR IT!")
                }
            } else if !self.nested.is_empty() {
                todo!()
            }
        }
        events
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SpanReference(Vec<String>);

#[derive(Debug, Clone)]
struct PendingSpan {
    timestamp: NaiveDateTime,
    data: HashMap<String, String>,
}

impl PendingSpan {
    fn new(timestamp: NaiveDateTime, data: HashMap<String, String>) -> Self {
        Self { timestamp, data }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct PendingSpans(RefCell<HashMap<SpanReference, PendingSpan>>);

#[cfg(test)]
mod tests {
    use std::cell::{LazyCell, OnceCell};

    use chrono::{Duration, NaiveDateTime};
    use regex::Regex;
    use uuid::Uuid;

    use crate::event::Event;
    use crate::parser::InternalSingleParser;
    use crate::parser::tests::TEST_ID;

    use super::super::tests::common_test_data;
    use super::*;

    const TS_FMT: &str = "%Y-%m-%d %H:%M:%S";

    fn test_span(data: &[(&str, &str)], timestamp: &str, duration: i64) -> Event {
        let (ts, data_map) = common_test_data(data, timestamp);
        Event::Span {
            name: "test".into(),
            timestamp: NaiveDateTime::parse_from_str(&ts, TS_FMT).unwrap(),
            data: data_map,
            duration: Duration::new(duration, 0).unwrap(),
            id: TEST_ID.to_string(),
        }
    }

    #[test]
    fn test_span_parse() {
        let start =
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<ref>[a-z0-9]{5})\s+START";
        let end =
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<ref>[a-z0-9]{5})\s+END";
        for (start_pattern, end_pattern, log, expected) in [
            (
                start,
                end,
                "2026-01-01 00:00:00 abc01 START\n2026-01-01 00:00:05 abc01 END",
                vec![test_span(&[("ref", "abc01")], "2026-01-01 00:00:00", 5)],
            ),
            // (
            //     start,
            //     end,
            //     "2026-01-01 00:00:00 abc01 START\n2026-01-01 00:00:03 abc01 nested\n2026-01-01 00:00:03 abc02 nested\n2026-01-01 00:00:05 abc01 END",
            //     vec![test_span(&[("ref", "abc01")], "2026-01-01 00:00:00", 5)], // checks that nested events work, but only if they have the same reference value
            // ),
        ] {
            let parser = InternalSpanParser::new(
                "test".into(),
                TS_FMT.into(),
                Regex::new(start_pattern).unwrap(),
                Regex::new(end_pattern).unwrap(),
                vec![Parser::Single(InternalSingleParser {
                    name: "test_inner".into(),
                    pattern: Regex::new(
                        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<ref>[a-z0-9]{5})\s+nested",
                    )
                    .unwrap(),
                    timestamp_format: TS_FMT.to_string(),
                })],
                vec!["ref".into()],
            );
            let mut actual = parser.parse(log);
            actual.iter_mut().for_each(|f| f.set_id(TEST_ID));
            assert_eq!(actual, expected);
        }
    }
}
