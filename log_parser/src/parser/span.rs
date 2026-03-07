use std::collections::HashMap;

use chrono::NaiveDateTime;
use regex::Regex;
use uuid::Uuid;

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

    fn extract_span_reference(&self, data: &HashMap<String, String>) -> SpanReference {
        SpanReference(
            self.reference_fields
                .iter()
                .map(|rf| data.get(rf).expect("reference field {rf} missing!").clone()) // when the span parser is built we make sure the reference_fields are in the data
                .collect(),
        )
    }

    fn process_line(
        &mut self,
        line: &str,
        parent_lookup: &dyn Fn(&HashMap<String, String>) -> Option<Uuid>,
    ) -> Vec<Event> {
        let mut events = vec![];
        if let Some(start_captures) = self.start_pattern.captures(line) {
            let Some(timestamp) =
                super::extract_timestamp(&start_captures["timestamp"], &self.timestamp_format)
            else {
                // TODO do we want to log here? Error?
                return events;
            };
            let mut capture_names = self.start_pattern.capture_names();
            let data = super::extract_data(&mut capture_names, &start_captures);
            let span_reference = self.extract_span_reference(&data);
            let parent_id = parent_lookup(&data);
            let pending_span = PendingSpan::new(timestamp, data, parent_id);
            self.pending.0.insert(span_reference, pending_span);
        } else if let Some(end_captures) = self.end_pattern.captures(line) {
            let mut capture_names = self.end_pattern.capture_names();
            let mut data = super::extract_data(&mut capture_names, &end_captures);
            let span_reference = self.extract_span_reference(&data);
            if let Some((_pending_reference, pending_span)) =
                self.pending.0.remove_entry(&span_reference)
            {
                let Some(end_timestamp) = super::extract_timestamp(
                    &end_captures["timestamp"],
                    &self.timestamp_format,
                ) else {
                    // TODO do we want to log here? Error?
                    return events;
                };
                data.extend(pending_span.data); // TODO chekcx if this ends up overwiritng and if we want to stop that - we want it to overwrite the timestamp because its the start timestamp we want
                let duration = end_timestamp - pending_span.timestamp;
                events.push(Event::Span {
                    id: pending_span.id,
                    name: self.name.clone(),
                    timestamp: pending_span.timestamp,
                    data,
                    duration,
                    parent_id: pending_span.parent_id,
                })
            } else {
                panic!("FOUND AN END LINE {span_reference:?} WITHOUT A PENDING SPAN FOR IT!")
            }
        } else if !self.nested.is_empty() {
            let pending = &self.pending;
            let reference_fields = &self.reference_fields;
            let lookup = |data: &HashMap<String, String>| {
                pending.0.iter().find_map(|(span_ref, pending_span)| {
                    let matches = reference_fields
                        .iter()
                        .zip(span_ref.0.iter())
                        .all(|(field, value)| data.get(field).map_or(false, |v| v == value));
                    matches.then_some(pending_span.id)
                })
            };
            for parser in self.nested.iter_mut() {
                events.extend(parser.parse_line_with_context(line, &lookup));
            }
        }
        events
    }

    pub(super) fn parse(&mut self, input: &str) -> Vec<Event> {
        input
            .lines()
            .flat_map(|line| self.process_line(line, &|_| None))
            .collect()
    }

    pub(super) fn parse_line_with_context(
        &mut self,
        line: &str,
        parent_lookup: &dyn Fn(&HashMap<String, String>) -> Option<Uuid>,
    ) -> Vec<Event> {
        self.process_line(line, parent_lookup)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SpanReference(Vec<String>);

#[derive(Debug, Clone)]
struct PendingSpan {
    id: Uuid,
    timestamp: NaiveDateTime,
    data: HashMap<String, String>,
    parent_id: Option<Uuid>,
}

impl PendingSpan {
    fn new(timestamp: NaiveDateTime, data: HashMap<String, String>, parent_id: Option<Uuid>) -> Self {
        Self {
            timestamp,
            data,
            id: Uuid::new_v4(),
            parent_id,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct PendingSpans(HashMap<SpanReference, PendingSpan>);

#[cfg(test)]
mod tests {
    use chrono::{Duration, NaiveDateTime};
    use regex::Regex;
    use rstest::rstest;

    use crate::event::Event;
    use crate::parser::InternalSingleParser;
    use crate::parser::tests::TEST_ID;

    use super::super::tests::common_test_data;
    use super::*;

    const TS_FMT: &str = "%Y-%m-%d %H:%M:%S";
    const START: &str =
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<ref>[a-z0-9]{5})\s+START";
    const END: &str =
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<ref>[a-z0-9]{5})\s+END";

    fn test_span(data: &[(&str, &str)], timestamp: &str, duration: i64) -> Event {
        let (ts, data_map) = common_test_data(data, timestamp);
        let mut e = Event::new_span(
            "test",
            NaiveDateTime::parse_from_str(&ts, TS_FMT).unwrap(),
            data_map,
            Duration::new(duration, 0).unwrap(),
        );
        e.set_id(TEST_ID);
        e
    }

    #[rstest]
    #[case(
        START,
        END,
        "2026-01-01 00:00:00 abc01 START\n2026-01-01 00:00:05 abc01 END",
        vec![test_span(&[("ref", "abc01")], "2026-01-01 00:00:00", 5)],
    )]
    #[case(
        START,
        END,
        // two sequential non-overlapping spans
        r#"2026-01-01 00:00:00 abc01 START
2026-01-01 00:00:05 abc01 END
2026-01-01 00:00:10 abc02 START
2026-01-01 00:00:15 abc02 END"#,
        vec![
            test_span(&[("ref", "abc01")], "2026-01-01 00:00:00", 5),
            test_span(&[("ref", "abc02")], "2026-01-01 00:00:10", 5),
        ],
    )]
    #[case(
        START,
        END,
        // two overlapping concurrent spans with different ref values
        r#"2026-01-01 00:00:00 abc01 START
2026-01-01 00:00:02 abc02 START
2026-01-01 00:00:05 abc01 END
2026-01-01 00:00:08 abc02 END"#,
        vec![
            test_span(&[("ref", "abc01")], "2026-01-01 00:00:00", 5),
            test_span(&[("ref", "abc02")], "2026-01-01 00:00:02", 6),
        ],
    )]
    #[case(START, END, "", vec![])] // empty input
    #[case(
        START,
        END,
        "2026-01-01 00:00:00 abc01 START", // start only, no matching end
        vec![],
    )]
    #[case(
        START,
        END,
        // zero-duration span: start and end at same timestamp
        r#"2026-01-01 00:00:00 abc01 START
2026-01-01 00:00:00 abc01 END"#,
        vec![test_span(&[("ref", "abc01")], "2026-01-01 00:00:00", 0)],
    )]
    fn test_span_parse(
        #[case] start_pattern: &str,
        #[case] end_pattern: &str,
        #[case] log: &str,
        #[case] expected: Vec<Event>,
    ) {
        let mut parser = InternalSpanParser::new(
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

    #[test]
    fn test_nested_events_have_parent_id() {
        let log = r#"2026-01-01 00:00:00 abc01 START
2026-01-01 00:00:03 abc01 nested
2026-01-01 00:00:03 abc02 nested
2026-01-01 00:00:05 abc01 END"#;
        let mut parser = InternalSpanParser::new(
            "test".into(),
            TS_FMT.into(),
            Regex::new(START).unwrap(),
            Regex::new(END).unwrap(),
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
        let events = parser.parse(log);
        assert_eq!(events.len(), 3);
        // outer span is emitted last (when END is seen)
        let outer_id = events[2].id();
        // abc01 nested: parent_id links to the outer span
        assert!(matches!(&events[0], Event::Single { name, parent_id, .. } if name == "test_inner" && *parent_id == Some(outer_id)));
        // abc02 nested: no pending span with ref=abc02, so no parent
        assert!(matches!(&events[1], Event::Single { name, parent_id, .. } if name == "test_inner" && parent_id.is_none()));
        // outer span: top-level, no parent
        assert!(matches!(&events[2], Event::Span { name, parent_id, .. } if name == "test" && parent_id.is_none()));
    }
}
