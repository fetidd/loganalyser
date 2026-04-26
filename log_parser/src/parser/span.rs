use std::collections::HashMap;

use chrono::NaiveDateTime;
use regex::Regex;
use tracing::warn;
use uuid::Uuid;

use crate::pending_span::{PendingSpan, SpanReference, id_from_line};
use shared::event::Event;

use super::Parser;

#[derive(Debug, Clone)]
pub struct InternalSpanParser {
    pub name: String,
    pub timestamp_format: String,
    pub start_pattern: Regex,
    pub end_pattern: Regex,
    pub nested: Vec<Parser>,
    pub reference_fields: Vec<String>,
    pub(super) pending: PendingSpans,
    pub file_seed: Option<String>,
}

impl InternalSpanParser {
    pub(super) fn new(name: String, timestamp_format: String, start_pattern: Regex, end_pattern: Regex, nested: Vec<Parser>, reference_fields: Vec<String>) -> Self {
        Self {
            name,
            timestamp_format,
            start_pattern,
            end_pattern,
            nested,
            reference_fields,
            pending: PendingSpans::default(),
            file_seed: None,
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

    pub(super) fn parse_line_with_context(&mut self, line: &str) -> Option<Event> {
        if let Some(start_captures) = self.start_pattern.captures(line) {
            let Some(timestamp) = super::extract_timestamp(&start_captures["timestamp"], &self.timestamp_format) else {
                warn!(line, format = self.timestamp_format, "failed to parse start timestamp");
                return None;
            };
            let mut capture_names = self.start_pattern.capture_names();
            let data = super::extract_data(&mut capture_names, &start_captures);
            let span_reference = self.extract_span_reference(&data);
            let id = id_from_line(&match &self.file_seed {
                Some(fs) => format!("{}|{}", fs, line),
                None => line.to_string(),
            });
            let pending_span = PendingSpan::new(id, timestamp, data, None, line.to_string());
            if self.pending.contains(&span_reference) {
                tracing::warn!("pending span {span_reference:?} found multiple times! {line}");
            } else {
                self.pending.add(span_reference, pending_span);
            }
            None
        } else if let Some(end_captures) = self.end_pattern.captures(line) {
            let mut capture_names = self.end_pattern.capture_names();
            let mut data = super::extract_data(&mut capture_names, &end_captures);
            let span_reference = self.extract_span_reference(&data);
            if let Some((_pending_reference, pending_span)) = self.pending.remove(&span_reference) {
                let Some(end_timestamp) = super::extract_timestamp(&end_captures["timestamp"], &self.timestamp_format) else {
                    warn!(line, format = self.timestamp_format, "failed to parse end timestamp");
                    return None;
                };
                data.extend(pending_span.data);
                let duration = end_timestamp - pending_span.timestamp;
                Some(Event::Span {
                    id: pending_span.id,
                    name: self.name.clone(),
                    timestamp: pending_span.timestamp,
                    data,
                    duration,
                    parent_id: pending_span.parent_id,
                    raw_lines: (pending_span.raw_line, line.to_string()),
                })
            } else {
                None
            }
        } else {
            for parser in self.nested.iter_mut() {
                if let Some(event) = parser.parse_line_with_context(line) {
                    let parent_id = self.pending.spans.iter().find_map(|(span_ref, pending_span)| {
                        let matches = self.reference_fields.iter().zip(span_ref.0.iter()).all(|(field, value)| event.data().get(field) == Some(value));
                        matches.then_some(pending_span.id)
                    });
                    return if let Some(pid) = parent_id {
                        Some(event.with_parent(pid))
                    } else {
                        None // no matching parent span → suppress
                    };
                }
            }
            None
        }
    }

    pub fn has_pending(&self) -> bool {
        !self.pending.spans.is_empty()
    }

    pub fn clean(&mut self) {
        self.pending.dirty = false;
    }

    pub fn is_dirty(&self) -> bool {
        self.pending.dirty
    }

    pub fn pending_spans(&self) -> &HashMap<SpanReference, PendingSpan> {
        &self.pending.spans
    }

    pub fn restore_pending(&mut self, spans: Vec<(Vec<String>, Uuid, NaiveDateTime, HashMap<String, String>, Option<Uuid>, String)>) {
        for (span_ref_parts, id, timestamp, data, parent_id, raw_line) in spans {
            self.pending.spans.insert(SpanReference(span_ref_parts), PendingSpan { id, timestamp, data, parent_id, raw_line });
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PendingSpans {
    spans: HashMap<SpanReference, PendingSpan>,
    dirty: bool,
}

impl PendingSpans {
    fn add(&mut self, span_reference: SpanReference, pending_span: PendingSpan) {
        self.spans.insert(span_reference, pending_span);
        self.dirty = true;
    }

    fn remove(&mut self, span_reference: &SpanReference) -> Option<(SpanReference, PendingSpan)> {
        let span = self.spans.remove_entry(span_reference);
        if span.is_some() {
            self.dirty = true;
        }
        span
    }

    fn contains(&self, span_reference: &SpanReference) -> bool {
        self.spans.contains_key(span_reference)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, NaiveDateTime};
    use regex::Regex;
    use rstest::rstest;

    use crate::parser::InternalSingleParser;
    use crate::parser::tests::TEST_ID;
    use shared::event::Event;

    use super::super::tests::common_test_data;
    use super::*;

    fn set_id(event: &mut Event, id: uuid::Uuid) {
        match event {
            Event::Span { id: eid, .. } | Event::Single { id: eid, .. } => *eid = id,
        }
    }

    fn zero_raw(e: &mut Event) {
        match e {
            Event::Span { raw_lines, .. } => *raw_lines = (String::new(), String::new()),
            Event::Single { raw_line, .. } => *raw_line = String::new(),
        }
    }

    const TS_FMT: &str = "%Y-%m-%d %H:%M:%S";
    const START: &str = r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<ref>[a-z0-9]{5})\s+START";
    const END: &str = r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<ref>[a-z0-9]{5})\s+END";

    fn test_span(data: &[(&str, &str)], timestamp: &str, duration: i64) -> Event {
        let (ts, data_map) = common_test_data(data, timestamp);
        let mut e = Event::new_span("test", NaiveDateTime::parse_from_str(&ts, TS_FMT).unwrap(), data_map, Duration::new(duration, 0).unwrap(), (String::new(), String::new()));
        set_id(&mut e, TEST_ID);
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
    fn test_span_parse(#[case] start_pattern: &str, #[case] end_pattern: &str, #[case] log: &str, #[case] expected: Vec<Event>) {
        let mut parser = InternalSpanParser::new(
            "test".into(),
            TS_FMT.into(),
            Regex::new(start_pattern).unwrap(),
            Regex::new(end_pattern).unwrap(),
            vec![Parser::Single(InternalSingleParser {
                name: "test_inner".into(),
                pattern: Regex::new(r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<ref>[a-z0-9]{5})\s+nested").unwrap(),
                timestamp_format: TS_FMT.to_string(),
                file_seed: None,
            })],
            vec!["ref".into()],
        );
        let mut actual: Vec<Event> = log.lines().filter_map(|line| parser.parse_line_with_context(line)).collect();
        actual.iter_mut().for_each(|f| { set_id(f, TEST_ID); zero_raw(f); });
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
                pattern: Regex::new(r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<ref>[a-z0-9]{5})\s+nested").unwrap(),
                timestamp_format: TS_FMT.to_string(),
                file_seed: None,
            })],
            vec!["ref".into()],
        );
        let events: Vec<Event> = log.lines().filter_map(|line| parser.parse_line_with_context(line)).collect();
        // abc02 nested has no matching parent span so is suppressed — only 2 events emitted
        assert_eq!(events.len(), 2);
        // outer span is emitted last (when END is seen)
        let outer_id = events[1].id();
        // abc01 nested: parent_id links to the outer span
        assert!(matches!(&events[0], Event::Single { name, parent_id, .. } if name == "test_inner" && parent_id == &Some(outer_id)));
        // outer span: top-level, no parent
        assert!(matches!(&events[1], Event::Span { name, parent_id, .. } if name == "test" && parent_id.is_none()));
    }
}
