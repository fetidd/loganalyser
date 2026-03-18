use std::collections::HashMap;

use regex::Regex;
use tracing::warn;
use uuid::Uuid;

use shared::event::Event;

#[derive(Debug, Clone)]
pub struct InternalSingleParser {
    pub name: String,
    pub pattern: Regex,
    pub timestamp_format: String,
}

impl InternalSingleParser {
    pub(super) fn parse(&mut self, input: &str) -> Vec<Event> {
        input
            .lines()
            .filter_map(|line| self.parse_line(line))
            .collect()
    }

    pub(super) fn parse_line_with_context(
        &mut self,
        input: &str,
        lookup: &dyn Fn(&HashMap<String, String>) -> Option<Uuid>,
    ) -> Option<Event> {
        if let Some(captures) = self.pattern.captures(input) {
            let Some(timestamp) =
                super::extract_timestamp(&captures["timestamp"], &self.timestamp_format)
            else {
                return None;
            };
            let mut capture_names = self.pattern.capture_names();
            let data = super::extract_data(&mut capture_names, &captures);
            let parent_id = lookup(&data);
            let mut event = Event::new_single(&self.name, timestamp, data);
            if let Some(pid) = parent_id {
                event = event.with_parent(pid);
            }
            return Some(event);
        }
        None
    }

    pub(super) fn parse_line(&mut self, input: &str) -> Option<Event> {
        if let Some(captures) = self.pattern.captures(input) {
            let Some(timestamp) =
                super::extract_timestamp(&captures["timestamp"], &self.timestamp_format)
            else {
                warn!(line = input, format = self.timestamp_format, "failed to parse timestamp");
                return None;
            };
            let mut capture_names = self.pattern.capture_names();
            let data = super::extract_data(&mut capture_names, &captures);
            return Some(Event::new_single(&self.name, timestamp, data));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;
    use regex::Regex;
    use rstest::rstest;

    use shared::event::Event;
    use crate::parser::tests::TEST_ID;

    use super::super::tests::common_test_data;
    use super::*;

    fn set_id(event: &mut Event, id: uuid::Uuid) {
        match event {
            Event::Span { id: eid, .. } | Event::Single { id: eid, .. } => *eid = id,
        }
    }

    const TS_FMT: &str = "%Y-%m-%d %H:%M:%S";

    fn test_single(data: &[(&str, &str)], timestamp: &str) -> Event {
        let (ts, data_map) = common_test_data(data, timestamp);
        let mut e = Event::new_single(
            "test",
            NaiveDateTime::parse_from_str(&ts, TS_FMT).unwrap(),
            data_map,
        );
        set_id(&mut e, TEST_ID);
        e
    }

    #[rstest]
    #[case(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
        "2026-01-01 12:34:56",
        vec![test_single(&[], "2026-01-01 12:34:56")],
    )]
    #[case(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (?P<level>\w+) (?P<message>.+)",
        "2026-03-05 08:00:00 INFO Server started",
        vec![test_single(&[("level", "INFO"), ("message", "Server started")], "2026-03-05 08:00:00")],
    )]
    #[case(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (?P<user>\S+) (?P<action>\S+)",
        "not a log line\n2026-06-15 09:30:00 alice LOGIN\n2026-06-15 09:30:02 steve LOGIN\nskipped line",
        vec![
            test_single(&[("user", "alice"), ("action", "LOGIN")], "2026-06-15 09:30:00"),
            test_single(&[("user", "steve"), ("action", "LOGIN")], "2026-06-15 09:30:02"),
        ],
    )]
    #[case(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
        "",
        vec![],
    )]
    #[case(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
        "   \n  \n",
        vec![],
    )] // whitespace-only lines
    #[case(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
        "2026-01-01 00:00:00 2026-02-02 00:00:00",
        vec![test_single(&[], "2026-01-01 00:00:00")],
    )] // only first match per line is captured
    fn test_single_parse(#[case] pattern: &str, #[case] log: &str, #[case] expected: Vec<Event>) {
        let mut parser = InternalSingleParser {
            name: "test".into(),
            pattern: Regex::new(pattern).unwrap(),
            timestamp_format: TS_FMT.into(),
        };
        let mut actual = parser.parse(log);
        actual.iter_mut().for_each(|f| set_id(f, TEST_ID));
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_single_parse_timestamp_format_mismatch() {
        // Pattern matches, but the captured timestamp doesn't parse with the format → line skipped
        let mut parser = InternalSingleParser {
            name: "test".into(),
            pattern: Regex::new(r"(?P<timestamp>[0-9/]+ [0-9:]+)").unwrap(),
            timestamp_format: TS_FMT.into(), // expects "%Y-%m-%d %H:%M:%S", not slash format
        };
        let actual = parser.parse("15/01/2026 08:00:00");
        assert!(actual.is_empty());
    }

    #[rstest]
    #[case(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
        "2026-01-01 00:00:00 2026-02-02 00:00:00",
        Some(test_single(&[], "2026-01-01 00:00:00"))
    )]
    fn test_parse_line(#[case] pattern: &str, #[case] line: &str, #[case] expected: Option<Event>) {
        let mut parser = InternalSingleParser {
            name: "test".into(),
            pattern: Regex::new(pattern).unwrap(),
            timestamp_format: TS_FMT.into(),
        };
        let actual = parser.parse_line(line);
        if [&actual, &expected].into_iter().all(Option::is_some) {
            let (mut actual, expected) = (actual.unwrap(), expected.unwrap());
            set_id(&mut actual, TEST_ID);
            assert_eq!(actual, expected);
        } else if ![&actual, &expected].into_iter().all(Option::is_none) {
            panic!("{actual:?} != {expected:?}");
        }
    }
}
