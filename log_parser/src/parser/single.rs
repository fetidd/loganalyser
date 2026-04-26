use regex::Regex;
use tracing::warn;

use crate::pending_span::id_from_line;
use shared::event::Event;

#[derive(Debug, Clone)]
pub struct InternalSingleParser {
    pub name: String,
    pub pattern: Regex,
    pub timestamp_format: String,
    pub file_seed: Option<String>,
}

impl InternalSingleParser {
    pub(super) fn parse_line_with_context(&mut self, input: &str) -> Option<Event> {
        if let Some(captures) = self.pattern.captures(input) {
            let Some(timestamp) = super::extract_timestamp(&captures["timestamp"], &self.timestamp_format) else {
                warn!(line = input, format = self.timestamp_format, "failed to parse timestamp");
                return None;
            };
            let mut capture_names = self.pattern.capture_names();
            let data = super::extract_data(&mut capture_names, &captures);
            let seed = match &self.file_seed {
                Some(fs) => format!("{}|{}", fs, input),
                None => input.to_string(),
            };
            Some(Event::new_single(&self.name, timestamp, data, input.to_string()).with_id(id_from_line(&seed)))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;
    use regex::Regex;
    use rstest::rstest;

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

    fn test_single(data: &[(&str, &str)], timestamp: &str) -> Event {
        let (ts, data_map) = common_test_data(data, timestamp);
        let mut e = Event::new_single("test", NaiveDateTime::parse_from_str(&ts, TS_FMT).unwrap(), data_map, String::new());
        set_id(&mut e, TEST_ID);
        e
    }

    #[rstest]
    #[case(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
        "2026-01-01 12:34:56",
        Some(test_single(&[], "2026-01-01 12:34:56")),
    )]
    #[case(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (?P<level>\w+) (?P<message>.+)",
        "2026-03-05 08:00:00 INFO Server started",
        Some(test_single(&[("level", "INFO"), ("message", "Server started")], "2026-03-05 08:00:00")),
    )]
    #[case(r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", "", None)]
    #[case(r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", "   \n  \n", None)] // whitespace-only lines
    #[case(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
        "2026-01-01 00:00:00 2026-02-02 00:00:00",
        Some(test_single(&[], "2026-01-01 00:00:00")),
    )] // only first match per line is captured
    fn test_single_parse(#[case] pattern: &str, #[case] log: &str, #[case] expected: Option<Event>) {
        let mut parser = InternalSingleParser {
            name: "test".into(),
            pattern: Regex::new(pattern).unwrap(),
            timestamp_format: TS_FMT.into(),
            file_seed: None,
        };
        let mut actual = parser.parse_line_with_context(log);
        actual.iter_mut().for_each(|f| { set_id(f, TEST_ID); zero_raw(f); });
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_single_parse_timestamp_format_mismatch() {
        // Pattern matches, but the captured timestamp doesn't parse with the format → line skipped
        let mut parser = InternalSingleParser {
            name: "test".into(),
            pattern: Regex::new(r"(?P<timestamp>[0-9/]+ [0-9:]+)").unwrap(),
            timestamp_format: TS_FMT.into(), // expects "%Y-%m-%d %H:%M:%S", not slash format
            file_seed: None,
        };
        let actual = parser.parse_line_with_context("15/01/2026 08:00:00");
        assert!(actual.is_none());
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
            file_seed: None,
        };
        let actual = parser.parse_line_with_context(line);
        if [&actual, &expected].into_iter().all(Option::is_some) {
            let (mut actual, expected) = (actual.unwrap(), expected.unwrap());
            set_id(&mut actual, TEST_ID);
            zero_raw(&mut actual);
            assert_eq!(actual, expected);
        } else if ![&actual, &expected].into_iter().all(Option::is_none) {
            panic!("{actual:?} != {expected:?}");
        }
    }
}
