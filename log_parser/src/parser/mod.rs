mod build;
mod single;
mod span;

use std::collections::HashMap;

use regex::{CaptureNames, Captures};

pub use single::InternalSingleParser;
pub use span::InternalSpanParser;
use uuid::Uuid;

use shared::event::Event;


#[derive(Debug, Clone)]
pub enum Parser {
    Single(InternalSingleParser),
    Span(InternalSpanParser),
}

impl Parser {
    pub fn name(&self) -> &str {
        match self {
            Parser::Single(p) => &p.name,
            Parser::Span(p) => &p.name,
        }
    }

    pub fn timestamp_format(&self) -> &str {
        match self {
            Parser::Single(p) => &p.timestamp_format,
            Parser::Span(p) => &p.timestamp_format,
        }
    }

    pub fn parse(&mut self, input: &str) -> Vec<Event> {
        match self {
            Parser::Single(p) => p.parse(input),
            Parser::Span(p) => p.parse(input),
        }
    }

    fn parse_line_with_context(
        &mut self,
        line: &str,
        lookup: &dyn Fn(&HashMap<String, String>) -> Option<Uuid>,
    ) -> Vec<Event> {
        match self {
            Parser::Single(internal) => internal
                .parse_line_with_context(line, lookup)
                .into_iter()
                .collect(),
            Parser::Span(internal) => internal.parse_line_with_context(line, lookup),
        }
    }
}

pub(super) fn extract_timestamp(ts: &str, timestamp_format: &str) -> Option<chrono::NaiveDateTime> {
    chrono::NaiveDateTime::parse_from_str(ts, timestamp_format).ok()
}

pub(super) fn extract_data(
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

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;

    use chrono::NaiveDateTime;
    use regex::Regex;
    use rstest::rstest;
    use uuid::Uuid;

    use super::*;

    pub(crate) fn common_test_data(
        data: &[(&str, &str)],
        timestamp: &str,
    ) -> (String, HashMap<String, String>) {
        let mut data_map = HashMap::from([("timestamp".into(), timestamp.to_string())]);
        for (k, v) in data.iter() {
            data_map.insert(k.to_string(), v.to_string());
        }
        (timestamp.to_owned(), data_map)
    }

    pub(super) const TEST_ID: Uuid = Uuid::from_u128(0);

    #[rstest]
    #[case("2026-01-15 08:30:00", "%Y-%m-%d %H:%M:%S",
        Some(NaiveDateTime::parse_from_str("2026-01-15 08:30:00", "%Y-%m-%d %H:%M:%S").unwrap()))]
    #[case("2026-01-01 00:00:00", "%Y-%m-%d %H:%M:%S",
        Some(NaiveDateTime::parse_from_str("2026-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap()))]
    #[case("2026-12-31 23:59:59", "%Y-%m-%d %H:%M:%S",
        Some(NaiveDateTime::parse_from_str("2026-12-31 23:59:59", "%Y-%m-%d %H:%M:%S").unwrap()))]
    #[case("15/01/2026 08:30:00", "%d/%m/%Y %H:%M:%S",
        Some(NaiveDateTime::parse_from_str("15/01/2026 08:30:00", "%d/%m/%Y %H:%M:%S").unwrap()))]
    #[case("", "%Y-%m-%d %H:%M:%S", None)]
    #[case("not a timestamp", "%Y-%m-%d %H:%M:%S", None)]
    #[case("2026-01-15 08:30:00", "%d/%m/%Y %H:%M:%S", None)] // value doesn't match format
    #[case("2026-01-15", "%Y-%m-%d %H:%M:%S", None)] // date only, format expects time
    #[case("2026-13-01 00:00:00", "%Y-%m-%d %H:%M:%S", None)] // invalid month
    #[case("2026-01-15 08:30:00 extra", "%Y-%m-%d %H:%M:%S", None)] // trailing chars
    fn test_extract_timestamp(
        #[case] ts: &str,
        #[case] fmt: &str,
        #[case] expected: Option<NaiveDateTime>,
    ) {
        assert_eq!(extract_timestamp(ts, fmt), expected);
    }

    #[rstest]
    #[case(r"(?P<level>\w+)", "INFO", vec![("level", "INFO")])]
    #[case(r"(?P<ts>\d+) (?P<msg>.+)", "1234 hello world", vec![("ts", "1234"), ("msg", "hello world")])]
    #[case(r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})", "2026-03-07",
        vec![("year", "2026"), ("month", "03"), ("day", "07")])]
    #[case(r"(?P<a>\w+)(?P<b> \w+)?", "hello", vec![("a", "hello")])] // optional group absent
    #[case(r"(\d+) (?P<name>\w+)", "42 alice", vec![("name", "alice")])] // unnamed group excluded
    #[case(r"\d+", "99", vec![])] // no named groups at all
    #[case(r"(?P<prefix>a?)b", "b", vec![("prefix", "")])] // named group captures empty string
    fn test_extract_data(
        #[case] pattern: &str,
        #[case] input: &str,
        #[case] expected: Vec<(&str, &str)>,
    ) {
        let re = Regex::new(pattern).unwrap();
        let captures = re.captures(input).unwrap();
        let mut capture_names = re.capture_names();
        let actual = extract_data(&mut capture_names, &captures);
        let expected_map: HashMap<String, String> = expected
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        assert_eq!(actual, expected_map);
    }
}
