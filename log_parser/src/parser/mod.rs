mod single;
mod span;

use std::collections::HashMap;

use regex::{CaptureNames, Captures, Regex};

pub use single::InternalSingleParser;
pub use span::InternalSpanParser;

use crate::{
    error::{LogParserError, LogParserResult},
    event::Event,
};

struct ParseContext<'a> {
    timestamp_format: &'a str,
    reference_fields: Option<&'a [String]>,
}

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
    
    fn extract_config_type(t: &toml::Table) -> LogParserResult<&str> {
        t.get("type")
            .ok_or(error("missing type"))?
            .as_str()
            .ok_or(error("type was not a string"))
    }

    pub fn from_toml(t: &toml::Table) -> LogParserResult<Parser> {
        let config_type = Self::extract_config_type(t)?;
        let reference_fields: Option<Vec<String>> =
            if let Some(reference_fields) = t.get("reference_fields") {
                reference_fields
                    .as_array()
                    .ok_or(error("reference_fields not an array"))?
                    .into_iter()
                    .map(|v| match v.as_str() {
                        Some(s) => Ok(s.to_owned()),
                        None => Err(error("reference_fields elements must be strings")),
                    })
                    .collect::<Result<Vec<String>, LogParserError>>()?
                    .into()
            } else if config_type == "span" {
                return Err(error("reference_fields must be provided for span parsers"));
            } else {
                None
            };
        let ctx = ParseContext {
            timestamp_format: Self::parse_and_validate_str("timestamp_format", t)?,
            reference_fields: reference_fields.as_deref(),
        };
        Self::from_toml_table_and_parts(t, config_type, &ctx)
    }

    fn from_toml_table_and_parts(
        t: &toml::Table,
        config_type: &str,
        ctx: &ParseContext<'_>,
    ) -> LogParserResult<Parser> {
        let name = Self::parse_and_validate_str("name", t)?;
        match config_type {
            "span" => Self::build_span(t, name, ctx),
            "single" => Self::build_single(t, name, ctx),
            _ => todo!(),
        }
    }

    fn parse_and_validate_str<'a>(field: &str, t: &'a toml::Table) -> LogParserResult<&'a str> {
        Ok(t.get(field)
            .ok_or(error(&format!("missing {field}")))?
            .as_str()
            .ok_or(error(&format!("{field} was not a string")))?)
    }

    const REQUIRED_FIELDS: [&str; 1] = ["timestamp"];

    fn build_single(
        t: &toml::Table,
        name: &str,
        ctx: &ParseContext<'_>,
    ) -> LogParserResult<Parser> {
        let pattern = Regex::new(Self::parse_and_validate_str("pattern", t)?.into())?;
        Self::validate_required_pattern_fields(&pattern, Self::REQUIRED_FIELDS)?;
        if let Some(reference_fields) = ctx.reference_fields {
            Self::validate_required_pattern_fields(&pattern, reference_fields)?;
        }
        Ok(Parser::Single(InternalSingleParser {
            name: name.into(),
            pattern,
            timestamp_format: ctx.timestamp_format.into(),
        }))
    }

    fn build_span(
        t: &toml::Table,
        name: &str,
        ctx: &ParseContext<'_>,
    ) -> LogParserResult<Parser> {
        let nested_parsers = Self::parse_nested(t, ctx)?;
        let start_pattern = Regex::new(Self::parse_and_validate_str("start_pattern", t)?.into())?;
        let end_pattern = Regex::new(Self::parse_and_validate_str("end_pattern", t)?.into())?;
        for pattern in [&start_pattern, &end_pattern] {
            Self::validate_required_pattern_fields(pattern, Self::REQUIRED_FIELDS)?;
            if let Some(reference_fields) = ctx.reference_fields {
                Self::validate_required_pattern_fields(pattern, reference_fields)?;
            }
        }
        Ok(Parser::Span(InternalSpanParser::new(
            name.into(),
            ctx.timestamp_format.into(),
            start_pattern,
            end_pattern,
            nested_parsers,
            ctx.reference_fields.map(|r| r.to_vec()).unwrap_or_default(),
        )))
    }

    fn parse_nested(
        t: &toml::Table,
        ctx: &ParseContext<'_>,
    ) -> LogParserResult<Vec<Parser>> {
        let Some(nested) = t.get("nested") else {
            return Ok(vec![]);
        };
        let nested = nested
            .as_array()
            .ok_or(error("nested should be an array"))?;
        nested
            .iter()
            .map(|value| {
                let table = value
                    .as_table()
                    .ok_or(error("nested elements must be toml tables"))?;
                let nested_ts = match table.get("timestamp_format") {
                    Some(found) => found
                        .as_str()
                        .ok_or(error("timestamp_format was not a string"))?,
                    None => ctx.timestamp_format,
                };
                let config_type = Self::extract_config_type(table)?;
                let mut reference_fields: Vec<String> =
                    ctx.reference_fields.map(|r| r.to_vec()).unwrap_or_default();
                if let Some(nested_ref_fields) = table.get("reference_fields") {
                    let own_fields: Vec<String> = nested_ref_fields
                        .as_array()
                        .ok_or(error("reference_fields must be an array"))?
                        .iter()
                        .map(|v| {
                            v.as_str()
                                .ok_or(error("reference_fields must be strings"))
                                .map(|s| s.to_owned())
                        })
                        .collect::<LogParserResult<_>>()?;
                    if config_type == "span" {
                        for field in &own_fields {
                            if reference_fields.contains(field) {
                                return Err(error(&format!(
                                    "nested span reference field '{field}' duplicates an inherited field"
                                )));
                            }
                        }
                    }
                    reference_fields.extend(own_fields);
                } else if config_type == "span" {
                    return Err(error(
                        "nested span parsers must provide reference_fields to disambiguate from parent",
                    ));
                };
                let nested_ctx = ParseContext {
                    timestamp_format: nested_ts,
                    reference_fields: Some(&reference_fields),
                };
                Self::from_toml_table_and_parts(table, config_type, &nested_ctx)
            })
            .collect()
    }

    fn validate_required_pattern_fields(
        pattern: &Regex,
        fields: impl IntoIterator<Item: AsRef<str>>,
    ) -> LogParserResult<()> {
        let mut missing = vec![];
        for f in fields {
            if !pattern
                .capture_names()
                .any(|c| c.is_some_and(|c| c == f.as_ref()))
            {
                missing.push(f.as_ref().to_owned());
            }
        }
        if !missing.is_empty() {
            Err(error(&format!(
                "pattern missing fields: {}",
                missing.join(", ")
            )))
        } else {
            Ok(())
        }
    }

    pub fn parse(&mut self, input: &str) -> Vec<Event> {
        match self {
            Parser::Single(p) => p.parse(input),
            Parser::Span(p) => p.parse(input),
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

fn error(msg: &str) -> LogParserError {
    LogParserError::ConfigParseError(msg.to_string())
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;

    use chrono::{Duration, NaiveDateTime};

    use super::*;

    fn todo_event(timestamp: &str, todo: &str) -> Event {
        Event::Single {
            name: "gateway_todos".into(),
            timestamp: NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S").unwrap(),
            data: HashMap::from([
                ("timestamp".into(), timestamp.into()),
                ("todo".into(), todo.into()),
            ]),
            id: TEST_ID.to_string(),
        }
    }

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

    const GW_EXAMPLE: &str = include_str!("../../../gateway_example.log");
    const GW_CONFIG: &str = include_str!("../../../gateway_config.toml");
    pub(super) const TEST_ID: &str = "test_id";

    fn create_gateway_parsers() -> Vec<Parser> {
        let table: toml::Table = toml::from_str(GW_CONFIG).expect("failed to read toml to str");
        let parsers = table.get("parsers").unwrap().as_array().unwrap();
        parsers
            .iter()
            .map(|p| Parser::from_toml(p.as_table().unwrap()).unwrap())
            .collect()
    }

    #[test]
    fn parse_gateway_log() {
        let mut parsers = create_gateway_parsers();
        let events = parsers[0].parse(GW_EXAMPLE);
        let expected = 30;
        assert_eq!(events.len(), expected);
        assert_eq!(
            events[0],
            Event::Span {
                name: "".into(),
                timestamp: NaiveDateTime::parse_from_str(
                    "2026-02-02 00:00:01",
                    "%Y-%m-%d %H:%M:%S"
                )
                .unwrap(),
                data: HashMap::new(),
                duration: Duration::new(0, 0).unwrap(),
                id: events[0].id().to_string()
            }
        );
        let mut events = parsers[1].parse(GW_EXAMPLE);
        let expected = vec![
            todo_event(
                "2026-02-02 00:00:01",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'THREEDLOOKUP',)",
            ),
            todo_event(
                "2026-02-02 00:00:01",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:01",
                "W55-cQwjqjkB:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'TEST': 1, 'total': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:01",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:01",
                "W55-5ujkqBxE:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'TEST': 1, 'total': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:01",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:01",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "W55-a6ryg5ar:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'TEST': 1, 'total': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "W55-2w91GRYj:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 1, 'STFS': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "W55-BbbN06b0:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 1, 'STFS': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "W55-4knFAG75:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 1, 'STFS': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "TODO_BEFORE_ST_4_202 requests ['TRANSACTIONUPDATE'] requesttypes ('TRANSACTIONUPDATE',)",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "W55-6bR1tuNh:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 1, 'STFS': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "W55-wYNYwYgH:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 1, 'STFS': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "TODO_BEFORE_ST_4_202 requests ['TRANSACTIONUPDATE'] requesttypes ('TRANSACTIONUPDATE',)",
            ),
            todo_event(
                "2026-02-02 00:00:02",
                "TODO_BEFORE_ST_4_202 requests ['TRANSACTIONUPDATE'] requesttypes ('TRANSACTIONUPDATE',)",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "W55-q6Fepa99:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 1, 'STFS': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "W55-hm45fq3f:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 2, 'ST5PPRO': 2}",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "W55-bwBv9HY3:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 1, 'STFS': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "W55-yc5N13vH:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 1, 'STFS': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "W55-vfekR4kT:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 1, 'STFS': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "W55-eB3x96Ra:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 1, 'BARCLAYS': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'AUTH',)",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "W55-pfqRf0Qk:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 1, 'PAYSAFE': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "W55-v08A0j4R:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 1, 'HSBC': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "W55-pf4ce1Af:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 1, 'HSBC': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "TODO_BEFORE_ST_4_202 requests ['RISKDEC2'] requesttypes ('RISKDEC2',)",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "W55-0b8nF5vw:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 1, 'STFS': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "W55-GEY370H1:55-0-0    TODO_BEFORE_ST_4_200 performTransactionQuery Acquirer query limit=500 count: {'total': 1, 'HSBC': 1}",
            ),
            todo_event(
                "2026-02-02 00:00:03",
                "TODO_BEFORE_ST_4_202 requests [None] requesttypes (u'TRANSACTIONQUERY',)",
            ),
        ];
        events.iter_mut().for_each(|f| f.set_id(TEST_ID));
        assert_eq!(events, expected);
    }

    #[test]
    fn test_create_gateway_parsers() {
        let parsers = create_gateway_parsers();
        let parser = parsers[0].clone();
        let Parser::Span(req) = &parser else {
            panic!("expected gateway_request to be a Span parser");
        };
        assert_eq!(req.name, "gateway_request");
        assert_eq!(req.timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert_eq!(
            req.start_pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<requestreference>\S+)\s+InHeads:(?P<headers>\{[^\}]*\})\s+Apache:(?P<apachereference>\S+)"
        );
        assert_eq!(
            req.end_pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<requestreference>\S+)\s+Return (?P<response_bytes>\d+)bytes to client\s(?P<username>\S+)\s+(?P<time_taken>[0-9\.]+)s"
        );
        assert_eq!(req.reference_fields, &["requestreference"]);
        assert_eq!(req.nested.len(), 1);

        let Parser::Span(txn) = &req.nested[0] else {
            panic!("expected gateway_transaction to be a Span parser");
        };
        assert_eq!(txn.name, "gateway_transaction");
        assert_eq!(txn.timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert_eq!(
            txn.start_pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<requestreference>\S+):(?P<transactionreference>\S+)\s+Bgn:\s+(?P<interface>\S+)\s+(?P<requesttypedescription>\S+)\s+(?P<accounttypedescription>\S+)\s+(?P<sitereference>\S+)\s+(?P<paymenttypedescription>\S+)\s+(?P<currencyiso3a>\S+)\s+(?P<mainamount>\S+)\s+Status:(?P<status>\S+)"
        );
        assert_eq!(
            txn.end_pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<requestreference>\S+):(?P<transactionreference>\S+)\s+End:\s+(?P<interface>\S+)\s+(?P<requesttypedescription>\S+)\s+(?P<accounttypedescription>\S+)\s+(?P<sitereference>\S+)\s+(?P<paymenttypedescription>\S+)\s+(?P<currencyiso3a>\S+)\s+(?P<mainamount>\S+)\s+Status:(?P<status>\S+)\s+E:(?P<errorcode>\S+)"
        );
        assert_eq!(
            txn.reference_fields,
            &["requestreference", "transactionreference"]
        );
        assert_eq!(txn.nested.len(), 2);

        let Parser::Single(txn_req) = &txn.nested[0] else {
            panic!("expected gateway_transaction_request to be a Single parser");
        };
        assert_eq!(txn_req.name, "gateway_transaction_request");
        assert_eq!(txn_req.timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert_eq!(
            txn_req.pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<requestreference>\S+):(?P<transactionreference>\S+)\s+REQ:(?P<encrypted_request>\S+)"
        );

        let Parser::Single(txn_res) = &txn.nested[1] else {
            panic!("expected gateway_transaction_response to be a Single parser");
        };
        assert_eq!(txn_res.name, "gateway_transaction_response");
        assert_eq!(txn_res.timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert_eq!(
            txn_res.pattern.as_str(),
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<requestreference>\S+):(?P<transactionreference>\S+)\s+RES:(?P<encrypted_response>\S+)"
        );
    }

    // ---- Additional imports for unit tests below ----
    use regex::Regex;
    use rstest::rstest;

    // ---- TOML fixtures ----

    const SINGLE_VALID: &str = r#"
type = "single"
name = "my_parser"
timestamp_format = "%Y-%m-%d %H:%M:%S"
pattern = '(?P<timestamp>\d+) (?P<level>\w+)'
"#;

    const SPAN_VALID: &str = r#"
type = "span"
name = "my_span"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) END'
reference_fields = ["ref"]
"#;

    const SPAN_EMPTY_NESTED: &str = r#"
type = "span"
name = "my_span"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = []
"#;

    const SPAN_ONE_NESTED: &str = r#"
type = "span"
name = "my_span"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "single", name = "n", pattern = '(?P<timestamp>.+) (?P<ref>\S+)' }]
"#;

    const SPAN_TWO_NESTED: &str = r#"
type = "span"
name = "my_span"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [
  { type = "single", name = "n1", pattern = '(?P<timestamp>.+) (?P<ref>\S+)' },
  { type = "single", name = "n2", pattern = '(?P<timestamp>.+) (?P<ref>\S+)' }
]
"#;

    const SPAN_NESTED_INHERITS_TS_FMT: &str = r#"
type = "span"
name = "my_span"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "single", name = "n", pattern = '(?P<timestamp>.+) (?P<ref>\S+)' }]
"#;

    const SPAN_NESTED_OVERRIDES_TS_FMT: &str = r#"
type = "span"
name = "my_span"
timestamp_format = "%Y-%m-%d %H:%M:%S"
start_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>\d+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "single", name = "n", pattern = '(?P<timestamp>.+) (?P<ref>\S+)', timestamp_format = "%Y" }]
"#;

    // ---- extract_timestamp ----

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

    // ---- extract_data ----

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

    // ---- parse_and_validate_str ----

    #[rstest]
    #[case(r#"name = "foo""#, "name", "foo")]
    #[case(r#"name = """#, "name", "")]
    fn test_parse_and_validate_str_ok(
        #[case] toml_str: &str,
        #[case] field: &str,
        #[case] expected: &str,
    ) {
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        assert_eq!(Parser::parse_and_validate_str(field, &t).unwrap(), expected);
    }

    #[rstest]
    #[case(r#"other = "x""#, "name", "missing name")]
    #[case(r#"name = 42"#, "name", "name was not a string")]
    #[case(r#"name = ["a", "b"]"#, "name", "name was not a string")]
    #[case(r#"name = true"#, "name", "name was not a string")]
    fn test_parse_and_validate_str_err(
        #[case] toml_str: &str,
        #[case] field: &str,
        #[case] expected_err: &str,
    ) {
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let err = Parser::parse_and_validate_str(field, &t).unwrap_err();
        assert!(
            err.to_string().contains(expected_err),
            "expected error containing {expected_err:?}, got: {}",
            err.to_string()
        );
    }

    // ---- extract_config_type ----

    #[rstest]
    #[case(r#"type = "span""#, "span")]
    #[case(r#"type = "single""#, "single")]
    fn test_extract_config_type_ok(#[case] toml_str: &str, #[case] expected: &str) {
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        assert_eq!(Parser::extract_config_type(&t).unwrap(), expected);
    }

    #[rstest]
    #[case(r#"other = "x""#, "missing type")]
    #[case(r#"type = 42"#, "type was not a string")]
    #[case(r#"type = true"#, "type was not a string")]
    fn test_extract_config_type_err(#[case] toml_str: &str, #[case] expected_err: &str) {
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let err = Parser::extract_config_type(&t).unwrap_err();
        assert!(
            err.to_string().contains(expected_err),
            "expected error containing {expected_err:?}, got: {}",
            err.to_string()
        );
    }

    // ---- validate_required_pattern_fields ----

    #[rstest]
    #[case(r"(?P<timestamp>\d+) (?P<level>\w+)", vec!["timestamp", "level"])]
    #[case(r"(?P<timestamp>\d+)", vec!["timestamp"])]
    #[case(r"(?P<timestamp>\d+)", vec![])]
    #[case(r"(?P<timestamp>\d+) (?P<extra>.+)", vec!["timestamp"])] // extra fields in pattern are fine
    fn test_validate_required_pattern_fields_ok(#[case] pattern: &str, #[case] fields: Vec<&str>) {
        let re = Regex::new(pattern).unwrap();
        assert!(Parser::validate_required_pattern_fields(&re, fields).is_ok());
    }

    #[rstest]
    #[case(r"(?P<level>\w+)", vec!["timestamp"], "timestamp")]
    #[case(r"(?P<other>\w+)", vec!["timestamp", "level"], "timestamp")] // both missing, error mentions first
    #[case(r"(\d+)(\w+)", vec!["timestamp"], "timestamp")] // only unnamed groups
    fn test_validate_required_pattern_fields_err(
        #[case] pattern: &str,
        #[case] fields: Vec<&str>,
        #[case] expected_in_err: &str,
    ) {
        let re = Regex::new(pattern).unwrap();
        let err = Parser::validate_required_pattern_fields(&re, fields).unwrap_err();
        assert!(
            err.to_string().contains(expected_in_err),
            "expected error containing {expected_in_err:?}, got: {}",
            err.to_string()
        );
    }

    // ---- from_toml — Single parser ----

    #[test]
    fn test_from_toml_single_valid() {
        let t: toml::Table = toml::from_str(SINGLE_VALID).unwrap();
        let parser = Parser::from_toml(&t).unwrap();
        let Parser::Single(p) = parser else {
            panic!("expected Single")
        };
        assert_eq!(p.name, "my_parser");
        assert_eq!(p.timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert!(p.pattern.capture_names().any(|c| c == Some("timestamp")));
        assert!(p.pattern.capture_names().any(|c| c == Some("level")));
    }

    #[rstest]
    #[case(
        r#"name = "t"
timestamp_format = "%Y"
pattern = '(?P<timestamp>.+)'"#,
        "missing type"
    )]
    #[case(
        r#"type = "single"
timestamp_format = "%Y"
pattern = '(?P<timestamp>.+)'"#,
        "missing name"
    )]
    #[case(
        r#"type = "single"
name = "t"
pattern = '(?P<timestamp>.+)'"#,
        "missing timestamp_format"
    )]
    #[case(
        r#"type = "single"
name = "t"
timestamp_format = "%Y""#,
        "missing pattern"
    )]
    #[case(
        r#"type = "single"
name = "t"
timestamp_format = "%Y"
pattern = '[invalid'"#,
        "bad regex"
    )]
    #[case(
        r#"type = "single"
name = "t"
timestamp_format = "%Y"
pattern = '(?P<level>\w+)'"#,
        "timestamp"
    )] // pattern missing timestamp capture group
    fn test_from_toml_single_err(#[case] toml_str: &str, #[case] expected_err: &str) {
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let err = Parser::from_toml(&t).unwrap_err();
        assert!(
            err.to_string().contains(expected_err),
            "expected error containing {expected_err:?}, got: {}",
            err.to_string()
        );
    }

    // ---- from_toml — Span parser ----

    #[test]
    fn test_from_toml_span_valid() {
        let t: toml::Table = toml::from_str(SPAN_VALID).unwrap();
        let parser = Parser::from_toml(&t).unwrap();
        let Parser::Span(p) = parser else {
            panic!("expected Span")
        };
        assert_eq!(p.name, "my_span");
        assert_eq!(p.timestamp_format, "%Y-%m-%d %H:%M:%S");
        assert_eq!(p.reference_fields, &["ref"]);
        assert_eq!(p.nested.len(), 0);
    }

    #[rstest]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'"#,
        "reference_fields must be provided for span parsers"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = "ref""#,
        "reference_fields not an array"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = [1, 2]"#,
        "reference_fields elements must be strings"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = ["ref"]"#,
        "missing start_pattern"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
reference_fields = ["ref"]"#,
        "missing end_pattern"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '[invalid'
end_pattern = '(?P<timestamp>.+) END'
reference_fields = []"#,
        "bad regex"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<ref>\S+) START'
end_pattern = '(?P<ref>\S+) END'
reference_fields = ["ref"]"#,
        "timestamp"
    )] // patterns missing timestamp group
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) START'
end_pattern = '(?P<timestamp>.+) END'
reference_fields = ["ref"]"#,
        "ref"
    )] // reference field not in patterns
    fn test_from_toml_span_err(#[case] toml_str: &str, #[case] expected_err: &str) {
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let err = Parser::from_toml(&t).unwrap_err();
        assert!(
            err.to_string().contains(expected_err),
            "expected error containing {expected_err:?}, got: {}",
            err.to_string()
        );
    }

    // ---- parse_nested (tested via from_toml on span configs) ----

    #[rstest]
    #[case(SPAN_VALID, 0)] // no nested key
    #[case(SPAN_EMPTY_NESTED, 0)] // nested = []
    #[case(SPAN_ONE_NESTED, 1)] // one nested single
    #[case(SPAN_TWO_NESTED, 2)] // two nested singles
    fn test_parse_nested_count(#[case] toml_str: &str, #[case] expected_count: usize) {
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let Parser::Span(p) = Parser::from_toml(&t).unwrap() else {
            panic!("expected Span");
        };
        assert_eq!(p.nested.len(), expected_count);
    }

    #[rstest]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) START'
end_pattern = '(?P<timestamp>.+) END'
reference_fields = []
nested = "not an array""#,
        "nested should be an array"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) START'
end_pattern = '(?P<timestamp>.+) END'
reference_fields = []
nested = ["not a table"]"#,
        "nested elements must be toml tables"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) START'
end_pattern = '(?P<timestamp>.+) END'
reference_fields = []
nested = [{ name = "n", pattern = '(?P<timestamp>.+)' }]"#,
        "missing type"
    )]
    #[case(
        r#"type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) START'
end_pattern = '(?P<timestamp>.+) END'
reference_fields = []
nested = [{ type = "single", name = "n", pattern = '(?P<timestamp>.+)', timestamp_format = 42 }]"#,
        "timestamp_format was not a string"
    )]
    fn test_parse_nested_err(#[case] toml_str: &str, #[case] expected_err: &str) {
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let err = Parser::from_toml(&t).unwrap_err();
        assert!(
            err.to_string().contains(expected_err),
            "expected error containing {expected_err:?}, got: {}",
            err.to_string()
        );
    }

    #[test]
    fn test_nested_inherits_parent_timestamp_format() {
        let t: toml::Table = toml::from_str(SPAN_NESTED_INHERITS_TS_FMT).unwrap();
        let Parser::Span(p) = Parser::from_toml(&t).unwrap() else {
            panic!()
        };
        let Parser::Single(nested) = &p.nested[0] else {
            panic!()
        };
        assert_eq!(nested.timestamp_format, "%Y-%m-%d %H:%M:%S");
    }

    #[test]
    fn test_nested_overrides_timestamp_format() {
        let t: toml::Table = toml::from_str(SPAN_NESTED_OVERRIDES_TS_FMT).unwrap();
        let Parser::Span(p) = Parser::from_toml(&t).unwrap() else {
            panic!()
        };
        let Parser::Single(nested) = &p.nested[0] else {
            panic!()
        };
        assert_eq!(nested.timestamp_format, "%Y");
    }

    // ---- reference_fields inheritance ----

    #[test]
    fn test_top_level_span_requires_reference_fields() {
        let toml_str = r#"
type = "span"
name = "t"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) START'
end_pattern = '(?P<timestamp>.+) END'
"#;
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let err = Parser::from_toml(&t).unwrap_err();
        assert!(
            err.to_string().contains("reference_fields must be provided for span parsers"),
            "got: {err}"
        );
    }

    #[test]
    fn test_nested_single_inherits_parent_reference_fields() {
        // parent has reference_fields = ["ref"]; nested single pattern includes (?P<ref>...)
        let toml_str = r#"
type = "span"
name = "outer"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "single", name = "inner", pattern = '(?P<timestamp>.+) (?P<ref>\S+)' }]
"#;
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        assert!(Parser::from_toml(&t).is_ok());
    }

    #[test]
    fn test_nested_single_err_if_pattern_missing_parent_reference_field() {
        // parent has reference_fields = ["ref"]; nested single pattern has no (?P<ref>...)
        let toml_str = r#"
type = "span"
name = "outer"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "single", name = "inner", pattern = '(?P<timestamp>.+)' }]
"#;
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let err = Parser::from_toml(&t).unwrap_err();
        assert!(
            err.to_string().contains("ref"),
            "expected error mentioning 'ref', got: {err}"
        );
    }

    #[test]
    fn test_nested_span_inherits_parent_and_adds_own_reference_fields() {
        // inner span patterns contain both the inherited "ref" and its own "sub"
        let toml_str = r#"
type = "span"
name = "outer"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "span", name = "inner", start_pattern = '(?P<timestamp>.+) (?P<ref>\S+):(?P<sub>\S+) START', end_pattern = '(?P<timestamp>.+) (?P<ref>\S+):(?P<sub>\S+) END', reference_fields = ["sub"] }]
"#;
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let parser = Parser::from_toml(&t).unwrap();
        let Parser::Span(outer) = parser else { panic!("expected Span") };
        let Parser::Span(inner) = &outer.nested[0] else { panic!("expected nested Span") };
        // combined: parent's ["ref"] prepended to own ["sub"]
        assert_eq!(inner.reference_fields, &["ref", "sub"]);
    }

    #[test]
    fn test_nested_span_err_if_no_own_reference_fields() {
        // nested span omits reference_fields entirely — must provide at least one to disambiguate
        let toml_str = r#"
type = "span"
name = "outer"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "span", name = "inner", start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START', end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END' }]
"#;
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let err = Parser::from_toml(&t).unwrap_err();
        assert!(
            err.to_string().contains("nested span parsers must provide reference_fields to disambiguate from parent"),
            "got: {err}"
        );
    }

    #[test]
    fn test_nested_span_err_if_own_reference_field_duplicates_inherited() {
        // nested span lists "ref" in its own reference_fields, but "ref" is already inherited
        let toml_str = r#"
type = "span"
name = "outer"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "span", name = "inner", start_pattern = '(?P<timestamp>.+) (?P<ref>\S+):(?P<sub>\S+) START', end_pattern = '(?P<timestamp>.+) (?P<ref>\S+):(?P<sub>\S+) END', reference_fields = ["ref", "sub"] }]
"#;
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let err = Parser::from_toml(&t).unwrap_err();
        assert!(
            err.to_string().contains("duplicates an inherited field"),
            "got: {err}"
        );
    }

    #[test]
    fn test_nested_span_err_if_patterns_missing_inherited_reference_field() {
        // inner span patterns contain its own "sub" but NOT the inherited "ref"
        let toml_str = r#"
type = "span"
name = "outer"
timestamp_format = "%Y"
start_pattern = '(?P<timestamp>.+) (?P<ref>\S+) START'
end_pattern = '(?P<timestamp>.+) (?P<ref>\S+) END'
reference_fields = ["ref"]
nested = [{ type = "span", name = "inner", start_pattern = '(?P<timestamp>.+) (?P<sub>\S+) START', end_pattern = '(?P<timestamp>.+) (?P<sub>\S+) END', reference_fields = ["sub"] }]
"#;
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let err = Parser::from_toml(&t).unwrap_err();
        assert!(
            err.to_string().contains("ref"),
            "expected error mentioning 'ref', got: {err}"
        );
    }

    // ---- Parser::name() and Parser::timestamp_format() ----

    #[rstest]
    #[case(SINGLE_VALID, "my_parser", "%Y-%m-%d %H:%M:%S")]
    #[case(SPAN_VALID, "my_span", "%Y-%m-%d %H:%M:%S")]
    fn test_parser_name_and_timestamp_format(
        #[case] toml_str: &str,
        #[case] expected_name: &str,
        #[case] expected_ts_fmt: &str,
    ) {
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let parser = Parser::from_toml(&t).unwrap();
        assert_eq!(parser.name(), expected_name);
        assert_eq!(parser.timestamp_format(), expected_ts_fmt);
    }
}
