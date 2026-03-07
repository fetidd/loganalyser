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

    pub fn from_toml(t: &toml::Table) -> LogParserResult<Parser> {
        Self::from_toml_table_and_timestamp_format(
            t,
            Self::parse_and_validate_str("timestamp_format", t)?,
        )
    }

    fn from_toml_table_and_timestamp_format(
        t: &toml::Table,
        ts_fmt: &str,
    ) -> LogParserResult<Parser> {
        let config_type = t
            .get("type")
            .ok_or(error("missing type"))?
            .as_str()
            .ok_or(error("type was not a string"))?;
        let builder = match config_type {
            "span" => Self::build_span,
            "single" => Self::build_single,
            _ => todo!(),
        };
        let name = Self::parse_and_validate_str("name", t)?;
        builder(t, name, ts_fmt)
    }

    fn parse_and_validate_str<'a>(field: &str, t: &'a toml::Table) -> LogParserResult<&'a str> {
        Ok(t.get(field)
            .ok_or(error(&format!("missing {field}")))?
            .as_str()
            .ok_or(error(&format!("{field} was not a string")))?)
    }

    fn build_span(t: &toml::Table, name: &str, timestamp_format: &str) -> LogParserResult<Parser> {
        let reference_fields = t
            .get("reference_fields")
            .ok_or(error("missing reference_fields"))?
            .as_array()
            .ok_or(error("reference_fields not an array"))?
            .into_iter()
            .map(|v| match v.as_str() {
                Some(s) => Ok(s.to_owned()),
                None => Err(error("reference_fields elements must be strings")),
            })
            .collect::<Result<Vec<String>, LogParserError>>()?;
        let mut nested_parsers = vec![];
        if let Some(nested) = t.get("nested") {
            if let Some(nested) = nested.as_array() {
                for value in nested {
                    let table = value
                        .as_table()
                        .ok_or(error("nested elements must be toml tables"))?;
                    let nested_ts = match table.get("timestamp_format") {
                        Some(found) => found
                            .as_str()
                            .ok_or(error("timestamp_format was not a string"))?,
                        None => timestamp_format,
                    };
                    let parser = Self::from_toml_table_and_timestamp_format(table, nested_ts)?;
                    nested_parsers.push(parser);
                }
            } else {
                return Err(error("nested should be an array"));
            }
        }
        let start_pattern = Regex::new(Self::parse_and_validate_str("start_pattern", t)?.into())?;
        let end_pattern = Regex::new(Self::parse_and_validate_str("end_pattern", t)?.into())?;
        for pattern in [&start_pattern, &end_pattern] {
            Self::validate_required_pattern_fields(
                pattern,
                Self::REQUIRED_FIELDS
                    .iter()
                    .copied()
                    .chain(reference_fields.iter().map(|s| s.as_str())),
            )?;
        }
        Ok(Parser::Span(InternalSpanParser::new(
            name.into(),
            timestamp_format.into(),
            start_pattern,
            end_pattern,
            nested_parsers,
            reference_fields,
        )))
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

    const REQUIRED_FIELDS: [&str; 1] = ["timestamp"];

    fn build_single(
        t: &toml::Table,
        name: &str,
        timestamp_format: &str,
    ) -> LogParserResult<Parser> {
        let pattern = Regex::new(Self::parse_and_validate_str("pattern", t)?.into())?;
        Self::validate_required_pattern_fields(&pattern, Self::REQUIRED_FIELDS)?;
        Ok(Parser::Single(InternalSingleParser {
            name: name.into(),
            pattern,
            timestamp_format: timestamp_format.into(),
        }))
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

    use chrono::NaiveDateTime;

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
        // let events = parsers[0].parse(GW_EXAMPLE);
        // let expected = 30;
        // assert_eq!(events.len(), expected);
        // assert_eq!(
        //     events[0],
        //     Event::Span {
        //         name: "".into(),
        //         timestamp: NaiveDateTime::parse_from_str(
        //             "2026-02-02 00:00:01",
        //             "%Y-%m-%d %H:%M:%S"
        //         )
        //         .unwrap(),
        //         data: HashMap::new(),
        //         duration: Duration::new(0, 0).unwrap(),
        //         id: events[0].id().to_string()
        //     }
        // );
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
}
