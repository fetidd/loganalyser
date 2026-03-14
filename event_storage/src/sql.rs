use std::collections::HashMap;

use chrono::{Duration, NaiveDateTime};
use shared::event::Event;
use uuid::Uuid;

use crate::event_filter::{Cmp, Expr, Filter, Predicate};

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum ParamValue {
    String(String),
    SignedNumber(i64),
}

impl From<i64> for ParamValue {
    fn from(n: i64) -> Self {
        ParamValue::SignedNumber(n)
    }
}

impl From<String> for ParamValue {
    fn from(s: String) -> Self {
        ParamValue::String(s)
    }
}

impl From<&str> for ParamValue {
    fn from(s: &str) -> Self {
        ParamValue::String(s.to_owned())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Params(pub(crate) String, pub(crate) Vec<ParamValue>);

impl Params {
    pub(crate) fn new() -> Self {
        Self(String::new(), vec![])
    }

    pub(crate) fn add(&mut self, sql: &str, binds: &[ParamValue]) {
        self.0.push_str(sql);
        self.1.extend(binds.to_vec());
    }
}

pub(crate) struct EventForInsert {
    pub id: String,
    pub event_type: &'static str,
    pub name: String,
    pub timestamp: String,
    pub duration_ms: Option<i64>,
    pub parent_id: Option<String>,
    pub data_json: String,
}

impl EventForInsert {
    pub(crate) fn from_event(event: &Event) -> crate::Result<Self> {
        match event {
            Event::Span { id, name, timestamp, data, duration, parent_id } => Ok(Self {
                id: id.to_string(),
                event_type: "span",
                name: name.clone(),
                timestamp: timestamp.to_string(),
                duration_ms: Some(duration.num_milliseconds()),
                parent_id: parent_id.map(|p| p.to_string()),
                data_json: serde_json::to_string(data)?,
            }),
            Event::Single { id, name, timestamp, data, parent_id } => Ok(Self {
                id: id.to_string(),
                event_type: "single",
                name: name.clone(),
                timestamp: timestamp.to_string(),
                duration_ms: None,
                parent_id: parent_id.map(|p| p.to_string()),
                data_json: serde_json::to_string(data)?,
            }),
        }
    }

    pub(crate) fn insert_sql() -> &'static str {
        "INSERT INTO events (id, event_type, name, timestamp, duration_ms, parent_id, data) \
         VALUES (?, ?, ?, ?, ?, ?, ?)"
    }
}

pub(crate) fn build_event(
    id: Uuid,
    event_type: String,
    name: String,
    timestamp: NaiveDateTime,
    data_json: String,
    parent_id: Option<Uuid>,
    duration_ms: Option<i64>,
) -> crate::Result<Event> {
    let data: HashMap<String, String> = serde_json::from_str(&data_json)?;
    match event_type.as_str() {
        "span" => Ok(Event::Span {
            id,
            name,
            timestamp,
            data,
            duration: Duration::milliseconds(duration_ms.unwrap_or(0)),
            parent_id,
        }),
        "single" => Ok(Event::Single { id, name, timestamp, data, parent_id }),
        other => Err(crate::Error::Storage(format!("unknown event_type: {other}"))),
    }
}

pub(crate) trait Dialect {
    fn placeholder(&mut self) -> String;
    fn json_condition(&mut self, field: &str, op: &str, val: String) -> (String, Vec<ParamValue>);
}

pub(crate) fn build_where(filter: &Filter, dialect: &mut impl Dialect) -> Params {
    let mut params = Params::new();
    if let Some(expr) = filter.expr() {
        build_expr(expr, &mut params, false, dialect);
        params.0 = format!(" WHERE {}", params.0);
    }
    params
}

fn build_expr(expr: &Expr, params: &mut Params, and_parent: bool, dialect: &mut impl Dialect) {
    match expr {
        Expr::Condition(predicate) => build_predicate(predicate, params, dialect),
        Expr::And(exprs) => build_and(exprs, params, dialect),
        Expr::Or(exprs) => build_or(exprs, params, and_parent, dialect),
    }
}

fn build_predicate(predicate: &Predicate, params: &mut Params, dialect: &mut impl Dialect) {
    match predicate {
        Predicate::Data(cmp) => build_data(cmp, params, dialect),
        Predicate::Timestamp(cmp) => build_string_column("timestamp", cmp, params, dialect),
        Predicate::Id(cmp) => build_string_column("id", cmp, params, dialect),
        Predicate::ParentId(cmp) => build_string_column("parent_id", cmp, params, dialect),
        Predicate::Duration(cmp) => build_i64_column("duration_ms", cmp, params, dialect),
        Predicate::Name(cmp) => build_string_column("name", cmp, params, dialect),
    }
}

fn build_data(cmp: &Cmp<String>, params: &mut Params, dialect: &mut impl Dialect) {
    match cmp {
        Cmp::Json(field, inner) => {
            let (op, val) = match &**inner {
                Cmp::Eq(s) => ("=", s.clone()),
                Cmp::Like(s) => ("LIKE", s.clone()),
                _ => panic!("only = or LIKE supported for data"),
            };
            let (sql, binds) = dialect.json_condition(field, op, val);
            params.add(&sql, &binds);
        }
        other => panic!("data can not be filtered by {other:?}"),
    }
}

fn build_string_column(
    column: &str,
    cmp: &Cmp<String>,
    params: &mut Params,
    dialect: &mut impl Dialect,
) {
    match cmp {
        Cmp::In(vals) => {
            let placeholders = vals
                .iter()
                .map(|_| dialect.placeholder())
                .collect::<Vec<_>>()
                .join(", ");
            let binds: Vec<ParamValue> = vals.iter().map(|v| v.clone().into()).collect();
            params.add(&format!("{column} IN ({placeholders})"), &binds);
        }
        other => {
            let (op, val) = match other {
                Cmp::Eq(v) => ("=", v),
                Cmp::Lt(v) => ("<", v),
                Cmp::Gt(v) => (">", v),
                Cmp::Lte(v) => ("<=", v),
                Cmp::Gte(v) => (">=", v),
                Cmp::Like(v) => ("LIKE", v),
                _ => panic!("{column} can not be filtered by {other:?}"),
            };
            let ph = dialect.placeholder();
            params.add(&format!("{column} {op} {ph}"), &[val.clone().into()]);
        }
    }
}

fn build_i64_column(
    column: &str,
    cmp: &Cmp<i64>,
    params: &mut Params,
    dialect: &mut impl Dialect,
) {
    match cmp {
        Cmp::In(vals) => {
            let placeholders = vals
                .iter()
                .map(|_| dialect.placeholder())
                .collect::<Vec<_>>()
                .join(", ");
            let binds: Vec<ParamValue> = vals.iter().map(|v| ParamValue::SignedNumber(*v)).collect();
            params.add(&format!("{column} IN ({placeholders})"), &binds);
        }
        other => {
            let (op, val) = match other {
                Cmp::Eq(v) => ("=", v),
                Cmp::Lt(v) => ("<", v),
                Cmp::Gt(v) => (">", v),
                Cmp::Lte(v) => ("<=", v),
                Cmp::Gte(v) => (">=", v),
                _ => panic!("{column} can not be filtered by {other:?}"),
            };
            let ph = dialect.placeholder();
            params.add(&format!("{column} {op} {ph}"), &[ParamValue::SignedNumber(*val)]);
        }
    }
}

fn build_and(exprs: &[Expr], params: &mut Params, dialect: &mut impl Dialect) {
    let mut sql = String::new();
    let mut binds = vec![];
    for expr in exprs {
        let mut part = Params::new();
        build_expr(expr, &mut part, true, dialect);
        if !sql.is_empty() {
            sql.push_str(" AND ");
        }
        sql.push_str(&part.0);
        binds.extend(part.1);
    }
    params.add(&sql, &binds);
}

fn build_or(exprs: &[Expr], params: &mut Params, wrap: bool, dialect: &mut impl Dialect) {
    let mut sql = String::new();
    let mut binds = vec![];
    for expr in exprs {
        let mut part = Params::new();
        build_expr(expr, &mut part, false, dialect);
        if !sql.is_empty() {
            sql.push_str(" OR ");
        }
        sql.push_str(&part.0);
        binds.extend(part.1);
    }
    if wrap {
        sql = format!("({sql})");
    }
    params.add(&sql, &binds);
}
