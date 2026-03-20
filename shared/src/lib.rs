use chrono::{NaiveDate, NaiveDateTime, NaiveTime, ParseError};

pub mod env;
pub mod event;
pub mod tree;

pub fn datetime_from(ts: &str) -> Result<NaiveDateTime, ParseError> {
    if ts.len() == 10 {
        let d = NaiveDate::parse_from_str(ts, "%Y-%m-%d")?;
        Ok(NaiveDateTime::new(
            d,
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        ))
    } else {
        NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S")
    }
}
