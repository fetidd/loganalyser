use chrono::{NaiveDate, NaiveDateTime, NaiveTime, ParseError};

pub mod env;
pub mod event;
pub mod event_filter;
pub mod tree;

/// Retries an async expression with exponential backoff.
///
/// # Usage
/// ```ignore
/// async_retry!(some_async_call(&args))
/// async_retry!(some_async_call(&args), attempts = 3, delay_ms = 50)
/// ```
///
/// The expression is re-evaluated on each attempt. It must return a `Result`.
/// Requires `tokio` and `tracing` to be dependencies of the calling crate.
#[macro_export]
macro_rules! async_retry {
    ($expr:expr) => {
        $crate::async_retry!($expr, attempts = 5, delay_ms = 100)
    };
    ($expr:expr, attempts = $attempts:expr, delay_ms = $delay_ms:expr) => {
        async move {
            let max: u32 = $attempts;
            let mut delay = ::std::time::Duration::from_millis($delay_ms);
            for attempt in 1..=max {
                match ($expr).await {
                    ::std::result::Result::Ok(v) => return ::std::result::Result::Ok(v),
                    ::std::result::Result::Err(e) if attempt < max => {
                        ::tracing::warn!("attempt {attempt}/{max} failed: {e:?}, retrying in {delay:?}");
                        ::tokio::time::sleep(delay).await;
                        delay *= 2;
                    }
                    ::std::result::Result::Err(e) => {
                        ::tracing::error!("failed after {max} attempts: {e:?}");
                        return ::std::result::Result::Err(e);
                    }
                }
            }
            ::std::unreachable!()
        }
    };
}

/// Converts a string of a timestamp to a chrono::NaiveDateTime.
/// Either yyyy-mm-dd or yyyy-mm-dd hh:mm:ss.
pub fn datetime_from(ts: &str) -> Result<NaiveDateTime, ParseError> {
    if ts.len() == 10 {
        let d = NaiveDate::parse_from_str(ts, "%Y-%m-%d")?;
        Ok(NaiveDateTime::new(d, NaiveTime::from_hms_opt(0, 0, 0).unwrap()))
    } else {
        NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S")
    }
}

pub enum ExitReason {
    DatabaseFailure,
    Interrupt,
    Unknown,
}
