pub mod alert;
mod config;
pub mod file_watcher;
mod state;

use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(default)]
pub(crate) struct Settings {
    poll_interval_secs: u64,
    alert_interval_secs: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self { poll_interval_secs: 3, alert_interval_secs: 5 }
    }
}
