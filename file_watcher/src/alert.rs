#![allow(dead_code)]
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use event_storage::EventStorage;
use serde::Deserialize;
use shared::event_filter::Filter as EventFilter;

pub struct Alert {
    name: String,
    description: String,
    filter: EventFilter,
    trigger: AlertTrigger,
    has_fired: bool,
    status: AlertStatus,
    handler: AlertHandler,
    last_fired: Instant,
}

impl Alert {
    pub fn handle(&self) {
        self.handler.handle();
    }
}

pub enum AlertTrigger {
    /// Fires if quantity are logged within the last duration.
    Velocity { duration: Duration, quantity: usize },
    /// Fires the first time the filter matches
    Once,
}

pub enum AlertHandler {
    Email(EmailHandler),
    Log(LogHandler),
}

impl AlertHandler {
    pub fn handle(&self) {}
}

enum AlertStatus {
    Active,
    Disabled,
    Paused(u64),
}

pub struct AlertsContainer {
    alerts: Vec<Alert>,
}

impl AlertsContainer {
    pub fn from_config(config: &Config) -> Self {
        Self { alerts: vec![] }
    }

    pub async fn get_fired(&self, store: Arc<EventStorage>) -> anyhow::Result<Vec<&Alert>> {
        let mut fired = vec![];
        for alert in self.alerts.iter() {
            let found = store.load(&alert.filter).await?;
        }
        Ok(fired)
    }
}

#[derive(Deserialize, Debug)]
pub struct Config {
    alerts: Vec<AlertConfig>,
}

#[derive(Deserialize, Debug)]
pub struct AlertConfig {}

// ==============
// EMAIL HANDLER
// ==============
pub struct EmailHandler {
    address: String,
    subject: String,
    content: String,
}

impl EmailHandler {
    fn handle(&self) {}
}

// ===========
// LOG HANDLER
// ===========
pub struct LogHandler {
    prefix: String,
    path: PathBuf,
}

impl LogHandler {
    fn handle(&self) {}
}
