#![allow(dead_code)]
use crate::event_filter::Filter;

pub struct Alert {
    name: String,
    description: Option<String>,
    filter: Filter,
    trigger: AlertTrigger,
    handler: AlertHandler,
}

impl Alert {
    pub fn handle(&self) {
        self.handler.handle();
    }
}

pub enum AlertTrigger {
    /// Fires if quantity are logged within the last duration.
    Velocity { duration: chrono::Duration, quantity: usize },
}

pub enum AlertHandler {
    Email(EmailHandler),
}

impl AlertHandler {
    pub fn handle(&self) {}
}

// ==============
// EMAIL HANDLER
// ==============
pub struct EmailHandler {
    address: String,
    subject: String,
    content: String,
}
