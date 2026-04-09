use axum::extract::State;
use crate::{AppState, routes::{HtmlResult, render}};

pub async fn handler(State(state): State<AppState>) -> HtmlResult {
    render(&state, "charts.html", "charts")
}
