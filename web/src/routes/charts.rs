use crate::{
    AppState,
    routes::{HtmlResult, render},
};
use axum::extract::State;

pub async fn handler(State(state): State<AppState>) -> HtmlResult {
    render(&state, "charts.html", "charts")
}
