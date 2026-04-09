use axum::{
    Router,
    http::StatusCode,
    response::{Html, IntoResponse},
};
use minijinja::context;

use crate::AppState;

pub mod charts;
pub mod events;
pub mod index;
pub mod searches;
pub mod spans;
pub mod tail;

pub fn router(state: AppState) -> Router {
    use axum::routing::get;
    Router::new()
        .route("/", get(index::handler))
        .route("/events", get(events::handler))
        .route("/events/results", get(events::results))
        .route("/events/results/more", get(events::more))
        .route("/events/{id}/detail", get(events::detail))
        .route("/tail", get(tail::handler))
        .route("/spans", get(spans::handler))
        .route("/charts", get(charts::handler))
        .route("/searches", get(searches::handler))
        .with_state(state)
}

pub type HtmlResult = Result<Html<String>, AppError>;

pub fn render(state: &AppState, template: &str, page: &str) -> HtmlResult {
    let tmpl = state.env.get_template(template)?;
    let html = tmpl.render(context! { page })?;
    Ok(Html(html))
}

pub struct AppError(minijinja::Error);

impl From<minijinja::Error> for AppError {
    fn from(e: minijinja::Error) -> Self { AppError(e) }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}
