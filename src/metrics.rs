use axum::{
    extract::State,
    http::{header::CONTENT_TYPE, Response},
    response::IntoResponse,
    routing::get,
    Router,
};
use prometheus::{Encoder, Registry, TextEncoder};
use tokio::net::TcpListener;
use tracing::debug;

pub async fn run_metrics_task(
    metrics_addr: &str,
    metrics: Registry,
) -> Result<(), Box<dyn std::error::Error>> {
    let metrics_listener = TcpListener::bind(metrics_addr).await?;
    tokio::spawn(async move {
        let router = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(HttpState { metrics });
        axum::serve(metrics_listener, router)
            .await
            .expect("can run metrics server");
    });
    Ok(())
}

#[derive(Clone)]
struct HttpState {
    metrics: Registry,
}

async fn metrics_handler(State(state): State<HttpState>) -> impl IntoResponse {
    let text_encoder = TextEncoder::new();
    let metric_family = state.metrics.gather();

    let encoded_metrics = text_encoder
        .encode_to_string(&metric_family)
        .expect("can encode known metrics");

    debug!(?encoded_metrics);

    Response::builder()
        .header(CONTENT_TYPE, text_encoder.format_type())
        .body(encoded_metrics)
        .expect("valid response type")
}
