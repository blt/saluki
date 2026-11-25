//! Internal telemetry API handler.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http::header;
use saluki_api::{
    extract::State,
    response::IntoResponse,
    routing::{get, Router},
    APIHandler, DynamicRoute, EndpointType,
};
use saluki_common::sync::shutdown::ShutdownHandle;
use saluki_error::generic_error;
use tokio::sync::Mutex;

use super::{
    get_shared_metrics_state, AggregatedMetricsProcessor, AggregatedMetricsState, Reflector, TelemetryProcessor,
};
use crate::runtime::{state::DataspaceRegistry, InitializationError, Supervisable, SupervisorFuture};

/// State shared by the telemetry API route.
#[derive(Clone)]
pub struct InternalTelemetryState {
    metrics: Reflector<AggregatedMetricsProcessor>,
    exposition: Arc<Mutex<Exposition>>,
}

#[derive(Default)]
struct Exposition {
    processor: TelemetryProcessor,
    response: Option<(u64, Bytes)>,
}

impl Exposition {
    fn render(&mut self, metrics: &AggregatedMetricsState) -> Bytes {
        // Keep the snapshot stable until the cached response has been checked or rebuilt.
        let generation = metrics.lock_generation();
        if let Some((cached_generation, response)) = &self.response {
            if cached_generation == &*generation {
                return response.clone();
            }
        }

        let response = Bytes::from(self.processor.process(metrics));
        self.response = Some((*generation, response.clone()));
        response
    }
}

/// API handler that exposes `/metrics` on the unprivileged endpoint.
pub struct InternalTelemetryAPIHandler {
    state: InternalTelemetryState,
}

impl InternalTelemetryAPIHandler {
    fn new(metrics: Reflector<AggregatedMetricsProcessor>) -> Self {
        Self {
            state: InternalTelemetryState {
                metrics,
                exposition: Arc::new(Mutex::new(Exposition::default())),
            },
        }
    }

    async fn metrics_handler(State(state): State<InternalTelemetryState>) -> impl IntoResponse {
        let response = state.exposition.lock().await.render(state.metrics.state());
        ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], response)
    }
}

impl APIHandler for InternalTelemetryAPIHandler {
    type State = InternalTelemetryState;

    fn generate_initial_state(&self) -> Self::State {
        self.state.clone()
    }

    fn generate_routes(&self) -> Router<Self::State> {
        Router::new().route("/metrics", get(Self::metrics_handler))
    }
}

/// A worker that exposes the process's internal telemetry on the unprivileged API endpoint.
///
/// Asserts `GET /metrics`, serving the full aggregated metrics state as Prometheus text exposition.
pub struct InternalTelemetryAPIWorker;

#[async_trait]
impl Supervisable for InternalTelemetryAPIWorker {
    fn name(&self) -> &str {
        "internal-telemetry-api"
    }

    async fn initialize(&self, process_shutdown: ShutdownHandle) -> Result<SupervisorFuture, InitializationError> {
        let metrics = get_shared_metrics_state();
        let handler = InternalTelemetryAPIHandler::new(metrics);
        let route = DynamicRoute::http(EndpointType::Unprivileged, &handler);

        Ok(Box::pin(async move {
            DataspaceRegistry::try_current()
                .ok_or_else(|| generic_error!("Dataspace not available."))?
                .assert(route, "internal-telemetry-api");

            process_shutdown.await;
            Ok(())
        }))
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;
    use http::{Request, StatusCode};
    use http_body_util::Empty;
    use tower::ServiceExt as _;

    use super::*;
    use crate::{
        data_model::event::{
            metric::{context::Context, Metric},
            Event,
        },
        observability::metrics::{aggregate_upserts, MetricsSnapshot, Processor},
    };

    fn handler() -> InternalTelemetryAPIHandler {
        let (metrics, _worker) = Reflector::new(stream::empty::<Vec<MetricsSnapshot>>(), AggregatedMetricsProcessor);
        InternalTelemetryAPIHandler::new(metrics)
    }

    async fn status(handler: &InternalTelemetryAPIHandler, path: &str) -> StatusCode {
        let request = Request::get(path).body(Empty::<Bytes>::new()).unwrap();
        handler
            .generate_routes()
            .with_state(handler.generate_initial_state())
            .oneshot(request)
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn serves_only_metrics_route() {
        let handler = handler();
        assert_eq!(status(&handler, "/metrics").await, StatusCode::OK);
        assert_eq!(status(&handler, "/compat/metrics").await, StatusCode::NOT_FOUND);
    }

    #[test]
    fn reuses_exposition_until_a_snapshot_changes_the_gauge() {
        let context = Context::from_static_parts("adp.aggregate_active_contexts", &["component_id:dsd_agg"]);
        let gauge = |value| Event::Metric(Metric::gauge(context.clone(), value));
        let metrics = aggregate_upserts(vec![gauge(1.0)]);
        let mut exposition = Exposition::default();

        let first = exposition.render(&metrics);
        assert_eq!(first.as_ptr(), exposition.render(&metrics).as_ptr());
        assert!(str::from_utf8(&first)
            .unwrap()
            .contains("adp__aggregate_active_contexts{component_id=\"dsd_agg\"} 1\n"));

        AggregatedMetricsProcessor.process(
            MetricsSnapshot {
                upserts: vec![gauge(2.0)],
                ..MetricsSnapshot::default()
            },
            &metrics,
        );
        let next = exposition.render(&metrics);
        assert!(str::from_utf8(&next)
            .unwrap()
            .contains("adp__aggregate_active_contexts{component_id=\"dsd_agg\"} 2\n"));
    }
}
