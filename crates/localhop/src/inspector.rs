use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    convert::Infallible,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{broadcast, RwLock};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::forwarder::LocalForwarder;

const MAX_RECORDS: usize = 200;
pub const MAX_PREVIEW_BYTES: usize = 128 * 1024;
const INSPECTOR_HTML: &str = include_str!("inspector.html");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrafficRecord {
    pub id: u64,
    pub timestamp_ms: u64,
    pub source: String,
    pub method: String,
    pub path_and_query: String,
    pub request_headers: Vec<(String, String)>,
    pub request_body: String,
    pub request_bytes: usize,
    pub status: u16,
    pub response_headers: Vec<(String, String)>,
    pub response_body: String,
    pub response_bytes: usize,
    pub local_latency_us: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TunnelStatus {
    pub connected: bool,
    pub public_url: Option<String>,
    pub target: String,
    pub relay_server: String,
    pub rtt_us: Option<u64>,
    pub total_requests: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InspectorEvent {
    Record { record: TrafficRecord },
    Status { status: TunnelStatus },
    Cleared,
}

pub struct InspectorHub {
    records: RwLock<VecDeque<TrafficRecord>>,
    status: RwLock<TunnelStatus>,
    events_tx: broadcast::Sender<InspectorEvent>,
    next_id: AtomicU64,
    total_requests: AtomicU64,
}

impl InspectorHub {
    pub fn new(target: String, relay_server: String) -> Arc<Self> {
        let (events_tx, _) = broadcast::channel(256);
        Arc::new(Self {
            records: RwLock::new(VecDeque::with_capacity(MAX_RECORDS)),
            status: RwLock::new(TunnelStatus {
                connected: false,
                public_url: None,
                target,
                relay_server,
                rtt_us: None,
                total_requests: 0,
            }),
            events_tx,
            next_id: AtomicU64::new(1),
            total_requests: AtomicU64::new(0),
        })
    }

    pub fn next_record_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn push_record(&self, record: TrafficRecord) {
        let total = self.total_requests.fetch_add(1, Ordering::Relaxed) + 1;
        {
            let mut guard = self.records.write().await;
            if guard.len() >= MAX_RECORDS {
                guard.pop_back();
            }
            guard.push_front(record.clone());
        }
        {
            let mut st = self.status.write().await;
            st.total_requests = total;
        }
        let _ = self.events_tx.send(InspectorEvent::Record { record });
    }

    pub async fn set_connected(&self, connected: bool, public_url: Option<String>) {
        let snapshot = {
            let mut st = self.status.write().await;
            st.connected = connected;
            if public_url.is_some() || !connected {
                st.public_url = public_url;
            }
            if !connected {
                st.rtt_us = None;
            }
            st.clone()
        };
        let _ = self
            .events_tx
            .send(InspectorEvent::Status { status: snapshot });
    }

    pub async fn set_rtt_us(&self, rtt_us: u64) {
        let snapshot = {
            let mut st = self.status.write().await;
            st.rtt_us = Some(rtt_us);
            st.clone()
        };
        let _ = self
            .events_tx
            .send(InspectorEvent::Status { status: snapshot });
    }

    pub async fn clear(&self) {
        self.records.write().await.clear();
        let _ = self.events_tx.send(InspectorEvent::Cleared);
    }
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn format_body_preview(buf: &[u8], total_len: usize) -> String {
    if buf.is_empty() {
        return String::new();
    }
    let suffix = if total_len > buf.len() {
        format!("\n… [truncated, showing {} of {} bytes]", buf.len(), total_len)
    } else {
        String::new()
    };
    match std::str::from_utf8(buf) {
        Ok(s) => format!("{s}{suffix}"),
        Err(_) => format!("<binary payload: {total_len} bytes>"),
    }
}

struct InspectorAppState {
    hub: Arc<InspectorHub>,
    forwarder: Arc<LocalForwarder>,
}

#[derive(Serialize)]
struct SnapshotResponse {
    status: TunnelStatus,
    records: Vec<TrafficRecord>,
}

#[derive(Deserialize)]
struct ReplayRequest {
    method: String,
    path_and_query: String,
    #[serde(default)]
    headers: Vec<(String, String)>,
    #[serde(default)]
    body: String,
}

pub async fn start_inspector_server(
    port: u16,
    hub: Arc<InspectorHub>,
    forwarder: Arc<LocalForwarder>,
) -> Result<SocketAddr, std::io::Error> {
    let state = Arc::new(InspectorAppState { hub, forwarder });

    let app = Router::new()
        .route("/", get(|| async { Html(INSPECTOR_HTML) }))
        .route("/api/state", get(get_state))
        .route("/api/events", get(sse_events))
        .route("/api/replay", post(replay_handler))
        .route("/api/clear", post(clear_handler))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;

    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    Ok(local_addr)
}

async fn get_state(State(state): State<Arc<InspectorAppState>>) -> Json<SnapshotResponse> {
    let status = state.hub.status.read().await.clone();
    let records = state.hub.records.read().await.iter().cloned().collect();
    Json(SnapshotResponse { status, records })
}

async fn sse_events(
    State(state): State<Arc<InspectorAppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.hub.events_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|msg| match msg {
        Ok(event) => serde_json::to_string(&event)
            .ok()
            .map(|data| Ok(Event::default().data(data))),
        Err(_) => None,
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn replay_handler(
    State(state): State<Arc<InspectorAppState>>,
    Json(req): Json<ReplayRequest>,
) -> impl IntoResponse {
    let body_bytes = Bytes::from(req.body.into_bytes());
    let record = state
        .forwarder
        .replay_request(
            state.hub.next_record_id(),
            req.method,
            req.path_and_query,
            req.headers,
            body_bytes,
        )
        .await;

    state.hub.push_record(record.clone()).await;
    (StatusCode::OK, Json(record))
}

async fn clear_handler(State(state): State<Arc<InspectorAppState>>) -> StatusCode {
    state.hub.clear().await;
    StatusCode::NO_CONTENT
}
