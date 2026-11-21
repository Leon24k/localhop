use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use bytes::Bytes;
use clap::Parser;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use localhop_protocol::{
    is_hop_by_hop_header, is_valid_tunnel_id, Frame, Hello, ProtocolErrorMsg, RequestHead,
    ResponseHead, Welcome,
};
use rand::Rng;
use serde::Serialize;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info, warn};

const OUTBOUND_CHANNEL_CAPACITY: usize = 512;
const BODY_CHANNEL_CAPACITY: usize = 64;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "localhop-server",
    about = "Ultra-low-latency public relay server for localhop"
)]
struct Config {
    /// Interface address to bind to
    #[arg(long, env = "LOCALHOP_BIND", default_value = "0.0.0.0")]
    bind: String,

    /// Port to listen on (Render/Fly/Shuttle set PORT automatically)
    #[arg(short, long, env = "PORT", default_value_t = 8080)]
    port: u16,

    /// Optional public base URL (e.g. https://hop.example.com). Auto-detected if omitted.
    #[arg(long, env = "LOCALHOP_PUBLIC_URL")]
    public_url: Option<String>,

    /// Optional base domain for subdomain routing (e.g. hop.example.com -> <id>.hop.example.com)
    #[arg(long, env = "LOCALHOP_BASE_DOMAIN")]
    base_domain: Option<String>,

    /// Optional shared secret required by CLI clients to open tunnels
    #[arg(long, env = "LOCALHOP_AUTH_TOKEN")]
    auth_token: Option<String>,

    /// Per-request upstream response timeout in seconds
    #[arg(long, env = "LOCALHOP_TIMEOUT_SECS", default_value_t = 60)]
    timeout_secs: u64,
}

struct PendingResponse {
    head_tx: Option<oneshot::Sender<ResponseHead>>,
    body_tx: mpsc::Sender<Result<Bytes, std::io::Error>>,
}

struct TunnelSession {
    outbound_tx: mpsc::Sender<Bytes>,
    pending: DashMap<u64, PendingResponse>,
    next_req_id: AtomicU64,
}

impl TunnelSession {
    fn new(outbound_tx: mpsc::Sender<Bytes>) -> Self {
        Self {
            outbound_tx,
            pending: DashMap::new(),
            next_req_id: AtomicU64::new(1),
        }
    }
}

struct AppState {
    config: Config,
    tunnels: DashMap<String, Arc<TunnelSession>>,
    total_requests: AtomicU64,
    started_at: Instant,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    active_tunnels: usize,
    total_requests: u64,
    uptime_secs: u64,
    subdomain_routing: bool,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "localhop_server=info".into()),
        )
        .init();

    let config = Config::parse();
    let addr: SocketAddr = format!("{}:{}", config.bind, config.port)
        .parse()
        .expect("invalid bind address or port");

    let state = Arc::new(AppState {
        config: config.clone(),
        tunnels: DashMap::new(),
        total_requests: AtomicU64::new(0),
        started_at: Instant::now(),
    });

    let app = Router::new()
        .route("/_localhop/ws", get(ws_handler))
        .route("/_localhop/health", get(health_handler))
        .route("/t/{id}", axum::routing::any(path_tunnel_root))
        .route("/t/{id}/{*rest}", axum::routing::any(path_tunnel_rest))
        .fallback(fallback_handler)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind TCP listener");

    info!("localhop-server listening on http://{addr}");
    if let Some(domain) = &config.base_domain {
        info!("subdomain routing enabled for *.{domain}");
    }
    info!("path routing enabled at /t/<id>/...");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutting down localhop-server");
}

async fn health_handler(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        active_tunnels: state.tunnels.len(),
        total_requests: state.total_requests.load(Ordering::Relaxed),
        uptime_secs: state.started_at.elapsed().as_secs(),
        subdomain_routing: state.config.base_domain.is_some(),
    })
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Response {
    let inferred_base = infer_public_base(&headers, &state.config);
    ws.on_upgrade(move |socket| handle_tunnel_socket(socket, state, inferred_base))
}

fn infer_public_base(headers: &HeaderMap, config: &Config) -> String {
    if let Some(url) = &config.public_url {
        return url.trim_end_matches('/').to_string();
    }
    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get(http::header::HOST))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("127.0.0.1:8080");

    let proto = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_else(|| {
            if host.starts_with("localhost") || host.starts_with("127.0.0.1") {
                "http"
            } else {
                "https"
            }
        });

    format!("{proto}://{host}")
}

fn build_public_url(tunnel_id: &str, inferred_base: &str, config: &Config) -> String {
    if let Some(base_domain) = &config.base_domain {
        let scheme = if base_domain.contains("localhost") || base_domain.starts_with("127.") {
            "http"
        } else {
            "https"
        };
        format!("{scheme}://{tunnel_id}.{base_domain}")
    } else {
        format!("{inferred_base}/t/{tunnel_id}")
    }
}

fn generate_tunnel_id() -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..8)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect()
}

async fn handle_tunnel_socket(socket: WebSocket, state: Arc<AppState>, inferred_base: String) {
    let (mut ws_tx, mut ws_rx) = socket.split();

    // 1. Expect initial Hello frame within 10 seconds
    let hello = match tokio::time::timeout(Duration::from_secs(10), ws_rx.next()).await {
        Ok(Some(Ok(Message::Binary(raw)))) => match Frame::decode(raw) {
            Ok(Frame::Hello(h)) => h,
            Ok(_) => {
                let _ = send_error_frame(&mut ws_tx, "expected Hello frame first").await;
                return;
            }
            Err(e) => {
                let _ = send_error_frame(&mut ws_tx, &format!("invalid frame: {e}")).await;
                return;
            }
        },
        _ => return,
    };

    // 2. Authenticate if server requires a token
    if let Some(expected) = &state.config.auth_token {
        if hello.token.as_deref() != Some(expected.as_str()) {
            let _ = send_error_frame(&mut ws_tx, "unauthorized: invalid or missing auth token").await;
            return;
        }
    }

    // 3. Allocate tunnel ID & register atomically
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Bytes>(OUTBOUND_CHANNEL_CAPACITY);
    let session = Arc::new(TunnelSession::new(outbound_tx.clone()));

    let tunnel_id = match register_tunnel(&state, hello, session.clone()) {
        Ok(id) => id,
        Err(msg) => {
            let _ = send_error_frame(&mut ws_tx, &msg).await;
            return;
        }
    };

    let public_url = build_public_url(&tunnel_id, &inferred_base, &state.config);
    info!(tunnel_id = %tunnel_id, public_url = %public_url, "tunnel connected");

    let welcome = Frame::Welcome(Welcome {
        tunnel_id: tunnel_id.clone(),
        public_url,
    });
    if let Ok(bytes) = welcome.encode() {
        if ws_tx.send(Message::Binary(bytes)).await.is_err() {
            state.tunnels.remove(&tunnel_id);
            return;
        }
    }

    // 4. Dedicated writer task so reads never stall on writes
    let writer_handle = tokio::spawn(async move {
        while let Some(frame_bytes) = outbound_rx.recv().await {
            if ws_tx.send(Message::Binary(frame_bytes)).await.is_err() {
                break;
            }
        }
    });

    // 5. Reader loop: dispatch multiplexed responses & ping/pong with zero extra locking
    while let Some(Ok(msg)) = ws_rx.next().await {
        match msg {
            Message::Binary(raw) => match Frame::decode(raw) {
                Ok(Frame::ResponseHead(head)) => {
                    if let Some(mut entry) = session.pending.get_mut(&head.req_id) {
                        if let Some(tx) = entry.head_tx.take() {
                            let _ = tx.send(head);
                        }
                    }
                }
                Ok(Frame::ResponseBody { req_id, chunk }) => {
                    let body_tx = session.pending.get(&req_id).map(|e| e.body_tx.clone());
                    if let Some(tx) = body_tx {
                        if tx.send(Ok(chunk)).await.is_err() {
                            session.pending.remove(&req_id);
                        }
                    }
                }
                Ok(Frame::ResponseEnd { req_id }) => {
                    // Dropping PendingResponse closes body_tx and finishes the HTTP response stream
                    session.pending.remove(&req_id);
                }
                Ok(Frame::Ping { ts_us }) => {
                    if let Ok(pong) = (Frame::Pong { ts_us }).encode() {
                        let _ = outbound_tx.try_send(pong);
                    }
                }
                Ok(other) => {
                    debug!(?other, "ignoring unexpected client frame");
                }
                Err(e) => {
                    warn!(tunnel_id = %tunnel_id, error = %e, "frame decode error");
                }
            },
            Message::Close(_) => break,
            _ => {}
        }
    }

    // 6. Clean up tunnel & fail any in-flight requests cleanly
    state.tunnels.remove(&tunnel_id);
    session.pending.clear();
    writer_handle.abort();
    info!(tunnel_id = %tunnel_id, "tunnel disconnected");
}

fn register_tunnel(
    state: &AppState,
    hello: Hello,
    session: Arc<TunnelSession>,
) -> Result<String, String> {
    if let Some(req_id) = hello.requested_id {
        let clean = req_id.to_ascii_lowercase();
        if !is_valid_tunnel_id(&clean) {
            return Err(
                "invalid tunnel id: use 3-32 lowercase letters, digits, or hyphens".to_string(),
            );
        }
        use dashmap::mapref::entry::Entry;
        match state.tunnels.entry(clean.clone()) {
            Entry::Vacant(v) => {
                v.insert(session);
                Ok(clean)
            }
            Entry::Occupied(_) => Err(format!("tunnel id '{clean}' is already in use")),
        }
    } else {
        for _ in 0..16 {
            let candidate = generate_tunnel_id();
            use dashmap::mapref::entry::Entry;
            if let Entry::Vacant(v) = state.tunnels.entry(candidate.clone()) {
                v.insert(session);
                return Ok(candidate);
            }
        }
        Err("failed to allocate tunnel id".to_string())
    }
}

async fn send_error_frame(
    ws_tx: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    message: &str,
) -> Result<(), axum::Error> {
    if let Ok(bytes) = Frame::Error(ProtocolErrorMsg {
        message: message.to_string(),
    })
    .encode()
    {
        ws_tx.send(Message::Binary(bytes)).await?;
    }
    Ok(())
}

async fn path_tunnel_root(
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
) -> Response {
    let query = req.uri().query().map(|q| format!("?{q}")).unwrap_or_default();
    let target_path = format!("/{query}");
    proxy_to_tunnel(state, &id, target_path, req).await
}

async fn path_tunnel_rest(
    Path((id, rest)): Path<(String, String)>,
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
) -> Response {
    let query = req.uri().query().map(|q| format!("?{q}")).unwrap_or_default();
    let target_path = format!("/{rest}{query}");
    proxy_to_tunnel(state, &id, target_path, req).await
}

async fn fallback_handler(State(state): State<Arc<AppState>>, req: Request<Body>) -> Response {
    // Check subdomain routing if base_domain is configured
    if let Some(base_domain) = &state.config.base_domain {
        let subdomain = req
            .headers()
            .get(http::header::HOST)
            .and_then(|h| h.to_str().ok())
            .and_then(|host| {
                let host_no_port = host.split(':').next().unwrap_or(host);
                let suffix = format!(".{base_domain}");
                host_no_port
                    .strip_suffix(&suffix)
                    .filter(|sub| !sub.is_empty() && !sub.contains('.'))
                    .map(str::to_string)
            });

        if let Some(sub) = subdomain {
            let path_and_query = req
                .uri()
                .path_and_query()
                .map(|pq| pq.as_str().to_string())
                .unwrap_or_else(|| "/".to_string());
            return proxy_to_tunnel(state, &sub, path_and_query, req).await;
        }
    }

    if req.uri().path() == "/" {
        let active = state.tunnels.len();
        let total = state.total_requests.load(Ordering::Relaxed);
        return Html(format!(
            r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>localhop relay</title>
  <style>
    body {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; background: #0b0d12; color: #e6edf3; max-width: 680px; margin: 72px auto; padding: 0 24px; line-height: 1.6; }}
    .badge {{ display: inline-block; padding: 2px 10px; border-radius: 999px; background: #122b1e; color: #3fb950; font-size: 13px; }}
    pre {{ background: #141821; border: 1px solid #262c3a; padding: 14px 16px; border-radius: 8px; overflow-x: auto; }}
    .muted {{ color: #8b949e; }}
  </style>
</head>
<body>
  <span class="badge">● localhop relay online</span>
  <h1>localhop</h1>
  <p class="muted">Zero-GC, low-latency Rust localhost tunnel with built-in HTTP &amp; webhook inspector.</p>
  <p>Active tunnels: <strong>{active}</strong> &middot; Proxied requests: <strong>{total}</strong></p>
  <pre>localhop 3000 --server &lt;this-server-url&gt;</pre>
  <p class="muted">Health JSON: <a href="/_localhop/health" style="color:#58a6ff">/_localhop/health</a></p>
</body>
</html>"#
        ))
        .into_response();
    }

    (StatusCode::NOT_FOUND, "localhop: tunnel or route not found").into_response()
}

async fn proxy_to_tunnel(
    state: Arc<AppState>,
    tunnel_id: &str,
    path_and_query: String,
    req: Request<Body>,
) -> Response {
    let started = Instant::now();
    let Some(session) = state.tunnels.get(tunnel_id).map(|e| e.value().clone()) else {
        return (
            StatusCode::BAD_GATEWAY,
            format!("localhop: tunnel '{tunnel_id}' is not connected"),
        )
            .into_response();
    };

    state.total_requests.fetch_add(1, Ordering::Relaxed);
    let req_id = session.next_req_id.fetch_add(1, Ordering::Relaxed);

    let (parts, mut body) = req.into_parts();
    let headers: Vec<(String, String)> = parts
        .headers
        .iter()
        .filter(|(name, _)| !is_hop_by_hop_header(name.as_str()) && name.as_str() != "host")
        .filter_map(|(name, val)| {
            val.to_str()
                .ok()
                .map(|v| (name.as_str().to_string(), v.to_string()))
        })
        .collect();

    let (head_tx, head_rx) = oneshot::channel();
    let (body_tx, body_rx) = mpsc::channel(BODY_CHANNEL_CAPACITY);

    session.pending.insert(
        req_id,
        PendingResponse {
            head_tx: Some(head_tx),
            body_tx,
        },
    );

    // Send RequestHead first
    let head_frame = Frame::RequestHead(RequestHead {
        req_id,
        method: parts.method.as_str().to_string(),
        path_and_query,
        headers,
    });
    let Ok(encoded_head) = head_frame.encode() else {
        session.pending.remove(&req_id);
        return (StatusCode::INTERNAL_SERVER_ERROR, "failed to encode request").into_response();
    };
    if session.outbound_tx.send(encoded_head).await.is_err() {
        session.pending.remove(&req_id);
        return (StatusCode::BAD_GATEWAY, "tunnel disconnected").into_response();
    }

    // Stream request body chunks concurrently so large bodies never deadlock
    let outbound_tx = session.outbound_tx.clone();
    tokio::spawn(async move {
        while let Some(frame_res) = body.frame().await {
            match frame_res {
                Ok(frame) => {
                    if let Ok(data) = frame.into_data() {
                        if !data.is_empty() {
                            let msg = Frame::RequestBody {
                                req_id,
                                chunk: data,
                            }
                            .encode();
                            if let Ok(bytes) = msg {
                                if outbound_tx.send(bytes).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                }
                Err(_) => break,
            }
        }
        if let Ok(end) = (Frame::RequestEnd { req_id }).encode() {
            let _ = outbound_tx.send(end).await;
        }
    });

    // Wait for ResponseHead from the local agent
    let timeout = Duration::from_secs(state.config.timeout_secs);
    let resp_head = match tokio::time::timeout(timeout, head_rx).await {
        Ok(Ok(head)) => head,
        Ok(Err(_)) => {
            session.pending.remove(&req_id);
            return (StatusCode::BAD_GATEWAY, "tunnel closed before response").into_response();
        }
        Err(_) => {
            session.pending.remove(&req_id);
            return (StatusCode::GATEWAY_TIMEOUT, "localhop: upstream timed out").into_response();
        }
    };

    let status = StatusCode::from_u16(resp_head.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut builder = Response::builder().status(status);

    if let Some(hdrs) = builder.headers_mut() {
        for (name, val) in resp_head.headers {
            if is_hop_by_hop_header(&name) {
                continue;
            }
            if let (Ok(hn), Ok(hv)) = (
                HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_str(&val),
            ) {
                hdrs.append(hn, hv);
            }
        }
        let total_us = started.elapsed().as_micros() as u64;
        if let Ok(v) = HeaderValue::from_str(&resp_head.local_latency_us.to_string()) {
            hdrs.insert(HeaderName::from_static("x-localhop-local-us"), v);
        }
        if let Ok(v) = HeaderValue::from_str(&total_us.to_string()) {
            hdrs.insert(HeaderName::from_static("x-localhop-total-us"), v);
        }
    }

    let stream = ReceiverStream::new(body_rx);
    builder
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response())
}
