use axum::{
    Json, Router,
    body::Body,
    extract::{
        ConnectInfo, FromRequest, Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
use clap::Parser;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use localhop_protocol::{
    Frame, Hello, ProtocolErrorMsg, RequestHead, ResponseHead, Welcome, WsOpen, WsOpened,
    constant_time_eq, format_basic_auth_header, is_hop_by_hop_header, is_valid_tunnel_id,
};
use rand::Rng;
use serde::Serialize;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info, warn};

const OUTBOUND_CHANNEL_CAPACITY: usize = 512;
const BODY_CHANNEL_CAPACITY: usize = 64;
const WS_CHANNEL_CAPACITY: usize = 128;
const RELAY_LANDING_HTML: &str = include_str!("landing.html");

#[derive(Parser, Debug, Clone)]
#[command(
    name = "localhop-server",
    version,
    about = "Ultra-low-latency public relay server for localhop"
)]
pub struct Config {
    /// Interface address to bind to
    #[arg(long, env = "LOCALHOP_BIND", default_value = "0.0.0.0")]
    pub bind: String,

    /// Port to listen on (Render/Fly/Shuttle set PORT automatically)
    #[arg(short, long, env = "PORT", default_value_t = 8080)]
    pub port: u16,

    /// Optional public base URL (e.g. https://hop.example.com). Auto-detected if omitted.
    #[arg(long, env = "LOCALHOP_PUBLIC_URL")]
    pub public_url: Option<String>,

    /// Optional base domain for subdomain routing (e.g. hop.example.com -> <id>.hop.example.com)
    #[arg(long, env = "LOCALHOP_BASE_DOMAIN")]
    pub base_domain: Option<String>,

    /// Optional shared secret required by CLI clients to open tunnels
    #[arg(long, env = "LOCALHOP_AUTH_TOKEN")]
    pub auth_token: Option<String>,

    /// Per-request upstream response timeout in seconds
    #[arg(long, env = "LOCALHOP_TIMEOUT_SECS", default_value_t = 60)]
    pub timeout_secs: u64,

    /// Maximum simultaneous active tunnels allowed on this relay
    #[arg(long, env = "LOCALHOP_MAX_TUNNELS", default_value_t = 1000)]
    pub max_tunnels: usize,

    /// Maximum allowed HTTP request body size in bytes (default: 50 MiB)
    #[arg(long, env = "LOCALHOP_MAX_BODY_BYTES", default_value_t = 52_428_800)]
    pub max_body_bytes: u64,

    /// Disconnect idle/zombie tunnels if no frame or ping arrives within this many seconds
    #[arg(long, env = "LOCALHOP_IDLE_TIMEOUT_SECS", default_value_t = 45)]
    pub idle_timeout_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 8080,
            public_url: None,
            base_domain: None,
            auth_token: None,
            timeout_secs: 60,
            max_tunnels: 1000,
            max_body_bytes: 52_428_800,
            idle_timeout_secs: 45,
        }
    }
}

struct PendingResponse {
    head_tx: Option<oneshot::Sender<ResponseHead>>,
    body_tx: mpsc::Sender<Result<Bytes, std::io::Error>>,
}

struct PendingWs {
    opened_tx: Option<oneshot::Sender<WsOpened>>,
    msg_tx: mpsc::Sender<(bool, Bytes)>,
}

struct TunnelSession {
    outbound_tx: mpsc::Sender<Bytes>,
    pending_http: DashMap<u64, PendingResponse>,
    pending_ws: DashMap<u64, PendingWs>,
    next_req_id: AtomicU64,
    /// Pre-encoded `Basic <base64>` header if the tunnel owner enabled `--basic-auth`.
    expected_basic_auth: Option<String>,
}

impl TunnelSession {
    fn new(outbound_tx: mpsc::Sender<Bytes>, basic_auth: Option<String>) -> Self {
        Self {
            outbound_tx,
            pending_http: DashMap::new(),
            pending_ws: DashMap::new(),
            next_req_id: AtomicU64::new(1),
            expected_basic_auth: basic_auth.map(|creds| format_basic_auth_header(&creds)),
        }
    }
}

pub struct AppState {
    pub config: Config,
    tunnels: DashMap<String, Arc<TunnelSession>>,
    total_requests: AtomicU64,
    started_at: Instant,
}

impl AppState {
    pub fn active_tunnels(&self) -> usize {
        self.tunnels.len()
    }
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    active_tunnels: usize,
    max_tunnels: usize,
    total_requests: u64,
    uptime_secs: u64,
    subdomain_routing: bool,
    auth_required: bool,
}

pub fn build_router(config: Config) -> (Router, Arc<AppState>) {
    let state = Arc::new(AppState {
        config,
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
        .with_state(state.clone());

    (app, state)
}

async fn health_handler(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        active_tunnels: state.tunnels.len(),
        max_tunnels: state.config.max_tunnels,
        total_requests: state.total_requests.load(Ordering::Relaxed),
        uptime_secs: state.started_at.elapsed().as_secs(),
        subdomain_routing: state.config.base_domain.is_some(),
        auth_required: state.config.auth_token.is_some(),
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

    // 2. Authenticate in constant time if server requires a token
    if let Some(expected) = &state.config.auth_token {
        let ok = hello
            .token
            .as_deref()
            .is_some_and(|provided| constant_time_eq(provided.as_bytes(), expected.as_bytes()));
        if !ok {
            let _ =
                send_error_frame(&mut ws_tx, "unauthorized: invalid or missing auth token").await;
            return;
        }
    }

    // 3. Enforce max tunnel capacity
    if state.tunnels.len() >= state.config.max_tunnels {
        let _ = send_error_frame(
            &mut ws_tx,
            "relay capacity reached: too many active tunnels",
        )
        .await;
        return;
    }

    // 4. Validate optional Basic Auth format (`user:pass`)
    if let Some(ref creds) = hello.basic_auth
        && !creds.contains(':')
    {
        let _ = send_error_frame(&mut ws_tx, "invalid basic_auth format: expected user:pass").await;
        return;
    }

    // 5. Allocate tunnel ID & register atomically
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Bytes>(OUTBOUND_CHANNEL_CAPACITY);
    let session = Arc::new(TunnelSession::new(
        outbound_tx.clone(),
        hello.basic_auth.clone(),
    ));

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
    if let Ok(bytes) = welcome.encode()
        && ws_tx.send(Message::Binary(bytes)).await.is_err()
    {
        state.tunnels.remove(&tunnel_id);
        return;
    }

    // 6. Dedicated writer task so reads never stall on writes
    let writer_handle = tokio::spawn(async move {
        while let Some(frame_bytes) = outbound_rx.recv().await {
            if ws_tx.send(Message::Binary(frame_bytes)).await.is_err() {
                break;
            }
        }
    });

    // 7. Reader loop with idle timeout to evict dead/zombie clients cleanly
    let idle_timeout = Duration::from_secs(state.config.idle_timeout_secs);
    loop {
        let next_msg = match tokio::time::timeout(idle_timeout, ws_rx.next()).await {
            Ok(Some(Ok(msg))) => msg,
            Ok(Some(Err(e))) => {
                debug!(tunnel_id = %tunnel_id, error = %e, "tunnel read error");
                break;
            }
            Ok(None) => break,
            Err(_) => {
                warn!(tunnel_id = %tunnel_id, "evicting idle/zombie tunnel after {}s", state.config.idle_timeout_secs);
                break;
            }
        };

        match next_msg {
            Message::Binary(raw) => match Frame::decode(raw) {
                Ok(Frame::ResponseHead(head)) => {
                    if let Some(mut entry) = session.pending_http.get_mut(&head.req_id)
                        && let Some(tx) = entry.head_tx.take()
                    {
                        let _ = tx.send(head);
                    }
                }
                Ok(Frame::ResponseBody { req_id, chunk }) => {
                    let body_tx = session.pending_http.get(&req_id).map(|e| e.body_tx.clone());
                    if let Some(tx) = body_tx
                        && tx.send(Ok(chunk)).await.is_err()
                    {
                        session.pending_http.remove(&req_id);
                    }
                }
                Ok(Frame::ResponseEnd { req_id }) => {
                    session.pending_http.remove(&req_id);
                }
                Ok(Frame::WsOpened(opened)) => {
                    if let Some(mut entry) = session.pending_ws.get_mut(&opened.req_id)
                        && let Some(tx) = entry.opened_tx.take()
                    {
                        let _ = tx.send(opened);
                    }
                }
                Ok(Frame::WsMessage {
                    req_id,
                    is_binary,
                    payload,
                }) => {
                    let msg_tx = session.pending_ws.get(&req_id).map(|e| e.msg_tx.clone());
                    if let Some(tx) = msg_tx
                        && tx.send((is_binary, payload)).await.is_err()
                    {
                        session.pending_ws.remove(&req_id);
                    }
                }
                Ok(Frame::WsClose { req_id }) => {
                    session.pending_ws.remove(&req_id);
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

    // 8. Clean up tunnel & unblock all in-flight HTTP/WS streams
    state.tunnels.remove(&tunnel_id);
    session.pending_http.clear();
    session.pending_ws.clear();
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
    let query = req
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let target_path = format!("/{query}");
    proxy_to_tunnel(state, &id, target_path, req).await
}

async fn path_tunnel_rest(
    Path((id, rest)): Path<(String, String)>,
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
) -> Response {
    let query = req
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let target_path = format!("/{rest}{query}");
    proxy_to_tunnel(state, &id, target_path, req).await
}

async fn fallback_handler(State(state): State<Arc<AppState>>, req: Request<Body>) -> Response {
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
        let active = state.tunnels.len().to_string();
        let total = state.total_requests.load(Ordering::Relaxed).to_string();
        let html = RELAY_LANDING_HTML
            .replace("__ACTIVE_TUNNELS__", &active)
            .replace("__TOTAL_REQUESTS__", &total)
            .replace("__VERSION__", env!("CARGO_PKG_VERSION"));
        return Html(html).into_response();
    }

    (StatusCode::NOT_FOUND, "localhop: tunnel or route not found").into_response()
}

fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    let has_upgrade = headers
        .get(http::header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
    let has_connection_upgrade = headers
        .get(http::header::CONNECTION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',')
                .any(|part| part.trim().eq_ignore_ascii_case("upgrade"))
        });
    has_upgrade && has_connection_upgrade
}

fn check_basic_auth(session: &TunnelSession, headers: &HeaderMap) -> bool {
    let Some(expected) = &session.expected_basic_auth else {
        return true;
    };
    headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|provided| constant_time_eq(provided.as_bytes(), expected.as_bytes()))
}

fn unauthorized_basic_response() -> Response {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(
            http::header::WWW_AUTHENTICATE,
            "Basic realm=\"localhop\", charset=\"UTF-8\"",
        )
        .body(Body::from("localhop: authentication required"))
        .unwrap_or_else(|_| StatusCode::UNAUTHORIZED.into_response())
}

fn collect_forwarded_headers(
    headers: &HeaderMap,
    peer_ip: Option<String>,
    tunnel_id: &str,
) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = headers
        .iter()
        .filter(|(name, _)| !is_hop_by_hop_header(name.as_str()))
        .filter_map(|(name, val)| {
            val.to_str()
                .ok()
                .map(|v| (name.as_str().to_string(), v.to_string()))
        })
        .collect();

    let has_proto = headers.contains_key("x-forwarded-proto");
    let has_host = headers.contains_key("x-forwarded-host");

    if !has_host
        && let Some(host) = headers
            .get(http::header::HOST)
            .and_then(|v| v.to_str().ok())
    {
        out.push(("x-forwarded-host".to_string(), host.to_string()));
    }
    if !has_proto {
        let host = headers
            .get(http::header::HOST)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let proto = if host.starts_with("localhost") || host.starts_with("127.0.0.1") {
            "http"
        } else {
            "https"
        };
        out.push(("x-forwarded-proto".to_string(), proto.to_string()));
    }
    if let Some(ip) = peer_ip {
        let existing = headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .map(|v| format!("{v}, {ip}"))
            .unwrap_or(ip);
        out.retain(|(k, _)| !k.eq_ignore_ascii_case("x-forwarded-for"));
        out.push(("x-forwarded-for".to_string(), existing));
    }
    out.push(("x-localhop-tunnel-id".to_string(), tunnel_id.to_string()));
    out
}

async fn proxy_to_tunnel(
    state: Arc<AppState>,
    tunnel_id: &str,
    path_and_query: String,
    req: Request<Body>,
) -> Response {
    let Some(session) = state.tunnels.get(tunnel_id).map(|e| e.value().clone()) else {
        return (
            StatusCode::BAD_GATEWAY,
            format!("localhop: tunnel '{tunnel_id}' is not connected"),
        )
            .into_response();
    };

    if !check_basic_auth(&session, req.headers()) {
        return unauthorized_basic_response();
    }

    // Handle WebSocket upgrade passthrough (Vite HMR, Next.js Fast Refresh, Socket.IO, etc.)
    if is_websocket_upgrade(req.headers()) {
        return proxy_ws_to_tunnel(state, session, tunnel_id, path_and_query, req).await;
    }

    // Enforce declared Content-Length limit before allocating stream work
    if let Some(cl) = req
        .headers()
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        && cl > state.config.max_body_bytes
    {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "localhop: request body exceeds limit of {} bytes",
                state.config.max_body_bytes
            ),
        )
            .into_response();
    }

    let started = Instant::now();
    state.total_requests.fetch_add(1, Ordering::Relaxed);
    let req_id = session.next_req_id.fetch_add(1, Ordering::Relaxed);

    let peer_ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip().to_string());

    let (parts, mut body) = req.into_parts();
    let headers = collect_forwarded_headers(&parts.headers, peer_ip, tunnel_id);

    let (head_tx, head_rx) = oneshot::channel();
    let (body_tx, body_rx) = mpsc::channel(BODY_CHANNEL_CAPACITY);

    session.pending_http.insert(
        req_id,
        PendingResponse {
            head_tx: Some(head_tx),
            body_tx,
        },
    );

    let head_frame = Frame::RequestHead(RequestHead {
        req_id,
        method: parts.method.as_str().to_string(),
        path_and_query,
        headers,
    });
    let Ok(encoded_head) = head_frame.encode() else {
        session.pending_http.remove(&req_id);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to encode request",
        )
            .into_response();
    };
    if session.outbound_tx.send(encoded_head).await.is_err() {
        session.pending_http.remove(&req_id);
        return (StatusCode::BAD_GATEWAY, "tunnel disconnected").into_response();
    }

    // Stream request body chunks concurrently with strict max_body_bytes enforcement
    let outbound_tx = session.outbound_tx.clone();
    let max_body = state.config.max_body_bytes;
    tokio::spawn(async move {
        let mut streamed: u64 = 0;
        while let Some(frame_res) = body.frame().await {
            match frame_res {
                Ok(frame) => {
                    if let Ok(data) = frame.into_data()
                        && !data.is_empty()
                    {
                        streamed = streamed.saturating_add(data.len() as u64);
                        if streamed > max_body {
                            break;
                        }
                        if let Ok(bytes) = (Frame::RequestBody {
                            req_id,
                            chunk: data,
                        })
                        .encode()
                            && outbound_tx.send(bytes).await.is_err()
                        {
                            return;
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
            session.pending_http.remove(&req_id);
            return (StatusCode::BAD_GATEWAY, "tunnel closed before response").into_response();
        }
        Err(_) => {
            session.pending_http.remove(&req_id);
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
    builder.body(Body::from_stream(stream)).unwrap_or_else(|_| {
        (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
    })
}

async fn proxy_ws_to_tunnel(
    state: Arc<AppState>,
    session: Arc<TunnelSession>,
    tunnel_id: &str,
    path_and_query: String,
    req: Request<Body>,
) -> Response {
    let peer_ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip().to_string());
    let headers = collect_forwarded_headers(req.headers(), peer_ip, tunnel_id);

    let ws_upgrade = match WebSocketUpgrade::from_request(req, &state).await {
        Ok(ws) => ws,
        Err(rejection) => return rejection.into_response(),
    };

    state.total_requests.fetch_add(1, Ordering::Relaxed);
    let req_id = session.next_req_id.fetch_add(1, Ordering::Relaxed);

    let (opened_tx, opened_rx) = oneshot::channel();
    let (msg_tx, mut msg_rx) = mpsc::channel::<(bool, Bytes)>(WS_CHANNEL_CAPACITY);

    session.pending_ws.insert(
        req_id,
        PendingWs {
            opened_tx: Some(opened_tx),
            msg_tx,
        },
    );

    let open_frame = Frame::WsOpen(WsOpen {
        req_id,
        path_and_query,
        headers,
    });
    let Ok(encoded) = open_frame.encode() else {
        session.pending_ws.remove(&req_id);
        return (StatusCode::INTERNAL_SERVER_ERROR, "ws encode error").into_response();
    };
    if session.outbound_tx.send(encoded).await.is_err() {
        session.pending_ws.remove(&req_id);
        return (StatusCode::BAD_GATEWAY, "tunnel disconnected").into_response();
    }

    // Confirm local upstream accepted the WebSocket before upgrading the public client
    match tokio::time::timeout(Duration::from_secs(10), opened_rx).await {
        Ok(Ok(opened)) if opened.accepted => {}
        Ok(Ok(opened)) => {
            session.pending_ws.remove(&req_id);
            let msg = opened
                .error
                .unwrap_or_else(|| "local upstream rejected websocket upgrade".to_string());
            return (StatusCode::BAD_GATEWAY, msg).into_response();
        }
        _ => {
            session.pending_ws.remove(&req_id);
            return (
                StatusCode::GATEWAY_TIMEOUT,
                "localhop: websocket upstream timed out",
            )
                .into_response();
        }
    }

    ws_upgrade.on_upgrade(move |socket| async move {
        let (mut pub_tx, mut pub_rx) = socket.split();

        let to_public = tokio::spawn(async move {
            while let Some((is_binary, payload)) = msg_rx.recv().await {
                let msg = if is_binary {
                    Message::Binary(payload)
                } else {
                    match String::from_utf8(payload.to_vec()) {
                        Ok(text) => Message::Text(text.into()),
                        Err(e) => Message::Binary(Bytes::from(e.into_bytes())),
                    }
                };
                if pub_tx.send(msg).await.is_err() {
                    break;
                }
            }
            let _ = pub_tx.send(Message::Close(None)).await;
        });

        while let Some(Ok(msg)) = pub_rx.next().await {
            match msg {
                Message::Text(text) => {
                    if let Ok(bytes) = (Frame::WsMessage {
                        req_id,
                        is_binary: false,
                        payload: Bytes::copy_from_slice(text.as_bytes()),
                    })
                    .encode()
                        && session.outbound_tx.send(bytes).await.is_err()
                    {
                        break;
                    }
                }
                Message::Binary(data) => {
                    if let Ok(bytes) = (Frame::WsMessage {
                        req_id,
                        is_binary: true,
                        payload: data,
                    })
                    .encode()
                        && session.outbound_tx.send(bytes).await.is_err()
                    {
                        break;
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }

        if let Ok(close) = (Frame::WsClose { req_id }).encode() {
            let _ = session.outbound_tx.send(close).await;
        }
        session.pending_ws.remove(&req_id);
        to_public.abort();
    })
}
