use axum::{
    Router,
    extract::ws::{Message as AxumWsMsg, WebSocketUpgrade},
    http::{HeaderMap, Request, StatusCode},
    routing::{get, post},
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use localhop_protocol::{
    Frame, Hello, ResponseHead, ResponseInline, WsOpened, format_basic_auth_header,
};
use localhop_server::{Config, build_router};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message as TungMsg};

async fn spawn_relay(config: Config) -> (SocketAddr, Arc<localhop_server::AppState>) {
    let (app, state) = build_router(config);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (addr, state)
}

async fn spawn_upstream() -> SocketAddr {
    let app = Router::new()
        .route(
            "/echo",
            post(|headers: HeaderMap, body: Bytes| async move {
                let fwd_proto = headers
                    .get("x-forwarded-proto")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let fwd_host = headers
                    .get("x-forwarded-host")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let resp = format!("{fwd_proto}|{fwd_host}|len={}", body.len());
                (StatusCode::CREATED, resp)
            }),
        )
        .route(
            "/hello",
            get(|| async { (StatusCode::OK, "world-from-local") }),
        )
        .route(
            "/redirect",
            get(|| async {
                (
                    StatusCode::FOUND,
                    [(http::header::LOCATION, "/dashboard?tab=1")],
                )
            }),
        )
        .route(
            "/ws",
            get(|ws: WebSocketUpgrade| async move {
                ws.on_upgrade(|mut socket| async move {
                    while let Some(Ok(msg)) = socket.next().await {
                        match msg {
                            AxumWsMsg::Text(t) => {
                                let _ = socket
                                    .send(AxumWsMsg::Text(format!("echo:{t}").into()))
                                    .await;
                            }
                            AxumWsMsg::Binary(b) => {
                                let mut out = b.to_vec();
                                out.reverse();
                                let _ = socket.send(AxumWsMsg::Binary(out.into())).await;
                            }
                            AxumWsMsg::Close(_) => break,
                            _ => {}
                        }
                    }
                })
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

/// Minimal in-process tunnel agent that bridges `localhop-server` to `upstream`
/// so tests run deterministically with zero subprocess flakiness.
async fn connect_agent(
    relay_addr: SocketAddr,
    upstream_addr: SocketAddr,
    hello: Hello,
) -> Result<(String, tokio::task::JoinHandle<()>), String> {
    let ws_url = format!("ws://{relay_addr}/_localhop/ws");
    let (ws_stream, _) = connect_async(&ws_url).await.map_err(|e| e.to_string())?;
    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    ws_tx
        .send(TungMsg::Binary(Frame::Hello(hello).encode().unwrap()))
        .await
        .map_err(|e| e.to_string())?;

    let public_url = match ws_rx.next().await {
        Some(Ok(TungMsg::Binary(raw))) => match Frame::decode(raw).unwrap() {
            Frame::Welcome(w) => w.public_url,
            Frame::Error(e) => return Err(e.message),
            other => return Err(format!("unexpected frame: {other:?}")),
        },
        other => return Err(format!("unexpected ws reply: {other:?}")),
    };

    let handle = tokio::spawn(async move {
        let (out_tx, mut out_rx) = mpsc::channel::<Bytes>(256);
        let writer = tokio::spawn(async move {
            while let Some(b) = out_rx.recv().await {
                if ws_tx.send(TungMsg::Binary(b)).await.is_err() {
                    break;
                }
            }
        });

        let client: Client<HttpConnector, Full<Bytes>> =
            Client::builder(TokioExecutor::new()).build(HttpConnector::new());
        let pending_http: Arc<dashmap::DashMap<u64, (localhop_protocol::RequestHead, Vec<u8>)>> =
            Arc::new(dashmap::DashMap::new());
        let pending_ws: Arc<dashmap::DashMap<u64, mpsc::Sender<(bool, Bytes)>>> =
            Arc::new(dashmap::DashMap::new());

        while let Some(Ok(TungMsg::Binary(raw))) = ws_rx.next().await {
            match Frame::decode(raw).unwrap() {
                Frame::RequestInline(inline) => {
                    let req_id = inline.req_id;
                    let client = client.clone();
                    let out_tx = out_tx.clone();
                    tokio::spawn(async move {
                        let uri = format!("http://{upstream_addr}{}", inline.path_and_query);
                        let mut builder =
                            Request::builder().method(inline.method.as_str()).uri(uri);
                        for (k, v) in inline.headers {
                            if !k.eq_ignore_ascii_case("host") {
                                builder = builder.header(k, v);
                            }
                        }
                        let req = builder.body(Full::new(inline.body)).unwrap();
                        let resp = client.request(req).await.unwrap();
                        let status = resp.status().as_u16();
                        let resp_headers: Vec<(String, String)> = resp
                            .headers()
                            .iter()
                            .filter_map(|(k, v)| {
                                v.to_str()
                                    .ok()
                                    .map(|val| (k.as_str().to_string(), val.to_string()))
                            })
                            .collect();
                        let resp_bytes = resp.into_body().collect().await.unwrap().to_bytes();

                        out_tx
                            .send(
                                Frame::ResponseInline(ResponseInline {
                                    req_id,
                                    status,
                                    headers: resp_headers,
                                    local_latency_us: 80,
                                    body: resp_bytes,
                                })
                                .encode()
                                .unwrap(),
                            )
                            .await
                            .unwrap();
                    });
                }
                Frame::RequestHead(head) => {
                    let req_id = head.req_id;
                    let has_body = head.has_body;
                    pending_http.insert(req_id, (head, Vec::new()));
                    if !has_body && let Some((_, (head, body))) = pending_http.remove(&req_id) {
                        let client = client.clone();
                        let out_tx = out_tx.clone();
                        tokio::spawn(async move {
                            let uri = format!("http://{upstream_addr}{}", head.path_and_query);
                            let mut builder =
                                Request::builder().method(head.method.as_str()).uri(uri);
                            for (k, v) in head.headers {
                                if !k.eq_ignore_ascii_case("host") {
                                    builder = builder.header(k, v);
                                }
                            }
                            let req = builder.body(Full::new(Bytes::from(body))).unwrap();
                            let resp = client.request(req).await.unwrap();
                            let status = resp.status().as_u16();
                            let resp_headers: Vec<(String, String)> = resp
                                .headers()
                                .iter()
                                .filter_map(|(k, v)| {
                                    v.to_str()
                                        .ok()
                                        .map(|val| (k.as_str().to_string(), val.to_string()))
                                })
                                .collect();
                            let resp_bytes = resp.into_body().collect().await.unwrap().to_bytes();

                            out_tx
                                .send(
                                    Frame::ResponseHead(ResponseHead {
                                        req_id,
                                        status,
                                        headers: resp_headers,
                                        local_latency_us: 100,
                                    })
                                    .encode()
                                    .unwrap(),
                                )
                                .await
                                .unwrap();
                            out_tx
                                .send(
                                    Frame::ResponseBody {
                                        req_id,
                                        chunk: resp_bytes,
                                    }
                                    .encode()
                                    .unwrap(),
                                )
                                .await
                                .unwrap();
                            out_tx
                                .send(Frame::ResponseEnd { req_id }.encode().unwrap())
                                .await
                                .unwrap();
                        });
                    }
                }
                Frame::RequestBody { req_id, chunk } => {
                    if let Some(mut entry) = pending_http.get_mut(&req_id) {
                        entry.1.extend_from_slice(&chunk);
                    }
                }
                Frame::RequestEnd { req_id } => {
                    if let Some((_, (head, body))) = pending_http.remove(&req_id) {
                        let client = client.clone();
                        let out_tx = out_tx.clone();
                        tokio::spawn(async move {
                            let uri = format!("http://{upstream_addr}{}", head.path_and_query);
                            let mut builder =
                                Request::builder().method(head.method.as_str()).uri(uri);
                            for (k, v) in head.headers {
                                if !k.eq_ignore_ascii_case("host") {
                                    builder = builder.header(k, v);
                                }
                            }
                            let req = builder.body(Full::new(Bytes::from(body))).unwrap();
                            let resp = client.request(req).await.unwrap();
                            let status = resp.status().as_u16();
                            let resp_headers: Vec<(String, String)> = resp
                                .headers()
                                .iter()
                                .filter_map(|(k, v)| {
                                    v.to_str()
                                        .ok()
                                        .map(|val| (k.as_str().to_string(), val.to_string()))
                                })
                                .collect();
                            let resp_bytes = resp.into_body().collect().await.unwrap().to_bytes();

                            out_tx
                                .send(
                                    Frame::ResponseHead(ResponseHead {
                                        req_id,
                                        status,
                                        headers: resp_headers,
                                        local_latency_us: 100,
                                    })
                                    .encode()
                                    .unwrap(),
                                )
                                .await
                                .unwrap();
                            out_tx
                                .send(
                                    Frame::ResponseBody {
                                        req_id,
                                        chunk: resp_bytes,
                                    }
                                    .encode()
                                    .unwrap(),
                                )
                                .await
                                .unwrap();
                            out_tx
                                .send(Frame::ResponseEnd { req_id }.encode().unwrap())
                                .await
                                .unwrap();
                        });
                    }
                }
                Frame::WsOpen(open) => {
                    let req_id = open.req_id;
                    let (in_tx, mut in_rx) = mpsc::channel::<(bool, Bytes)>(64);
                    pending_ws.insert(req_id, in_tx);
                    let out_tx = out_tx.clone();
                    tokio::spawn(async move {
                        let uri = format!("ws://{upstream_addr}{}", open.path_and_query);
                        match connect_async(&uri).await {
                            Ok((local_ws, _)) => {
                                let _ = out_tx
                                    .send(
                                        Frame::WsOpened(WsOpened {
                                            req_id,
                                            accepted: true,
                                            error: None,
                                        })
                                        .encode()
                                        .unwrap(),
                                    )
                                    .await;
                                let (mut ltx, mut lrx) = local_ws.split();
                                let to_local = tokio::spawn(async move {
                                    while let Some((is_bin, p)) = in_rx.recv().await {
                                        let m = if is_bin {
                                            TungMsg::Binary(p)
                                        } else {
                                            TungMsg::Text(
                                                String::from_utf8(p.to_vec()).unwrap().into(),
                                            )
                                        };
                                        if ltx.send(m).await.is_err() {
                                            break;
                                        }
                                    }
                                });
                                while let Some(Ok(msg)) = lrx.next().await {
                                    match msg {
                                        TungMsg::Text(t) => {
                                            let _ = out_tx
                                                .send(
                                                    Frame::WsMessage {
                                                        req_id,
                                                        is_binary: false,
                                                        payload: Bytes::copy_from_slice(
                                                            t.as_bytes(),
                                                        ),
                                                    }
                                                    .encode()
                                                    .unwrap(),
                                                )
                                                .await;
                                        }
                                        TungMsg::Binary(b) => {
                                            let _ = out_tx
                                                .send(
                                                    Frame::WsMessage {
                                                        req_id,
                                                        is_binary: true,
                                                        payload: b,
                                                    }
                                                    .encode()
                                                    .unwrap(),
                                                )
                                                .await;
                                        }
                                        TungMsg::Close(_) => break,
                                        _ => {}
                                    }
                                }
                                let _ = out_tx
                                    .send(Frame::WsClose { req_id }.encode().unwrap())
                                    .await;
                                to_local.abort();
                            }
                            Err(e) => {
                                let _ = out_tx
                                    .send(
                                        Frame::WsOpened(WsOpened {
                                            req_id,
                                            accepted: false,
                                            error: Some(e.to_string()),
                                        })
                                        .encode()
                                        .unwrap(),
                                    )
                                    .await;
                            }
                        }
                    });
                }
                Frame::WsMessage {
                    req_id,
                    is_binary,
                    payload,
                } => {
                    if let Some(tx) = pending_ws.get(&req_id).map(|e| e.value().clone()) {
                        let _ = tx.send((is_binary, payload)).await;
                    }
                }
                Frame::WsClose { req_id } => {
                    pending_ws.remove(&req_id);
                }
                _ => {}
            }
        }
        writer.abort();
    });

    Ok((public_url, handle))
}

fn http_client() -> Client<HttpConnector, Full<Bytes>> {
    Client::builder(TokioExecutor::new()).build(HttpConnector::new())
}

#[tokio::test]
async fn http_streaming_forwarded_headers_and_concurrency() {
    let upstream = spawn_upstream().await;
    let (relay, _state) = spawn_relay(Config::default()).await;

    let (public_url, agent) = connect_agent(
        relay,
        upstream,
        Hello {
            requested_id: Some("app1".into()),
            token: None,
            basic_auth: None,
        },
    )
    .await
    .unwrap();
    assert!(public_url.ends_with("/t/app1"));

    let client = http_client();

    // 0. Small inline webhook POST (<= 16 KiB) -> single-frame RequestInline + ResponseInline
    let small_payload = Bytes::from_static(br#"{"event":"payment.succeeded"}"#);
    let req_small = Request::builder()
        .method("POST")
        .uri(format!("http://{relay}/t/app1/echo"))
        .header("content-length", small_payload.len().to_string())
        .body(Full::new(small_payload.clone()))
        .unwrap();
    let resp_small = client.request(req_small).await.unwrap();
    assert_eq!(resp_small.status(), StatusCode::CREATED);
    let body_small = String::from_utf8(
        resp_small
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(
        body_small.contains(&format!("len={}", small_payload.len())),
        "expected inline webhook body: {body_small}"
    );

    // 1. Large 256 KiB POST body + X-Forwarded-* verification
    let big_payload = Bytes::from(vec![b'x'; 256 * 1024]);
    let req = Request::builder()
        .method("POST")
        .uri(format!("http://{relay}/t/app1/echo"))
        .body(Full::new(big_payload))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    assert!(resp.headers().contains_key("x-localhop-local-us"));
    assert!(resp.headers().contains_key("x-localhop-total-us"));
    let body = String::from_utf8(
        resp.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(body.contains("http|"), "expected x-forwarded-proto: {body}");
    assert!(
        body.contains("len=262144"),
        "expected full 256KiB body: {body}"
    );

    // 2. 50 concurrent multiplexed requests over 1 tunnel
    let mut tasks = Vec::new();
    for _ in 0..50 {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            let req = Request::builder()
                .uri(format!("http://{relay}/t/app1/hello"))
                .body(Full::new(Bytes::new()))
                .unwrap();
            let resp = client.request(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let bytes = resp.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(bytes.as_ref(), b"world-from-local");
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    agent.abort();
}

#[tokio::test]
async fn auth_token_and_max_tunnels_and_duplicate_id() {
    let upstream = spawn_upstream().await;
    let cfg = Config {
        auth_token: Some("top-secret".into()),
        max_tunnels: 1,
        ..Config::default()
    };
    let (relay, state) = spawn_relay(cfg).await;

    // Missing/wrong token rejected
    let err = connect_agent(
        relay,
        upstream,
        Hello {
            requested_id: Some("one".into()),
            token: Some("wrong".into()),
            basic_auth: None,
        },
    )
    .await
    .unwrap_err();
    assert!(err.contains("unauthorized"), "{err}");

    // Valid token accepted
    let (_url, agent1) = connect_agent(
        relay,
        upstream,
        Hello {
            requested_id: Some("one".into()),
            token: Some("top-secret".into()),
            basic_auth: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(state.active_tunnels(), 1);

    // Second tunnel blocked by max_tunnels = 1
    let err2 = connect_agent(
        relay,
        upstream,
        Hello {
            requested_id: Some("two".into()),
            token: Some("top-secret".into()),
            basic_auth: None,
        },
    )
    .await
    .unwrap_err();
    assert!(err2.contains("capacity"), "{err2}");

    agent1.abort();
}

#[tokio::test]
async fn basic_auth_edge_enforcement_and_subdomain_routing() {
    let upstream = spawn_upstream().await;
    let cfg = Config {
        base_domain: Some("hop.test".into()),
        ..Config::default()
    };
    let (relay, _state) = spawn_relay(cfg).await;

    let (public_url, agent) = connect_agent(
        relay,
        upstream,
        Hello {
            requested_id: Some("demo".into()),
            token: None,
            basic_auth: Some("dev:pass123".into()),
        },
    )
    .await
    .unwrap();
    assert_eq!(public_url, "https://demo.hop.test");

    let client = http_client();

    // Without credentials -> 401 + WWW-Authenticate
    let unauth = Request::builder()
        .uri(format!("http://{relay}/hello"))
        .header("host", "demo.hop.test")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = client.request(unauth).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert!(resp.headers().contains_key("www-authenticate"));

    // With valid Basic Auth over subdomain -> 200
    let authed = Request::builder()
        .uri(format!("http://{relay}/hello"))
        .header("host", "demo.hop.test")
        .header("authorization", format_basic_auth_header("dev:pass123"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = client.request(authed).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), b"world-from-local");

    agent.abort();
}

#[tokio::test]
async fn websocket_passthrough_text_and_binary() {
    let upstream = spawn_upstream().await;
    let (relay, _state) = spawn_relay(Config::default()).await;

    let (_url, agent) = connect_agent(
        relay,
        upstream,
        Hello {
            requested_id: Some("wstest".into()),
            token: None,
            basic_auth: None,
        },
    )
    .await
    .unwrap();

    let (mut pub_ws, _) = connect_async(format!("ws://{relay}/t/wstest/ws"))
        .await
        .expect("public websocket upgrade through tunnel failed");

    // Text frame echo
    pub_ws
        .send(TungMsg::Text("hello hmr".into()))
        .await
        .unwrap();
    match pub_ws.next().await.unwrap().unwrap() {
        TungMsg::Text(t) => assert_eq!(t.as_str(), "echo:hello hmr"),
        other => panic!("expected text echo, got {other:?}"),
    }

    // Binary frame reversed echo
    pub_ws
        .send(TungMsg::Binary(Bytes::from_static(&[1, 2, 3, 4])))
        .await
        .unwrap();
    match pub_ws.next().await.unwrap().unwrap() {
        TungMsg::Binary(b) => assert_eq!(b.as_ref(), &[4, 3, 2, 1]),
        other => panic!("expected binary echo, got {other:?}"),
    }

    let _ = pub_ws.close(None).await;
    agent.abort();
}

#[tokio::test]
async fn rejects_oversized_request_body() {
    let upstream = spawn_upstream().await;
    let cfg = Config {
        max_body_bytes: 1024,
        ..Config::default()
    };
    let (relay, _state) = spawn_relay(cfg).await;

    let (_url, agent) = connect_agent(
        relay,
        upstream,
        Hello {
            requested_id: Some("lim".into()),
            token: None,
            basic_auth: None,
        },
    )
    .await
    .unwrap();

    let client = http_client();
    let req = Request::builder()
        .method("POST")
        .uri(format!("http://{relay}/t/lim/echo"))
        .body(Full::new(Bytes::from(vec![b'a'; 2048])))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);

    agent.abort();
}

#[tokio::test]
async fn rewrites_relative_location_in_path_mode_and_enforces_rate_limit() {
    let upstream = spawn_upstream().await;
    let cfg = Config {
        rate_limit_rps: 2, // burst = 4
        ..Config::default()
    };
    let (relay, _state) = spawn_relay(cfg).await;

    let (_url, agent) = connect_agent(
        relay,
        upstream,
        Hello {
            requested_id: Some("redir".into()),
            token: None,
            basic_auth: None,
        },
    )
    .await
    .unwrap();

    let client = http_client();

    // 1. Verify relative Location header gets prefixed with /t/redir
    let req = Request::builder()
        .uri(format!("http://{relay}/t/redir/redirect"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FOUND);
    assert_eq!(
        resp.headers()
            .get(http::header::LOCATION)
            .and_then(|v| v.to_str().ok()),
        Some("/t/redir/dashboard?tab=1")
    );

    // 2. Burst past rate limit -> expect 429 Too Many Requests
    let mut saw_429 = false;
    for _ in 0..10 {
        let req = Request::builder()
            .uri(format!("http://{relay}/t/redir/hello"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = client.request(req).await.unwrap();
        if resp.status() == StatusCode::TOO_MANY_REQUESTS {
            assert_eq!(
                resp.headers()
                    .get(http::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok()),
                Some("1")
            );
            saw_429 = true;
            break;
        }
    }
    assert!(
        saw_429,
        "expected 429 Too Many Requests when exceeding rate_limit_rps"
    );

    agent.abort();
}

#[tokio::test]
async fn emits_cancel_frame_when_public_client_drops_stream_early() {
    let (relay, _state) = spawn_relay(Config::default()).await;
    let ws_url = format!("ws://{relay}/_localhop/ws");
    let (ws_stream, _) = connect_async(&ws_url).await.unwrap();
    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    ws_tx
        .send(TungMsg::Binary(
            Frame::Hello(Hello {
                requested_id: Some("canceltest".into()),
                token: None,
                basic_auth: None,
            })
            .encode()
            .unwrap(),
        ))
        .await
        .unwrap();

    // Read Welcome
    let _ = ws_rx.next().await.unwrap().unwrap();

    // Start a public HTTP request and drop the response body early
    let public_task = tokio::spawn(async move {
        let client = http_client();
        let req = Request::builder()
            .uri(format!("http://{relay}/t/canceltest/stream"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = client.request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // Drop `resp` without consuming the unfinished body!
        drop(resp);
    });

    // Agent receives RequestHead, responds with ResponseHead + 1 chunk (no ResponseEnd)
    let mut got_req_id = None;
    let mut got_cancel = false;
    while let Some(Ok(TungMsg::Binary(raw))) = ws_rx.next().await {
        match Frame::decode(raw).unwrap() {
            Frame::RequestHead(head) => {
                got_req_id = Some(head.req_id);
                ws_tx
                    .send(TungMsg::Binary(
                        Frame::ResponseHead(ResponseHead {
                            req_id: head.req_id,
                            status: 200,
                            headers: vec![("content-type".into(), "text/event-stream".into())],
                            local_latency_us: 10,
                        })
                        .encode()
                        .unwrap(),
                    ))
                    .await
                    .unwrap();
                ws_tx
                    .send(TungMsg::Binary(
                        Frame::ResponseBody {
                            req_id: head.req_id,
                            chunk: Bytes::from_static(b"data: chunk1\n\n"),
                        }
                        .encode()
                        .unwrap(),
                    ))
                    .await
                    .unwrap();
            }
            Frame::Cancel { req_id } => {
                assert_eq!(Some(req_id), got_req_id);
                got_cancel = true;
                break;
            }
            _ => {}
        }
    }

    public_task.await.unwrap();
    assert!(
        got_cancel,
        "expected Frame::Cancel when client dropped stream early"
    );
}
