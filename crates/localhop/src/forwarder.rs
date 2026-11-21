use bytes::Bytes;
use http::{HeaderName, HeaderValue, Method, Request};
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame as HyperFrame;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use localhop_protocol::{is_hop_by_hop_header, Frame, RequestHead, ResponseHead};
use std::{
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::inspector::{
    format_body_preview, now_millis, InspectorHub, TrafficRecord, MAX_PREVIEW_BYTES,
};

type StreamingBody = StreamBody<ReceiverStream<Result<HyperFrame<Bytes>, Infallible>>>;

pub struct LocalForwarder {
    host: String,
    port: u16,
    stream_client: Client<HttpConnector, StreamingBody>,
    replay_client: Client<HttpConnector, Full<Bytes>>,
}

impl LocalForwarder {
    pub fn new(host: String, port: u16) -> Arc<Self> {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        connector.set_connect_timeout(Some(Duration::from_secs(5)));

        let stream_client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .build(connector.clone());

        let replay_client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .build(connector);

        Arc::new(Self {
            host,
            port,
            stream_client,
            replay_client,
        })
    }

    pub fn target_display(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    /// Streams a multiplexed tunnel request directly to the local dev server,
    /// streams the response back over the tunnel, and records a bounded preview
    /// for the local inspector.
    pub async fn forward_tunnel_request(
        &self,
        head: RequestHead,
        mut req_body_rx: mpsc::Receiver<Bytes>,
        outbound_tx: mpsc::Sender<Bytes>,
        hub: Arc<InspectorHub>,
    ) {
        let started = Instant::now();
        let req_id = head.req_id;
        let record_id = hub.next_record_id();

        // Tee incoming request chunks: stream directly to local upstream while
        // keeping a bounded preview for the inspector.
        let (upstream_tx, upstream_rx) = mpsc::channel::<Result<HyperFrame<Bytes>, Infallible>>(32);
        let req_preview_task = tokio::spawn(async move {
            let mut preview = Vec::new();
            let mut total_bytes = 0usize;
            while let Some(chunk) = req_body_rx.recv().await {
                total_bytes += chunk.len();
                if preview.len() < MAX_PREVIEW_BYTES {
                    let take = (MAX_PREVIEW_BYTES - preview.len()).min(chunk.len());
                    preview.extend_from_slice(&chunk[..take]);
                }
                if upstream_tx.send(Ok(HyperFrame::data(chunk))).await.is_err() {
                    break;
                }
            }
            (preview, total_bytes)
        });

        let uri = format!("http://{}:{}{}", self.host, self.port, head.path_and_query);
        let method = Method::from_bytes(head.method.as_bytes()).unwrap_or(Method::GET);

        let mut builder = Request::builder().method(method).uri(&uri);
        if let Some(hdrs) = builder.headers_mut() {
            for (k, v) in &head.headers {
                if is_hop_by_hop_header(k) || k.eq_ignore_ascii_case("host") {
                    continue;
                }
                if let (Ok(hn), Ok(hv)) = (
                    HeaderName::from_bytes(k.as_bytes()),
                    HeaderValue::from_str(v),
                ) {
                    hdrs.append(hn, hv);
                }
            }
            if let Ok(hv) = HeaderValue::from_str(&format!("{}:{}", self.host, self.port)) {
                hdrs.insert(http::header::HOST, hv);
            }
        }

        let body = StreamBody::new(ReceiverStream::new(upstream_rx));
        let Ok(http_req) = builder.body(body) else {
            let _ = send_error_response(&outbound_tx, req_id, 500, "invalid request").await;
            return;
        };

        let resp_res = self.stream_client.request(http_req).await;
        let local_latency_us = started.elapsed().as_micros() as u64;

        match resp_res {
            Ok(mut resp) => {
                let status = resp.status().as_u16();
                let resp_headers: Vec<(String, String)> = resp
                    .headers()
                    .iter()
                    .filter(|(n, _)| !is_hop_by_hop_header(n.as_str()))
                    .filter_map(|(n, v)| {
                        v.to_str()
                            .ok()
                            .map(|val| (n.as_str().to_string(), val.to_string()))
                    })
                    .collect();

                let resp_head = Frame::ResponseHead(ResponseHead {
                    req_id,
                    status,
                    headers: resp_headers.clone(),
                    local_latency_us,
                });
                if let Ok(bytes) = resp_head.encode() {
                    let _ = outbound_tx.send(bytes).await;
                }

                let mut resp_preview = Vec::new();
                let mut resp_bytes = 0usize;

                let body_stream = resp.body_mut();
                while let Some(frame_res) = body_stream.frame().await {
                    match frame_res {
                        Ok(frame) => {
                            if let Ok(data) = frame.into_data() {
                                if !data.is_empty() {
                                    resp_bytes += data.len();
                                    if resp_preview.len() < MAX_PREVIEW_BYTES {
                                        let take =
                                            (MAX_PREVIEW_BYTES - resp_preview.len()).min(data.len());
                                        resp_preview.extend_from_slice(&data[..take]);
                                    }
                                    if let Ok(bytes) = (Frame::ResponseBody {
                                        req_id,
                                        chunk: data,
                                    })
                                    .encode()
                                    {
                                        if outbound_tx.send(bytes).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }

                if let Ok(end) = (Frame::ResponseEnd { req_id }).encode() {
                    let _ = outbound_tx.send(end).await;
                }

                let (req_preview, req_bytes) = req_preview_task.await.unwrap_or_default();
                let total_us = started.elapsed().as_micros() as u64;

                print_access_line(status, &head.method, &head.path_and_query, local_latency_us, total_us);

                hub.push_record(TrafficRecord {
                    id: record_id,
                    timestamp_ms: now_millis(),
                    source: "tunnel".to_string(),
                    method: head.method,
                    path_and_query: head.path_and_query,
                    request_headers: head.headers,
                    request_body: format_body_preview(&req_preview, req_bytes),
                    request_bytes: req_bytes,
                    status,
                    response_headers: resp_headers,
                    response_body: format_body_preview(&resp_preview, resp_bytes),
                    response_bytes: resp_bytes,
                    local_latency_us,
                    error: None,
                })
                .await;
            }
            Err(err) => {
                let err_msg = format!(
                    "could not reach local server at http://{}:{}: {err}",
                    self.host, self.port
                );
                let _ = send_error_response(&outbound_tx, req_id, 502, &err_msg).await;
                let (req_preview, req_bytes) = req_preview_task.await.unwrap_or_default();

                print_access_line(502, &head.method, &head.path_and_query, local_latency_us, local_latency_us);

                hub.push_record(TrafficRecord {
                    id: record_id,
                    timestamp_ms: now_millis(),
                    source: "tunnel".to_string(),
                    method: head.method,
                    path_and_query: head.path_and_query,
                    request_headers: head.headers,
                    request_body: format_body_preview(&req_preview, req_bytes),
                    request_bytes: req_bytes,
                    status: 502,
                    response_headers: vec![],
                    response_body: err_msg.clone(),
                    response_bytes: err_msg.len(),
                    local_latency_us,
                    error: Some(err_msg),
                })
                .await;
            }
        }
    }

    /// Replays a captured (or edited) request directly against the local dev server.
    pub async fn replay_request(
        &self,
        record_id: u64,
        method_str: String,
        path_and_query: String,
        headers: Vec<(String, String)>,
        body: Bytes,
    ) -> TrafficRecord {
        let started = Instant::now();
        let req_bytes = body.len();
        let req_preview = format_body_preview(
            &body[..body.len().min(MAX_PREVIEW_BYTES)],
            req_bytes,
        );

        let normalized_path = if path_and_query.starts_with('/') {
            path_and_query
        } else {
            format!("/{path_and_query}")
        };
        let uri = format!("http://{}:{}{}", self.host, self.port, normalized_path);
        let method = Method::from_bytes(method_str.as_bytes()).unwrap_or(Method::GET);

        let mut builder = Request::builder().method(method).uri(&uri);
        if let Some(hdrs) = builder.headers_mut() {
            for (k, v) in &headers {
                if is_hop_by_hop_header(k)
                    || k.eq_ignore_ascii_case("host")
                    || k.eq_ignore_ascii_case("content-length")
                {
                    continue;
                }
                if let (Ok(hn), Ok(hv)) = (
                    HeaderName::from_bytes(k.as_bytes()),
                    HeaderValue::from_str(v),
                ) {
                    hdrs.append(hn, hv);
                }
            }
            if let Ok(hv) = HeaderValue::from_str(&format!("{}:{}", self.host, self.port)) {
                hdrs.insert(http::header::HOST, hv);
            }
        }

        let http_req = match builder.body(Full::new(body)) {
            Ok(r) => r,
            Err(e) => {
                return TrafficRecord {
                    id: record_id,
                    timestamp_ms: now_millis(),
                    source: "replay".to_string(),
                    method: method_str,
                    path_and_query: normalized_path,
                    request_headers: headers,
                    request_body: req_preview,
                    request_bytes: req_bytes,
                    status: 0,
                    response_headers: vec![],
                    response_body: e.to_string(),
                    response_bytes: 0,
                    local_latency_us: 0,
                    error: Some(e.to_string()),
                };
            }
        };

        let resp_res = self.replay_client.request(http_req).await;
        let local_latency_us = started.elapsed().as_micros() as u64;

        match resp_res {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let response_headers: Vec<(String, String)> = resp
                    .headers()
                    .iter()
                    .filter(|(n, _)| !is_hop_by_hop_header(n.as_str()))
                    .filter_map(|(n, v)| {
                        v.to_str()
                            .ok()
                            .map(|val| (n.as_str().to_string(), val.to_string()))
                    })
                    .collect();

                let body_bytes = resp
                    .into_body()
                    .collect()
                    .await
                    .map(|c| c.to_bytes())
                    .unwrap_or_default();
                let resp_bytes = body_bytes.len();
                let resp_preview = format_body_preview(
                    &body_bytes[..body_bytes.len().min(MAX_PREVIEW_BYTES)],
                    resp_bytes,
                );

                print_access_line(status, &method_str, &normalized_path, local_latency_us, local_latency_us);

                TrafficRecord {
                    id: record_id,
                    timestamp_ms: now_millis(),
                    source: "replay".to_string(),
                    method: method_str,
                    path_and_query: normalized_path,
                    request_headers: headers,
                    request_body: req_preview,
                    request_bytes: req_bytes,
                    status,
                    response_headers,
                    response_body: resp_preview,
                    response_bytes: resp_bytes,
                    local_latency_us,
                    error: None,
                }
            }
            Err(err) => {
                let msg = format!("replay failed: {err}");
                TrafficRecord {
                    id: record_id,
                    timestamp_ms: now_millis(),
                    source: "replay".to_string(),
                    method: method_str,
                    path_and_query: normalized_path,
                    request_headers: headers,
                    request_body: req_preview,
                    request_bytes: req_bytes,
                    status: 502,
                    response_headers: vec![],
                    response_body: msg.clone(),
                    response_bytes: msg.len(),
                    local_latency_us,
                    error: Some(msg),
                }
            }
        }
    }
}

async fn send_error_response(
    outbound_tx: &mpsc::Sender<Bytes>,
    req_id: u64,
    status: u16,
    message: &str,
) -> Result<(), ()> {
    let head = Frame::ResponseHead(ResponseHead {
        req_id,
        status,
        headers: vec![("content-type".to_string(), "text/plain; charset=utf-8".to_string())],
        local_latency_us: 0,
    });
    if let Ok(b) = head.encode() {
        let _ = outbound_tx.send(b).await;
    }
    let body = Frame::ResponseBody {
        req_id,
        chunk: Bytes::copy_from_slice(message.as_bytes()),
    };
    if let Ok(b) = body.encode() {
        let _ = outbound_tx.send(b).await;
    }
    if let Ok(b) = (Frame::ResponseEnd { req_id }).encode() {
        let _ = outbound_tx.send(b).await;
    }
    Ok(())
}

fn print_access_line(status: u16, method: &str, path: &str, local_us: u64, total_us: u64) {
    let color = match status {
        200..=299 => "\x1b[32m",
        300..=399 => "\x1b[36m",
        400..=499 => "\x1b[33m",
        _ => "\x1b[31m",
    };
    let local_ms = local_us as f64 / 1000.0;
    let total_ms = total_us as f64 / 1000.0;
    println!(
        "  {color}{status:3}\x1b[0m  {method:<6} {path:<40} \x1b[90mlocal {local_ms:.2}ms · total {total_ms:.2}ms\x1b[0m"
    );
}
