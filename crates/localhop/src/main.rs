mod forwarder;
mod inspector;

use bytes::Bytes;
use clap::Parser;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use localhop_protocol::{Frame, Hello};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    forwarder::LocalForwarder,
    inspector::{InspectorHub, start_inspector_server},
};

#[derive(Parser, Debug)]
#[command(
    name = "localhop",
    version,
    about = "Seamless, ultra-low-latency Rust localhost tunnel with live HTTP/webhook inspector & replay"
)]
struct Cli {
    /// Local port to expose (e.g. `localhop 3000`)
    port: u16,

    /// Local host to forward requests to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Public localhop relay server URL (http/https/ws/wss)
    #[arg(long, env = "LOCALHOP_SERVER", default_value = "ws://127.0.0.1:8080")]
    server: String,

    /// Optional custom tunnel ID / subdomain (e.g. `-s myapp`)
    #[arg(short = 's', long = "id", alias = "subdomain")]
    tunnel_id: Option<String>,

    /// Optional relay authentication token
    #[arg(long, env = "LOCALHOP_AUTH_TOKEN")]
    token: Option<String>,

    /// Protect the public tunnel URL with HTTP Basic Auth (`user:pass`)
    #[arg(long, env = "LOCALHOP_BASIC_AUTH")]
    basic_auth: Option<String>,

    /// Preserve incoming Host header instead of rewriting to `<host>:<port>`
    #[arg(long, default_value_t = false)]
    preserve_host: bool,

    /// Suppress per-request terminal access logs
    #[arg(short, long, default_value_t = false)]
    quiet: bool,

    /// Port for the built-in live HTTP & webhook inspector UI
    #[arg(long, default_value_t = 4040)]
    inspector_port: u16,

    /// Disable the local web inspector UI
    #[arg(long, default_value_t = false)]
    no_inspector: bool,
}

enum SessionEnd {
    Disconnected { was_connected: bool },
    Fatal(String),
    Transient { err: String, was_connected: bool },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "localhop=warn".into()),
        )
        .init();

    let cli = Cli::parse();
    if let Some(ref creds) = cli.basic_auth
        && !creds.contains(':')
    {
        eprintln!("\x1b[31merror:\x1b[0m --basic-auth must be in `user:pass` format");
        std::process::exit(2);
    }

    let ws_url = normalize_ws_url(&cli.server);
    let forwarder = LocalForwarder::new(cli.host.clone(), cli.port, cli.preserve_host, cli.quiet);
    let hub = InspectorHub::new(forwarder.target_display(), ws_url.clone());

    let inspector_url = if cli.no_inspector {
        None
    } else {
        match start_inspector_server(cli.inspector_port, hub.clone(), forwarder.clone()).await {
            Ok(addr) => Some(format!("http://{addr}")),
            Err(e) => {
                eprintln!(
                    "\x1b[33mwarn:\x1b[0m could not bind inspector on port {}: {e}",
                    cli.inspector_port
                );
                None
            }
        }
    };

    println!();
    println!("  \x1b[1;36mlocalhop\x1b[0m v{}", env!("CARGO_PKG_VERSION"));
    println!("  \x1b[90m────────────────────────────────────────────────────────\x1b[0m");
    println!("  Forwarding   ->  {}", forwarder.target_display());
    if let Some(url) = &inspector_url {
        println!("  Inspector    ->  \x1b[36m{url}\x1b[0m");
    }
    if cli.basic_auth.is_some() {
        println!("  Basic Auth   ->  \x1b[33menabled\x1b[0m");
    }
    println!("  Relay        ->  {ws_url}");
    println!();

    let base_backoff = Duration::from_millis(400);
    let mut backoff = base_backoff;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\n  \x1b[90mShutting down localhop…\x1b[0m");
                break;
            }
            outcome = run_session(
                &ws_url,
                cli.tunnel_id.clone(),
                cli.token.clone(),
                cli.basic_auth.clone(),
                forwarder.clone(),
                hub.clone(),
            ) => {
                hub.set_connected(false, None).await;
                match outcome {
                    SessionEnd::Fatal(err) => {
                        eprintln!("  \x1b[31m! Fatal error:\x1b[0m {err}");
                        std::process::exit(1);
                    }
                    SessionEnd::Disconnected { was_connected } => {
                        if was_connected {
                            backoff = base_backoff;
                        }
                        println!("  \x1b[33m! Relay disconnected. Reconnecting in {}ms…\x1b[0m", backoff.as_millis());
                    }
                    SessionEnd::Transient { err, was_connected } => {
                        if was_connected {
                            backoff = base_backoff;
                        }
                        eprintln!("  \x1b[31m! Connection error:\x1b[0m {err}");
                        println!("  \x1b[90m  Retrying in {}ms…\x1b[0m", backoff.as_millis());
                    }
                }
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => break,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        }
    }
}

async fn run_session(
    ws_url: &str,
    requested_id: Option<String>,
    token: Option<String>,
    basic_auth: Option<String>,
    forwarder: Arc<LocalForwarder>,
    hub: Arc<InspectorHub>,
) -> SessionEnd {
    let (ws_stream, _) = match connect_async(ws_url).await {
        Ok(ok) => ok,
        Err(e) => {
            return SessionEnd::Transient {
                err: format!("failed to connect to relay {ws_url}: {e}"),
                was_connected: false,
            };
        }
    };

    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    let hello = match Frame::Hello(Hello {
        requested_id,
        token,
        basic_auth,
    })
    .encode()
    {
        Ok(b) => b,
        Err(e) => return SessionEnd::Fatal(e.to_string()),
    };

    if let Err(e) = ws_tx.send(Message::Binary(hello)).await {
        return SessionEnd::Transient {
            err: e.to_string(),
            was_connected: false,
        };
    }

    let welcome = match tokio::time::timeout(Duration::from_secs(10), ws_rx.next()).await {
        Ok(Some(Ok(Message::Binary(raw)))) => match Frame::decode(raw) {
            Ok(Frame::Welcome(w)) => w,
            Ok(Frame::Error(e)) => {
                if e.message.contains("unauthorized")
                    || e.message.contains("invalid tunnel id")
                    || e.message.contains("invalid basic_auth")
                {
                    return SessionEnd::Fatal(e.message);
                }
                return SessionEnd::Transient {
                    err: e.message,
                    was_connected: false,
                };
            }
            Ok(other) => {
                return SessionEnd::Transient {
                    err: format!("unexpected handshake frame: {other:?}"),
                    was_connected: false,
                };
            }
            Err(e) => {
                return SessionEnd::Transient {
                    err: format!("handshake decode error: {e}"),
                    was_connected: false,
                };
            }
        },
        Ok(Some(Err(e))) => {
            return SessionEnd::Transient {
                err: format!("websocket error: {e}"),
                was_connected: false,
            };
        }
        Ok(None) => {
            return SessionEnd::Transient {
                err: "relay closed connection during handshake".to_string(),
                was_connected: false,
            };
        }
        Ok(_) => {
            return SessionEnd::Transient {
                err: "unexpected non-binary handshake message".to_string(),
                was_connected: false,
            };
        }
        Err(_) => {
            return SessionEnd::Transient {
                err: "handshake timed out".to_string(),
                was_connected: false,
            };
        }
    };

    hub.set_connected(true, Some(welcome.public_url.clone()))
        .await;

    println!(
        "  \x1b[1;32m● LIVE\x1b[0m  Public URL  ->  \x1b[1;32m{}\x1b[0m",
        welcome.public_url
    );
    println!("  \x1b[90m────────────────────────────────────────────────────────\x1b[0m");

    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Bytes>(512);
    let in_flight_http: Arc<DashMap<u64, mpsc::Sender<Bytes>>> = Arc::new(DashMap::new());
    let in_flight_ws: Arc<DashMap<u64, mpsc::Sender<(bool, Bytes)>>> = Arc::new(DashMap::new());

    let writer = tokio::spawn(async move {
        while let Some(frame_bytes) = outbound_rx.recv().await {
            if ws_tx.send(Message::Binary(frame_bytes)).await.is_err() {
                break;
            }
        }
    });

    // Periodic RTT Ping task (also keeps the tunnel alive past idle timeout)
    let ping_tx = outbound_tx.clone();
    let pinger = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            let ts_us = now_micros();
            if let Ok(bytes) = (Frame::Ping { ts_us }).encode()
                && ping_tx.send(bytes).await.is_err()
            {
                break;
            }
        }
    });

    while let Some(msg_res) = ws_rx.next().await {
        match msg_res {
            Ok(Message::Binary(raw)) => match Frame::decode(raw) {
                Ok(Frame::RequestHead(head)) => {
                    let req_id = head.req_id;
                    let (body_tx, body_rx) = mpsc::channel::<Bytes>(64);
                    in_flight_http.insert(req_id, body_tx);

                    let fwd = forwarder.clone();
                    let out = outbound_tx.clone();
                    let hub_clone = hub.clone();
                    let map = in_flight_http.clone();
                    tokio::spawn(async move {
                        fwd.forward_tunnel_request(head, body_rx, out, hub_clone)
                            .await;
                        map.remove(&req_id);
                    });
                }
                Ok(Frame::RequestBody { req_id, chunk }) => {
                    let tx = in_flight_http.get(&req_id).map(|e| e.value().clone());
                    if let Some(tx) = tx {
                        let _ = tx.send(chunk).await;
                    }
                }
                Ok(Frame::RequestEnd { req_id }) => {
                    in_flight_http.remove(&req_id);
                }
                Ok(Frame::WsOpen(open)) => {
                    let req_id = open.req_id;
                    let (ws_in_tx, ws_in_rx) = mpsc::channel::<(bool, Bytes)>(128);
                    in_flight_ws.insert(req_id, ws_in_tx);

                    let fwd = forwarder.clone();
                    let out = outbound_tx.clone();
                    let hub_clone = hub.clone();
                    let map = in_flight_ws.clone();
                    tokio::spawn(async move {
                        fwd.forward_ws_connection(open, ws_in_rx, out, hub_clone)
                            .await;
                        map.remove(&req_id);
                    });
                }
                Ok(Frame::WsMessage {
                    req_id,
                    is_binary,
                    payload,
                }) => {
                    let tx = in_flight_ws.get(&req_id).map(|e| e.value().clone());
                    if let Some(tx) = tx {
                        let _ = tx.send((is_binary, payload)).await;
                    }
                }
                Ok(Frame::WsClose { req_id }) => {
                    in_flight_ws.remove(&req_id);
                }
                Ok(Frame::Pong { ts_us }) => {
                    let now = now_micros();
                    if now >= ts_us {
                        hub.set_rtt_us(now - ts_us).await;
                    }
                }
                Ok(Frame::Error(e)) => {
                    writer.abort();
                    pinger.abort();
                    return SessionEnd::Transient {
                        err: e.message,
                        was_connected: true,
                    };
                }
                _ => {}
            },
            Ok(Message::Close(_)) => break,
            Err(e) => {
                writer.abort();
                pinger.abort();
                return SessionEnd::Transient {
                    err: e.to_string(),
                    was_connected: true,
                };
            }
            _ => {}
        }
    }

    writer.abort();
    pinger.abort();
    SessionEnd::Disconnected {
        was_connected: true,
    }
}

fn normalize_ws_url(input: &str) -> String {
    let trimmed = input.trim().trim_end_matches('/');
    let with_scheme = if let Some(rest) = trimmed.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = trimmed.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        trimmed.to_string()
    } else if trimmed.starts_with("localhost") || trimmed.starts_with("127.0.0.1") {
        format!("ws://{trimmed}")
    } else {
        format!("wss://{trimmed}")
    };

    if with_scheme.ends_with("/_localhop/ws") {
        with_scheme
    } else {
        format!("{with_scheme}/_localhop/ws")
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::normalize_ws_url;

    #[test]
    fn normalizes_relay_urls() {
        assert_eq!(
            normalize_ws_url("https://hop.onrender.com/"),
            "wss://hop.onrender.com/_localhop/ws"
        );
        assert_eq!(
            normalize_ws_url("http://127.0.0.1:8080"),
            "ws://127.0.0.1:8080/_localhop/ws"
        );
        assert_eq!(
            normalize_ws_url("hop.example.com"),
            "wss://hop.example.com/_localhop/ws"
        );
        assert_eq!(
            normalize_ws_url("wss://hop.example.com/_localhop/ws"),
            "wss://hop.example.com/_localhop/ws"
        );
    }
}
