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
    inspector::{start_inspector_server, InspectorHub},
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
    #[arg(
        long,
        env = "LOCALHOP_SERVER",
        default_value = "ws://127.0.0.1:8080"
    )]
    server: String,

    /// Optional custom tunnel ID / subdomain (e.g. `-s myapp`)
    #[arg(short = 's', long = "id", alias = "subdomain")]
    tunnel_id: Option<String>,

    /// Optional relay authentication token
    #[arg(long, env = "LOCALHOP_AUTH_TOKEN")]
    token: Option<String>,

    /// Port for the built-in live HTTP & webhook inspector UI
    #[arg(long, default_value_t = 4040)]
    inspector_port: u16,

    /// Disable the local web inspector UI
    #[arg(long, default_value_t = false)]
    no_inspector: bool,
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
    let ws_url = normalize_ws_url(&cli.server);
    let forwarder = LocalForwarder::new(cli.host.clone(), cli.port);
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
    println!("  Relay        ->  {ws_url}");
    println!();

    // Auto-reconnect loop with clean Ctrl+C shutdown
    let mut backoff = Duration::from_millis(400);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\n  \x1b[90mShutting down localhop…\x1b[0m");
                break;
            }
            res = run_session(&ws_url, cli.tunnel_id.clone(), cli.token.clone(), forwarder.clone(), hub.clone()) => {
                hub.set_connected(false, None).await;
                match res {
                    Ok(()) => {
                        println!("  \x1b[33m! Relay disconnected. Reconnecting in {}ms…\x1b[0m", backoff.as_millis());
                    }
                    Err(err) => {
                        eprintln!("  \x1b[31m! Connection error:\x1b[0m {err}");
                        if err.contains("unauthorized") || err.contains("invalid tunnel id") {
                            std::process::exit(1);
                        }
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
    forwarder: Arc<LocalForwarder>,
    hub: Arc<InspectorHub>,
) -> Result<(), String> {
    let (ws_stream, _) = connect_async(ws_url)
        .await
        .map_err(|e| format!("failed to connect to relay {ws_url}: {e}"))?;

    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    // Send Hello handshake
    let hello = Frame::Hello(Hello {
        requested_id,
        token,
    })
    .encode()
    .map_err(|e| e.to_string())?;
    ws_tx
        .send(Message::Binary(hello))
        .await
        .map_err(|e| e.to_string())?;

    // Wait for Welcome or Error
    let welcome = match tokio::time::timeout(Duration::from_secs(10), ws_rx.next()).await {
        Ok(Some(Ok(Message::Binary(raw)))) => match Frame::decode(raw) {
            Ok(Frame::Welcome(w)) => w,
            Ok(Frame::Error(e)) => return Err(e.message),
            Ok(other) => return Err(format!("unexpected handshake frame: {other:?}")),
            Err(e) => return Err(format!("handshake decode error: {e}")),
        },
        Ok(Some(Err(e))) => return Err(format!("websocket error: {e}")),
        Ok(None) => return Err("relay closed connection during handshake".to_string()),
        Ok(_) => return Err("unexpected non-binary handshake message".to_string()),
        Err(_) => return Err("handshake timed out".to_string()),
    };

    hub.set_connected(true, Some(welcome.public_url.clone())).await;

    println!("  \x1b[1;32m● LIVE\x1b[0m  Public URL  ->  \x1b[1;32m{}\x1b[0m", welcome.public_url);
    println!("  \x1b[90m────────────────────────────────────────────────────────\x1b[0m");

    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Bytes>(512);
    let in_flight: Arc<DashMap<u64, mpsc::Sender<Bytes>>> = Arc::new(DashMap::new());

    // Writer task
    let writer = tokio::spawn(async move {
        while let Some(frame_bytes) = outbound_rx.recv().await {
            if ws_tx.send(Message::Binary(frame_bytes)).await.is_err() {
                break;
            }
        }
    });

    // Periodic RTT Ping task (every 5 seconds)
    let ping_tx = outbound_tx.clone();
    let pinger = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            let ts_us = now_micros();
            if let Ok(bytes) = (Frame::Ping { ts_us }).encode() {
                if ping_tx.send(bytes).await.is_err() {
                    break;
                }
            }
        }
    });

    // Reader loop
    while let Some(msg_res) = ws_rx.next().await {
        match msg_res {
            Ok(Message::Binary(raw)) => match Frame::decode(raw) {
                Ok(Frame::RequestHead(head)) => {
                    let req_id = head.req_id;
                    let (body_tx, body_rx) = mpsc::channel::<Bytes>(64);
                    in_flight.insert(req_id, body_tx);

                    let fwd = forwarder.clone();
                    let out = outbound_tx.clone();
                    let hub_clone = hub.clone();
                    let map = in_flight.clone();
                    tokio::spawn(async move {
                        fwd.forward_tunnel_request(head, body_rx, out, hub_clone)
                            .await;
                        map.remove(&req_id);
                    });
                }
                Ok(Frame::RequestBody { req_id, chunk }) => {
                    let tx = in_flight.get(&req_id).map(|e| e.value().clone());
                    if let Some(tx) = tx {
                        let _ = tx.send(chunk).await;
                    }
                }
                Ok(Frame::RequestEnd { req_id }) => {
                    // Dropping sender signals EOF on the streaming upstream request body
                    in_flight.remove(&req_id);
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
                    return Err(e.message);
                }
                _ => {}
            },
            Ok(Message::Close(_)) => break,
            Err(e) => {
                writer.abort();
                pinger.abort();
                return Err(e.to_string());
            }
            _ => {}
        }
    }

    writer.abort();
    pinger.abort();
    Ok(())
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
