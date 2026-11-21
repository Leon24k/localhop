use axum::{routing::get, Router};
use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt, Empty};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const TOTAL_REQUESTS: usize = 3_000;
const CONCURRENCY: usize = 32;

async fn run_load(label: &str, url: String) -> (f64, f64, f64, f64, f64) {
    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    let client: Client<HttpConnector, Empty<Bytes>> = Client::builder(TokioExecutor::new())
        .pool_max_idle_per_host(CONCURRENCY)
        .build(connector);

    // Warmup
    for _ in 0..50 {
        let req = Request::builder().uri(&url).body(Empty::new()).unwrap();
        let resp = client.request(req).await.expect("warmup request failed");
        let _ = resp.into_body().collect().await;
    }

    let counter = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut handles = Vec::with_capacity(CONCURRENCY);

    for _ in 0..CONCURRENCY {
        let client = client.clone();
        let url = url.clone();
        let counter = counter.clone();
        handles.push(tokio::spawn(async move {
            let mut lats_us = Vec::with_capacity(TOTAL_REQUESTS / CONCURRENCY + 16);
            loop {
                let idx = counter.fetch_add(1, Ordering::Relaxed);
                if idx >= TOTAL_REQUESTS {
                    break;
                }
                let t0 = Instant::now();
                let req = Request::builder().uri(&url).body(Empty::new()).unwrap();
                let resp = client.request(req).await.expect("bench request failed");
                assert_eq!(resp.status(), 200);
                let _ = resp.into_body().collect().await.unwrap();
                lats_us.push(t0.elapsed().as_micros() as u64);
            }
            lats_us
        }));
    }

    let mut all = Vec::with_capacity(TOTAL_REQUESTS);
    for h in handles {
        all.extend(h.await.unwrap());
    }
    let elapsed = started.elapsed().as_secs_f64();
    all.sort_unstable();

    let pct = |p: f64| -> f64 {
        let idx = ((all.len() as f64 * p).ceil() as usize).saturating_sub(1);
        all[idx.min(all.len() - 1)] as f64 / 1000.0
    };

    let rps = all.len() as f64 / elapsed;
    let p50 = pct(0.50);
    let p95 = pct(0.95);
    let p99 = pct(0.99);
    let p999 = pct(0.999);
    println!(
        "{label:<22} | {rps:>8.0} req/s | p50 {p50:>5.2}ms | p95 {p95:>5.2}ms | p99 {p99:>5.2}ms | p99.9 {p999:>5.2}ms"
    );
    (rps, p50, p95, p99, p999)
}

#[tokio::main]
async fn main() {
    let target_url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:18080/t/bench/ping".to_string());

    if target_url == "--upstream" {
        let port: u16 = std::env::args()
            .nth(2)
            .and_then(|s| s.parse().ok())
            .unwrap_or(13001);
        let app = Router::new().route("/ping", get(|| async { r#"{"ok":true}"# }));
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        axum::serve(listener, app).await.unwrap();
        return;
    }

    println!("Benchmarking {TOTAL_REQUESTS} requests @ concurrency {CONCURRENCY}");
    let _ = run_load("direct (localhost)", "http://127.0.0.1:13001/ping".to_string()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = run_load("through localhop", target_url).await;
}
