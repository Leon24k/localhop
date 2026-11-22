use clap::Parser;
use localhop_server::{Config, build_router};
use std::net::SocketAddr;
use tracing::info;

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

    let (app, _state) = build_router(config.clone());

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind TCP listener");

    info!("localhop-server listening on http://{addr}");
    if let Some(domain) = &config.base_domain {
        info!("subdomain routing enabled for *.{domain}");
    }
    info!(
        max_tunnels = config.max_tunnels,
        max_body_mb = config.max_body_bytes / (1024 * 1024),
        idle_timeout_secs = config.idle_timeout_secs,
        "path routing enabled at /t/<id>/..."
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .expect("server error");
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutting down localhop-server");
}
