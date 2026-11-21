# Multi-stage build for an ultra-slim localhop-server image (~15 MB)
FROM rust:1-slim-bookworm AS builder
WORKDIR /app

# Cache dependencies first
COPY Cargo.toml ./
COPY crates/localhop-protocol/Cargo.toml crates/localhop-protocol/Cargo.toml
COPY crates/localhop-server/Cargo.toml crates/localhop-server/Cargo.toml
COPY crates/localhop/Cargo.toml crates/localhop/Cargo.toml
RUN mkdir -p crates/localhop-protocol/src crates/localhop-server/src crates/localhop/src \
 && echo "pub fn _stub() {}" > crates/localhop-protocol/src/lib.rs \
 && echo "fn main() {}" > crates/localhop-server/src/main.rs \
 && echo "fn main() {}" > crates/localhop/src/main.rs \
 && cargo build --release --bin localhop-server || true

# Build actual source
COPY crates ./crates
RUN touch crates/localhop-protocol/src/lib.rs crates/localhop-server/src/main.rs \
 && cargo build --release --bin localhop-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/localhop-server /usr/local/bin/localhop-server

ENV LOCALHOP_BIND=0.0.0.0
ENV PORT=8080
EXPOSE 8080

CMD ["localhop-server"]
