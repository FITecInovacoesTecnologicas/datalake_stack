# syntax=docker/dockerfile:1.7
# lake-query: multi-stage build with cargo cache mounts (see lake-worker/Dockerfile).
FROM rust:1.99-bookworm AS builder
WORKDIR /app
COPY Cargo.toml ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release && cp target/release/lake-query /lake-query

FROM debian:bookworm-slim
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    useradd --system --uid 10001 --create-home lake; \
    mkdir -p /spill && chown lake:lake /spill
COPY --from=builder /lake-query /usr/local/bin/lake-query
USER lake
ENV RUST_LOG=info
EXPOSE 50051
CMD ["lake-query"]
