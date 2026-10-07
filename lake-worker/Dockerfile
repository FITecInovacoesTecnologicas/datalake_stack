# syntax=docker/dockerfile:1.7
# lake-worker: multi-stage build. Cache mounts keep the cargo registry and the
# target dir between builds so iterations do not recompile DataFusion.
FROM rust:1.99-bookworm AS builder
WORKDIR /app
COPY Cargo.toml ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release && cp target/release/lake-worker /lake-worker

FROM debian:bookworm-slim
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    useradd --system --uid 10001 --create-home lake; \
    mkdir -p /spill && chown lake:lake /spill
COPY --from=builder /lake-worker /usr/local/bin/lake-worker
USER lake
ENV RUST_LOG=info
CMD ["lake-worker"]
