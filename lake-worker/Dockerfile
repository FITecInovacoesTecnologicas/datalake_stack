# syntax=docker/dockerfile:1.7
# lake-worker: multi-stage build. The dependency layer and the target cache mount
# keep iterations from downloading or recompiling DataFusion.
FROM rust:1.99-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
# Dependencies are downloaded into this layer, so they are fetched again only when
# Cargo.lock changes. (A registry cache mount shared by the parallel worker and query
# builds failed with "File exists" when both unpacked the same new crate.)
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo fetch --locked
COPY src ./src
# Each service keeps its own target cache, so the two builds never wait on each other.
RUN --mount=type=cache,id=lake-worker-target,target=/app/target \
    cargo build --release --locked --offline && cp target/release/lake-worker /lake-worker

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
