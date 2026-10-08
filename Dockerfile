# syntax=docker/dockerfile:1.7
# Rust images of the stack (Cargo workspace): one builder stage compiles lake-worker AND
# lake-query in a single cargo run, so DataFusion, Arrow and iceberg-rust are built once.
# The compose picks the final image with build.target (lake-worker | lake-query); both
# targets reuse the same builder stage, which BuildKit runs only once.
FROM rust:1.99-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY lake-worker/Cargo.toml lake-worker/
COPY lake-query/Cargo.toml lake-query/
# Dependencies are downloaded into this layer, so they are fetched again only when
# Cargo.lock changes. The stub main.rs files exist only so cargo can read the workspace.
RUN set -eux; \
    mkdir -p lake-worker/src lake-query/src; \
    echo 'fn main() {}' > lake-worker/src/main.rs; \
    echo 'fn main() {}' > lake-query/src/main.rs; \
    cargo fetch --locked
COPY lake-worker/src lake-worker/src
COPY lake-query/src lake-query/src
# LTO only adds link time in dev: off by default. Cargo reads this build arg as an env var;
# pass "thin" (RUST_RELEASE_LTO=thin in .env) for a production build.
ARG CARGO_PROFILE_RELEASE_LTO=false
# One target cache for the workspace. Only this stage writes to it, so nothing races.
RUN --mount=type=cache,id=rust-datalake-target,target=/app/target \
    cargo build --release --locked --offline --workspace \
    && cp target/release/lake-worker target/release/lake-query /usr/local/bin/

FROM debian:bookworm-slim AS runtime
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    useradd --system --uid 10001 --create-home lake; \
    mkdir -p /spill && chown lake:lake /spill
USER lake
ENV RUST_LOG=info

FROM runtime AS lake-query
COPY --from=builder /usr/local/bin/lake-query /usr/local/bin/lake-query
EXPOSE 50051
CMD ["lake-query"]

FROM runtime AS lake-worker
COPY --from=builder /usr/local/bin/lake-worker /usr/local/bin/lake-worker
CMD ["lake-worker"]
