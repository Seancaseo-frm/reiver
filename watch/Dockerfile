# Builder stage (latest stable Rust for deps)
FROM rust:bookworm AS builder

WORKDIR /app

# Install build dependencies for xmlsec (SAML), bindgen, and rdkafka
RUN apt-get update && apt-get install -y \
    pkg-config \
    libxml2-dev \
    libxmlsec1-dev \
    libxmlsec1-openssl \
    libclang-dev \
    clang \
    cmake \
    libssl-dev \
    libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

# --- Dependency cache layer ---
COPY core/Cargo.toml /app/core/Cargo.toml
COPY sdk/rust/Cargo.toml /app/sdk/rust/Cargo.toml
COPY watch/Cargo.toml /app/watch/Cargo.toml
COPY watch/integrations/server/webhooks/Cargo.toml /app/watch/integrations/server/webhooks/Cargo.toml
COPY watch/integrations/server/aws/Cargo.toml /app/watch/integrations/server/aws/Cargo.toml
COPY watch/integrations/server/azure/Cargo.toml /app/watch/integrations/server/azure/Cargo.toml
COPY watch/integrations/server/gcp/Cargo.toml /app/watch/integrations/server/gcp/Cargo.toml
COPY watch/integrations/server/oci/Cargo.toml /app/watch/integrations/server/oci/Cargo.toml
COPY watch/integrations/server/snowflake/Cargo.toml /app/watch/integrations/server/snowflake/Cargo.toml

RUN mkdir -p /app/core/src && echo "" > /app/core/src/lib.rs \
    && mkdir -p /app/sdk/rust/src && echo "" > /app/sdk/rust/src/lib.rs \
    && mkdir -p /app/watch/src && echo "fn main(){}" > /app/watch/src/main.rs \
    && mkdir -p /app/watch/benches && echo "fn main(){}" > /app/watch/benches/hot_path_benchmarks.rs \
    && mkdir -p /app/watch/integrations/server/webhooks/src && echo "" > /app/watch/integrations/server/webhooks/src/lib.rs \
    && mkdir -p /app/watch/integrations/server/aws/src && echo "" > /app/watch/integrations/server/aws/src/lib.rs \
    && mkdir -p /app/watch/integrations/server/azure/src && echo "" > /app/watch/integrations/server/azure/src/lib.rs \
    && mkdir -p /app/watch/integrations/server/gcp/src && echo "" > /app/watch/integrations/server/gcp/src/lib.rs \
    && mkdir -p /app/watch/integrations/server/oci/src && echo "" > /app/watch/integrations/server/oci/src/lib.rs \
    && mkdir -p /app/watch/integrations/server/snowflake/src && echo "" > /app/watch/integrations/server/snowflake/src/lib.rs

WORKDIR /app/watch
# Limit parallel jobs to avoid OOM when compiling aws-sdk-ec2 and crypto deps
ENV CARGO_BUILD_JOBS=2
RUN cargo build --release && rm -rf /app/core/src /app/sdk/rust/src /app/watch/src /app/watch/integrations

# --- Application build layer ---
COPY core/ /app/core/
COPY sdk/ /app/sdk/
COPY watch/ /app/watch/

WORKDIR /app/watch
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libxml2 \
    libxmlsec1 \
    libxmlsec1-openssl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/watch/target/release/reiver-watch /app/reiver-watch

EXPOSE 3000

CMD ["./reiver-watch"]
