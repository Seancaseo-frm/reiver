# Builder stage
# Use latest stable Rust to avoid repeated rustc version bumps from deps
FROM rust:bookworm AS builder

WORKDIR /app

# Install build dependencies for bindgen, rdkafka, xmlsec (samael/SAML), and protobuf (googleads-rs)
RUN apt-get update && apt-get install -y \
    pkg-config \
    libxml2-dev \
    libxmlsec1-dev \
    libxmlsec1-openssl \
    protobuf-compiler \
    libclang-dev \
    clang \
    cmake \
    libssl-dev \
    libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

# --- Dependency cache layer ---
COPY core/Cargo.toml /app/core/Cargo.toml
COPY sdk/rust/Cargo.toml /app/sdk/rust/Cargo.toml
COPY pond/Cargo.toml /app/pond/Cargo.toml

RUN mkdir -p /app/core/src && echo "" > /app/core/src/lib.rs \
    && mkdir -p /app/sdk/rust/src && echo "" > /app/sdk/rust/src/lib.rs \
    && mkdir -p /app/pond/src && echo "fn main(){}" > /app/pond/src/main.rs \
    && mkdir -p /app/pond/benches \
    && echo "fn main(){}" > /app/pond/benches/warehouse_benchmarks.rs \
    && echo "fn main(){}" > /app/pond/benches/realistic_io_benchmark.rs \
    && echo "fn main(){}" > /app/pond/benches/file_size_benchmark.rs

WORKDIR /app/pond
RUN cargo build --release && rm -rf /app/core/src /app/sdk/rust/src /app/pond/src

# --- Application build layer ---
COPY core/ /app/core/
COPY sdk/ /app/sdk/
COPY pond/ /app/pond/

WORKDIR /app/pond
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

COPY --from=builder /app/pond/target/release/reiver-pond /app/reiver-pond

EXPOSE 3002
EXPOSE 5433

CMD ["./reiver-pond"]
