# Builder stage (use latest stable to avoid repeated rustc version bumps)
FROM rust:bookworm AS builder

WORKDIR /app

# Install build dependencies for xmlsec (samael/SAML), bindgen, and rdkafka
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
# Copy only manifests and lock file so this layer is invalidated only when deps change
COPY core/Cargo.toml /app/core/Cargo.toml
COPY flow/Cargo.toml /app/flow/Cargo.toml
COPY flow/.cargo /app/flow/.cargo

RUN mkdir -p /app/core/src && echo "" > /app/core/src/lib.rs \
    && mkdir -p /app/flow/src && echo "fn main(){}" > /app/flow/src/main.rs

WORKDIR /app/flow
RUN cargo build --release && rm -rf /app/core/src /app/flow/src

# --- Application build layer ---
COPY core/ /app/core/
COPY flow/ /app/flow/

WORKDIR /app/flow
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

COPY --from=builder /app/flow/target/release/reiver-flow /app/reiver-flow

EXPOSE 3001

CMD ["./reiver-flow"]
