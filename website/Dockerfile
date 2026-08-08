# Builder stage
# Latest stable Rust for deps (avoids repeated version bumps)
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
COPY website/Cargo.toml /app/website/Cargo.toml

RUN mkdir -p /app/core/src && echo "" > /app/core/src/lib.rs \
    && mkdir -p /app/website/src && echo "fn main(){}" > /app/website/src/main.rs

WORKDIR /app/website
RUN cargo build --release && rm -rf /app/core/src /app/website/src

# --- Application build layer ---
COPY core/ /app/core/
COPY website/ /app/website/

WORKDIR /app/website
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

COPY --from=builder /app/website/target/release/reiver-website /app/reiver-website
COPY --from=builder /app/website/migrations /app/migrations

EXPOSE 3003

CMD ["./reiver-website"]
