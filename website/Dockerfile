FROM rust:bookworm AS builder

WORKDIR /app

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

COPY Cargo.toml Cargo.lock /app/
COPY core/Cargo.toml /app/core/Cargo.toml
COPY website/Cargo.toml /app/website/Cargo.toml

RUN mkdir -p /app/core/src && echo "" > /app/core/src/lib.rs \
    && mkdir -p /app/website/src && echo "fn main(){}" > /app/website/src/main.rs \
    && mkdir -p /app/watch/src && echo "fn main(){}" > /app/watch/src/main.rs \
    && mkdir -p /app/flow/src && echo "fn main(){}" > /app/flow/src/main.rs \
    && mkdir -p /app/mcp/src && echo "fn main(){}" > /app/mcp/src/main.rs \
    && mkdir -p /app/herd/src && echo "fn main(){}" > /app/herd/src/main.rs

COPY watch/Cargo.toml /app/watch/Cargo.toml
COPY flow/Cargo.toml /app/flow/Cargo.toml
COPY mcp/Cargo.toml /app/mcp/Cargo.toml
COPY herd/Cargo.toml /app/herd/Cargo.toml

RUN cargo build --release -p reiver-website 2>/dev/null || true && rm -rf /app/core/src /app/website/src

COPY core/ /app/core/
COPY website/ /app/website/

RUN cargo build --release -p reiver-website

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libxml2 \
    libxmlsec1 \
    libxmlsec1-openssl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/reiver-website /app/reiver-website
COPY website/migrations /app/migrations

EXPOSE 3003

CMD ["./reiver-website"]
