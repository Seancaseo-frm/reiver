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
COPY flow/Cargo.toml /app/flow/Cargo.toml

RUN mkdir -p /app/core/src && echo "" > /app/core/src/lib.rs \
    && mkdir -p /app/flow/src && echo "fn main(){}" > /app/flow/src/main.rs \
    && mkdir -p /app/watch/src && echo "fn main(){}" > /app/watch/src/main.rs \
    && mkdir -p /app/website/src && echo "fn main(){}" > /app/website/src/main.rs \
    && mkdir -p /app/mcp/src && echo "fn main(){}" > /app/mcp/src/main.rs \
    && mkdir -p /app/herd/src && echo "fn main(){}" > /app/herd/src/main.rs

COPY watch/Cargo.toml /app/watch/Cargo.toml
COPY website/Cargo.toml /app/website/Cargo.toml
COPY mcp/Cargo.toml /app/mcp/Cargo.toml
COPY herd/Cargo.toml /app/herd/Cargo.toml

RUN cargo build --release -p reiver-flow 2>/dev/null || true && rm -rf /app/core/src /app/flow/src

COPY core/ /app/core/
COPY flow/ /app/flow/

RUN cargo build --release -p reiver-flow

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libxml2 \
    libxmlsec1 \
    libxmlsec1-openssl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/reiver-flow /app/reiver-flow

EXPOSE 3001

CMD ["./reiver-flow"]
