FROM rust:bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    pkg-config \
    libclang-dev \
    clang \
    cmake \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY core/Cargo.toml /app/core/Cargo.toml
COPY mcp/Cargo.toml /app/mcp/Cargo.toml

RUN mkdir -p /app/core/src && echo "" > /app/core/src/lib.rs \
    && mkdir -p /app/mcp/src && echo "fn main(){}" > /app/mcp/src/main.rs

WORKDIR /app/mcp
RUN cargo build --release && rm -rf /app/core/src /app/mcp/src

COPY core/ /app/core/
COPY mcp/ /app/mcp/

WORKDIR /app/mcp
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/mcp/target/release/reiver-mcp /app/reiver-mcp

EXPOSE 3002

CMD ["./reiver-mcp", "--transport", "http", "--listen", "0.0.0.0:3002"]
