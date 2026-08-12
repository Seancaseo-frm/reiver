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

COPY core/Cargo.toml /app/core/Cargo.toml
COPY herd/Cargo.toml /app/herd/Cargo.toml

RUN mkdir -p /app/core/src && echo "" > /app/core/src/lib.rs \
    && mkdir -p /app/herd/src && echo "fn main(){}" > /app/herd/src/main.rs

WORKDIR /app/herd
RUN cargo build --release && rm -rf /app/core/src /app/herd/src

COPY core/ /app/core/
COPY herd/ /app/herd/

WORKDIR /app/herd
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libxml2 \
    libxmlsec1 \
    libxmlsec1-openssl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/herd/target/release/reiver-herd /app/reiver-herd

EXPOSE 3003

CMD ["./reiver-herd"]
