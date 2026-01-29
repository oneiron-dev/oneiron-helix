FROM rust:latest AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --release --package helix-container

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/helix-container /usr/local/bin/helix-container

RUN mkdir -p /data

ENV HELIX_DATA_DIR=/data
ENV HELIX_PORT=8080

EXPOSE 8080

CMD ["helix-container"]
