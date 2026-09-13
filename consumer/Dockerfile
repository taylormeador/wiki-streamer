# Build scaffolding only — no pipeline logic here.
# Note: once you add a Kafka client crate (e.g. rdkafka), it links against
# librdkafka in C, so this builder stage will likely need
# `apt-get install -y cmake build-essential libssl-dev pkg-config` too.
FROM rust:1-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/consumer /usr/local/bin/consumer
CMD ["consumer"]
