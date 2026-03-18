FROM ubuntu:20.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    libsqlite3-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY shared/ shared/
COPY log_parser/ log_parser/
COPY event_storage/ event_storage/
COPY file_watcher/ file_watcher/

RUN cargo build --release -p file_watcher

FROM scratch AS export
COPY --from=builder /build/target/release/file_watcher /file_watcher
