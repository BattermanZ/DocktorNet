# syntax=docker/dockerfile:1
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json --bin docktornet-server

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json --bin docktornet-server
COPY . .
RUN cargo build --release --bin docktornet-server
# Distroless has no shell to mkdir with, so the empty /data is made here.
RUN mkdir -p /out/data

FROM gcr.io/distroless/cc-debian13:nonroot AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/docktornet-server /usr/local/bin/docktornet-server
# No VOLUME: an anonymous volume would hide a forgotten mount from the
# server's ephemeral storage check.  A named volume at /data copies this
# directory's 65532 ownership on first use.
COPY --from=builder --chown=65532:65532 /out/data /data
ENV DOCKTORNET_DB_PATH=/data/docktornet.db
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/docktornet-server"]
