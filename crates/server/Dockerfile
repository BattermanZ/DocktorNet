# syntax=docker/dockerfile:1
# Base images are pinned by tag and digest.  The cargo-chef image's Rust
# matches rust-toolchain.toml, and its Debian matches the distroless runtime.
FROM docker.io/lukemathwalker/cargo-chef:0.1.78-rust-1.99.1-trixie@sha256:cccf15fc75944521a3aa230fe72a101ee0edc65cd1d4b50b5914ce25317c1805 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
# The recipe keeps the whole workspace.  `--bin` would drop the other members,
# and `cook --locked` then fails because the lock file no longer matches.
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked --bin docktornet-server
# Distroless has no shell to mkdir with, so the empty /data is made here.
RUN mkdir -p /out/data

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:83b9b50586253821cf63ffcec87d3d94265a093ad30da232039af962dd8f4edf AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/docktornet-server /usr/local/bin/docktornet-server
# No VOLUME: an anonymous volume would hide a forgotten mount from the
# server's ephemeral storage check.  A named volume at /data copies this
# directory's 65532 ownership on first use.
COPY --from=builder --chown=65532:65532 /out/data /data
ENV DOCKTORNET_DB_PATH=/data/docktornet.db
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/docktornet-server"]
