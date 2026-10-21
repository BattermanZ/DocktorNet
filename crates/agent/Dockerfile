# syntax=docker/dockerfile:1
FROM docker.io/lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json --bin docktornet-agent

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json --bin docktornet-agent
COPY . .
RUN cargo build --release --bin docktornet-agent

FROM gcr.io/distroless/cc-debian13:nonroot AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/docktornet-agent /usr/local/bin/docktornet-agent
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/docktornet-agent"]
