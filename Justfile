default:
    @just --list

# format all crates
fmt:
    cargo fmt --all

# clippy across the workspace
lint:
    cargo clippy --workspace --all-targets -- -D warnings

# fast check
check:
    cargo check --workspace --all-targets

# run tests for whole workspace
test:
    cargo test --workspace

# run agent locally with ./agent.env loaded (sourced by the shell: quote values with spaces or $&;)
run-agent:
    set -a && . ./agent.env && set +a && cargo run -p docktornet-agent

# run server locally with ./server.env loaded (sourced by the shell: quote values with spaces or $&;)
run-server:
    set -a && . ./server.env && set +a && cargo run -p docktornet-server
