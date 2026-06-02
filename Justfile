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

# run agent locally (requires .env in CWD)
run-agent:
    cargo run -p docktornet-agent

# run server locally (requires .env in CWD)
run-server:
    cargo run -p docktornet-server
