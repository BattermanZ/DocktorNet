//! `docktornet-server healthcheck`: probe a running server's `/healthz` for a
//! container healthcheck.
//!
//! The server image is distroless, so it has no shell or curl to probe with.
//! This needs only `DOCKTORNET_LISTEN`, so no other setting can fail it. It
//! checks liveness alone.  It sends no token and ignores `status`, so an NPM
//! outage never marks the container unhealthy.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use anyhow::Context;

use crate::audit::describe;

/// How long the probe waits for the whole answer.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// The `healthcheck` subcommand: return 0 when `/healthz` answers 200, and 1
/// on anything else.
pub async fn cli() -> i32 {
    match probe().await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("docktornet-server healthcheck: {}", describe(&e));
            1
        }
    }
}

async fn probe() -> anyhow::Result<()> {
    let listen = std::env::var("DOCKTORNET_LISTEN").context("read DOCKTORNET_LISTEN")?;
    let url = healthz_url(&listen);
    let client = reqwest::Client::builder()
        .timeout(PROBE_TIMEOUT)
        .no_proxy()
        .build()
        .context("build the HTTP client")?;
    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    anyhow::ensure!(
        response.status() == reqwest::StatusCode::OK,
        "GET {url} answered {}",
        response.status()
    );
    Ok(())
}

/// The `/healthz` URL on the address the server listens on.  An unspecified
/// bind address (`0.0.0.0` or `::`) becomes loopback of the same family; a
/// specific address or a host name is used as is.
fn healthz_url(listen: &str) -> String {
    match listen.parse::<SocketAddr>() {
        Ok(mut addr) => {
            if addr.ip().is_unspecified() {
                addr.set_ip(match addr.ip() {
                    IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
                    IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
                });
            }
            format!("http://{addr}/healthz")
        }
        Err(_) => format!("http://{listen}/healthz"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unspecified_binds_are_probed_on_loopback() {
        assert_eq!(healthz_url("0.0.0.0:5363"), "http://127.0.0.1:5363/healthz");
        assert_eq!(healthz_url("[::]:5363"), "http://[::1]:5363/healthz");
    }

    #[test]
    fn specific_addresses_and_host_names_are_kept() {
        assert_eq!(
            healthz_url("192.168.1.10:5363"),
            "http://192.168.1.10:5363/healthz"
        );
        assert_eq!(healthz_url("[::1]:5363"), "http://[::1]:5363/healthz");
        assert_eq!(
            healthz_url("localhost:5363"),
            "http://localhost:5363/healthz"
        );
    }
}
