//! The server binary's argument handling, run as a real process.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::tempdir;

const BIN: &str = env!("CARGO_BIN_EXE_docktornet-server");

/// Wait for `child` to exit, killing it after `limit`.
fn wait_with_limit(child: &mut std::process::Child, limit: Duration) -> Option<i32> {
    let start = Instant::now();
    while start.elapsed() < limit {
        if let Some(status) = child.try_wait().unwrap() {
            return status.code();
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    child.kill().ok();
    child.wait().ok();
    None
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn an_unknown_argument_exits_2_with_usage() {
    let dir = tempdir().unwrap();
    let mut child = Command::new(BIN)
        .arg("bogus")
        .current_dir(dir.path())
        .env_clear()
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let code = wait_with_limit(&mut child, Duration::from_secs(10));
    let output = child.wait_with_output().unwrap();
    assert_eq!(code, Some(2));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("usage: docktornet-server"));
}

#[test]
fn a_non_utf8_argument_exits_2_with_usage() {
    use std::os::unix::ffi::OsStrExt;

    let dir = tempdir().unwrap();
    let mut child = Command::new(BIN)
        .arg(std::ffi::OsStr::from_bytes(b"audit\xff"))
        .current_dir(dir.path())
        .env_clear()
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let code = wait_with_limit(&mut child, Duration::from_secs(10));
    let output = child.wait_with_output().unwrap();
    assert_eq!(code, Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("usage: docktornet-server"));
}

#[test]
fn audit_without_config_exits_2_with_nothing_on_stdout() {
    let dir = tempdir().unwrap();
    let mut child = Command::new(BIN)
        .arg("audit")
        .current_dir(dir.path())
        .env_clear()
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let code = wait_with_limit(&mut child, Duration::from_secs(10));
    let output = child.wait_with_output().unwrap();
    assert_eq!(code, Some(2));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("DOCKTORNET_LISTEN"));
}

#[test]
fn no_argument_starts_the_server() {
    let dir = tempdir().unwrap();
    let port = free_port();
    let mut child = Command::new(BIN)
        .current_dir(dir.path())
        .env_clear()
        .env("DOCKTORNET_LISTEN", format!("127.0.0.1:{port}"))
        .env("DOCKTORNET_SECRET", "0123456789abcdef".repeat(4))
        .env("DOCKTORNET_DB_PATH", dir.path().join("server.db"))
        .env("DOCKTORNET_ALLOW_EPHEMERAL_DB", "true")
        .env("DOCKTORNET_DRY_RUN", "true")
        .env("DOCKTORNET_GATUS_CONFIG_DIR", dir.path().join("gatus"))
        .env("DOCKTORNET_NPM_URL", "http://127.0.0.1:9")
        .env("DOCKTORNET_NPM_USER", "u")
        .env("DOCKTORNET_NPM_PASSWORD", "p")
        .env("DOCKTORNET_CF_API_URL", "http://127.0.0.1:9/client/v4")
        .env("DOCKTORNET_CF_API_TOKEN", "t")
        .env("DOCKTORNET_CF_ZONE_ID", "z")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let start = Instant::now();
    let mut listening = false;
    while start.elapsed() < Duration::from_secs(15) {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            listening = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    child.kill().ok();
    child.wait().ok();
    assert!(listening, "the server never listened on {port}");
}

#[test]
fn audit_only_caddy_without_caddy_configured_exits_2() {
    let dir = tempdir().unwrap();
    let mut child = Command::new(BIN)
        .args(["audit", "--only", "caddy"])
        .current_dir(dir.path())
        .env_clear()
        .env("DOCKTORNET_LISTEN", "127.0.0.1:0")
        .env("DOCKTORNET_SECRET", "0123456789abcdef".repeat(4))
        .env("DOCKTORNET_DB_PATH", dir.path().join("server.db"))
        .env("DOCKTORNET_GATUS_CONFIG_DIR", dir.path().join("gatus"))
        .env("DOCKTORNET_NPM_URL", "http://127.0.0.1:9")
        .env("DOCKTORNET_NPM_USER", "u")
        .env("DOCKTORNET_NPM_PASSWORD", "p")
        .env("DOCKTORNET_CF_API_URL", "http://127.0.0.1:9/client/v4")
        .env("DOCKTORNET_CF_API_TOKEN", "t")
        .env("DOCKTORNET_CF_ZONE_ID", "z")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let code = wait_with_limit(&mut child, Duration::from_secs(10));
    let output = child.wait_with_output().unwrap();
    assert_eq!(code, Some(2));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("DOCKTORNET_CADDY_ADMIN_URL"), "{stderr}");
    assert!(!dir.path().join("server.db").exists());
}
