use std::collections::HashMap;
use std::time::Duration;

use docktornet_core::MachineId;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub server_url: String,
    pub secret: String,
    pub poll_interval: Duration,
    pub machine_id: MachineId,
    pub sockets: Vec<SocketSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocketSpec {
    pub runtime: Runtime,
    pub path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Runtime {
    Docker,
    Podman,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing environment variable '{0}'")]
    Missing(&'static str),
    #[error("invalid value for {var}: {msg}")]
    Invalid { var: &'static str, msg: String },
}

#[derive(Debug, Deserialize)]
struct DurationStr(#[serde(with = "humantime_serde")] Duration);

impl AgentConfig {
    /// Load from the current process environment. Reads (in order):
    ///   DOCKTORNET_SERVER_URL, DOCKTORNET_SECRET, DOCKTORNET_POLL_INTERVAL,
    ///   DOCKTORNET_MACHINE_ID, DOCKTORNET_SOCKETS
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_iter(std::env::vars().collect::<HashMap<_, _>>())
    }

    /// Same as `from_env` but reads from an explicit map (used by tests).
    pub fn from_iter(env: HashMap<String, String>) -> Result<Self, ConfigError> {
        let get = |k: &'static str| env.get(k).cloned().ok_or(ConfigError::Missing(k));

        let server_url = get("DOCKTORNET_SERVER_URL")?;
        let secret = get("DOCKTORNET_SECRET")?;

        let interval_str = get("DOCKTORNET_POLL_INTERVAL")?;
        let DurationStr(poll_interval) = serde_json::from_value(serde_json::Value::String(
            interval_str.clone(),
        ))
        .map_err(|e| ConfigError::Invalid {
            var: "DOCKTORNET_POLL_INTERVAL",
            msg: e.to_string(),
        })?;

        let machine_id = MachineId::new(get("DOCKTORNET_MACHINE_ID")?);

        let sockets_raw = get("DOCKTORNET_SOCKETS")?;
        let sockets = parse_sockets(&sockets_raw).map_err(|msg| ConfigError::Invalid {
            var: "DOCKTORNET_SOCKETS",
            msg,
        })?;

        Ok(Self {
            server_url,
            secret,
            poll_interval,
            machine_id,
            sockets,
        })
    }
}

fn parse_sockets(raw: &str) -> Result<Vec<SocketSpec>, String> {
    let mut out = Vec::new();

    for item in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (runtime, path) = item
            .split_once(':')
            .ok_or_else(|| format!("expected 'runtime:path', got '{item}'"))?;
        let runtime = match runtime {
            "docker" => Runtime::Docker,
            "podman" => Runtime::Podman,
            other => return Err(format!("unknown runtime '{other}', expected docker|podman")),
        };

        out.push(SocketSpec {
            runtime,
            path: path.to_string(),
        });
    }

    if out.is_empty() {
        return Err("at least one socket is required".into());
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn happy_path_loads_all_fields() {
        let c = AgentConfig::from_iter(env(&[
            ("DOCKTORNET_SERVER_URL", "http://127.0.0.1:5363"),
            ("DOCKTORNET_SECRET", "s3cret"),
            ("DOCKTORNET_POLL_INTERVAL", "30s"),
            ("DOCKTORNET_MACHINE_ID", "batterdock"),
            (
                "DOCKTORNET_SOCKETS",
                "docker:/var/run/docker.sock,podman:/run/user/1000/podman/podman.sock",
            ),
        ]))
        .unwrap();

        assert_eq!(c.server_url, "http://127.0.0.1:5363");
        assert_eq!(c.secret, "s3cret");
        assert_eq!(c.poll_interval, Duration::from_secs(30));
        assert_eq!(c.machine_id.as_str(), "batterdock");
        assert_eq!(c.sockets.len(), 2);
        assert_eq!(
            c.sockets[0],
            SocketSpec {
                runtime: Runtime::Docker,
                path: "/var/run/docker.sock".into(),
            }
        );
        assert_eq!(
            c.sockets[1],
            SocketSpec {
                runtime: Runtime::Podman,
                path: "/run/user/1000/podman/podman.sock".into(),
            }
        );
    }

    #[test]
    fn missing_var_errors_clearly_on_server_url() {
        let err = AgentConfig::from_iter(env(&[])).unwrap_err();

        match err {
            ConfigError::Missing(v) => assert_eq!(v, "DOCKTORNET_SERVER_URL"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn unknown_runtime_errors_with_sockets_var() {
        let err = AgentConfig::from_iter(env(&[
            ("DOCKTORNET_SERVER_URL", "u"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_POLL_INTERVAL", "1s"),
            ("DOCKTORNET_MACHINE_ID", "m"),
            ("DOCKTORNET_SOCKETS", "wasm:/x"),
        ]))
        .unwrap_err();

        match err {
            ConfigError::Invalid { var, .. } => assert_eq!(var, "DOCKTORNET_SOCKETS"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn empty_sockets_errors_with_sockets_var() {
        let err = AgentConfig::from_iter(env(&[
            ("DOCKTORNET_SERVER_URL", "u"),
            ("DOCKTORNET_SECRET", "s"),
            ("DOCKTORNET_POLL_INTERVAL", "1s"),
            ("DOCKTORNET_MACHINE_ID", "m"),
            ("DOCKTORNET_SOCKETS", ""),
        ]))
        .unwrap_err();

        assert!(matches!(
            err,
            ConfigError::Invalid {
                var: "DOCKTORNET_SOCKETS",
                ..
            }
        ));
    }
}
