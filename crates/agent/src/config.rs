use std::collections::HashMap;
use std::time::Duration;

use docktornet_core::{MachineId, Secret, WeakSecret, validate_shared_secret};

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub server_url: String,
    pub secret: Secret,
    pub poll_interval: Duration,
    pub machine_id: MachineId,
    pub forward_host: String,
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
    #[error(transparent)]
    WeakSecret(#[from] WeakSecret),
}

impl AgentConfig {
    /// Load from the current process environment. Reads (in order):
    ///   DOCKTORNET_SERVER_URL, DOCKTORNET_SECRET, DOCKTORNET_POLL_INTERVAL,
    ///   DOCKTORNET_MACHINE_ID, DOCKTORNET_FORWARD_HOST, DOCKTORNET_SOCKETS
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_iter(std::env::vars().collect::<HashMap<_, _>>())
    }

    /// Same as `from_env` but reads from an explicit map (used by tests).
    pub fn from_iter(env: HashMap<String, String>) -> Result<Self, ConfigError> {
        let get = |k: &'static str| env.get(k).cloned().ok_or(ConfigError::Missing(k));

        let server_url = get("DOCKTORNET_SERVER_URL")?;
        let secret = get("DOCKTORNET_SECRET")?;
        validate_shared_secret(&secret)?;

        let poll_interval =
            humantime::parse_duration(&get("DOCKTORNET_POLL_INTERVAL")?).map_err(|e| {
                ConfigError::Invalid {
                    var: "DOCKTORNET_POLL_INTERVAL",
                    msg: e.to_string(),
                }
            })?;

        const MIN_POLL_INTERVAL: Duration = Duration::from_secs(1);
        if poll_interval < MIN_POLL_INTERVAL {
            return Err(ConfigError::Invalid {
                var: "DOCKTORNET_POLL_INTERVAL",
                msg: format!(
                    "must be at least {MIN_POLL_INTERVAL:?}, got {poll_interval:?}; \
                     a sub-second interval would busy-loop the Docker socket and server ingest"
                ),
            });
        }

        let machine_id =
            MachineId::new(get("DOCKTORNET_MACHINE_ID")?).map_err(|e| ConfigError::Invalid {
                var: "DOCKTORNET_MACHINE_ID",
                msg: e.to_string(),
            })?;

        let forward_host = get("DOCKTORNET_FORWARD_HOST")?;

        let sockets_raw = get("DOCKTORNET_SOCKETS")?;
        let sockets = parse_sockets(&sockets_raw).map_err(|msg| ConfigError::Invalid {
            var: "DOCKTORNET_SOCKETS",
            msg,
        })?;

        Ok(Self {
            server_url,
            secret: secret.into(),
            poll_interval,
            machine_id,
            forward_host,
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
        let path = path.trim();
        if path.is_empty() {
            return Err(format!(
                "socket path for runtime '{runtime}' cannot be empty"
            ));
        }
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

    const SECRET: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

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
            ("DOCKTORNET_SECRET", SECRET),
            ("DOCKTORNET_POLL_INTERVAL", "30s"),
            ("DOCKTORNET_MACHINE_ID", "batterdock"),
            ("DOCKTORNET_FORWARD_HOST", "10.0.0.10"),
            (
                "DOCKTORNET_SOCKETS",
                "docker:/var/run/docker.sock,podman:/run/user/1000/podman/podman.sock",
            ),
        ]))
        .unwrap();

        assert_eq!(c.server_url, "http://127.0.0.1:5363");
        assert_eq!(c.secret.expose(), SECRET);
        assert_eq!(c.poll_interval, Duration::from_secs(30));
        assert_eq!(c.machine_id.as_str(), "batterdock");
        assert_eq!(c.forward_host, "10.0.0.10");
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
    fn missing_forward_host_errors() {
        let err = AgentConfig::from_iter(env(&[
            ("DOCKTORNET_SERVER_URL", "u"),
            ("DOCKTORNET_SECRET", SECRET),
            ("DOCKTORNET_POLL_INTERVAL", "1s"),
            ("DOCKTORNET_MACHINE_ID", "m"),
            ("DOCKTORNET_SOCKETS", "docker:/x"),
        ]))
        .unwrap_err();

        match err {
            ConfigError::Missing(v) => assert_eq!(v, "DOCKTORNET_FORWARD_HOST"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn unknown_runtime_errors_with_sockets_var() {
        let err = AgentConfig::from_iter(env(&[
            ("DOCKTORNET_SERVER_URL", "u"),
            ("DOCKTORNET_SECRET", SECRET),
            ("DOCKTORNET_POLL_INTERVAL", "1s"),
            ("DOCKTORNET_MACHINE_ID", "m"),
            ("DOCKTORNET_FORWARD_HOST", "10.0.0.10"),
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
            ("DOCKTORNET_SECRET", SECRET),
            ("DOCKTORNET_POLL_INTERVAL", "1s"),
            ("DOCKTORNET_MACHINE_ID", "m"),
            ("DOCKTORNET_FORWARD_HOST", "10.0.0.10"),
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

    #[test]
    fn zero_poll_interval_errors_with_poll_interval_var() {
        for value in ["0s", "0", "1ms", "999ms"] {
            let err = AgentConfig::from_iter(env(&[
                ("DOCKTORNET_SERVER_URL", "u"),
                ("DOCKTORNET_SECRET", SECRET),
                ("DOCKTORNET_POLL_INTERVAL", value),
                ("DOCKTORNET_MACHINE_ID", "m"),
                ("DOCKTORNET_FORWARD_HOST", "10.0.0.10"),
                ("DOCKTORNET_SOCKETS", "docker:/x"),
            ]))
            .unwrap_err();

            assert!(
                matches!(
                    err,
                    ConfigError::Invalid {
                        var: "DOCKTORNET_POLL_INTERVAL",
                        ..
                    }
                ),
                "expected Invalid poll-interval error for {value:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn unparseable_poll_interval_errors_with_poll_interval_var() {
        for value in ["", "abc", "30", "5 parsecs", "-1s"] {
            let err = AgentConfig::from_iter(env(&[
                ("DOCKTORNET_SERVER_URL", "u"),
                ("DOCKTORNET_SECRET", SECRET),
                ("DOCKTORNET_POLL_INTERVAL", value),
                ("DOCKTORNET_MACHINE_ID", "m"),
                ("DOCKTORNET_FORWARD_HOST", "10.0.0.10"),
                ("DOCKTORNET_SOCKETS", "docker:/x"),
            ]))
            .unwrap_err();

            assert!(
                matches!(
                    err,
                    ConfigError::Invalid {
                        var: "DOCKTORNET_POLL_INTERVAL",
                        ..
                    }
                ),
                "expected Invalid poll-interval error for {value:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn empty_socket_path_errors_with_sockets_var() {
        let err = AgentConfig::from_iter(env(&[
            ("DOCKTORNET_SERVER_URL", "u"),
            ("DOCKTORNET_SECRET", SECRET),
            ("DOCKTORNET_POLL_INTERVAL", "1s"),
            ("DOCKTORNET_MACHINE_ID", "m"),
            ("DOCKTORNET_FORWARD_HOST", "10.0.0.10"),
            ("DOCKTORNET_SOCKETS", "docker:   "),
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

    #[test]
    fn invalid_machine_id_errors_with_machine_id_var() {
        for value in ["", "a/b", "a b", "a\u{1}b"] {
            let err = AgentConfig::from_iter(env(&[
                ("DOCKTORNET_SERVER_URL", "u"),
                ("DOCKTORNET_SECRET", SECRET),
                ("DOCKTORNET_POLL_INTERVAL", "1s"),
                ("DOCKTORNET_MACHINE_ID", value),
                ("DOCKTORNET_FORWARD_HOST", "10.0.0.10"),
                ("DOCKTORNET_SOCKETS", "docker:/x"),
            ]))
            .unwrap_err();

            assert!(
                err.to_string().contains("DOCKTORNET_MACHINE_ID"),
                "expected the error for {value:?} to name the variable, got {err}"
            );
            assert!(
                matches!(
                    err,
                    ConfigError::Invalid {
                        var: "DOCKTORNET_MACHINE_ID",
                        ..
                    }
                ),
                "expected Invalid machine-id error for {value:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn rejects_weak_secrets() {
        let changeme_40 = format!("{}CHANGEME{}", "a".repeat(16), "b".repeat(16));
        for secret in ["", "s", "changeme", "changeme-please", changeme_40.as_str()] {
            let err = AgentConfig::from_iter(env(&[
                ("DOCKTORNET_SERVER_URL", "u"),
                ("DOCKTORNET_SECRET", secret),
                ("DOCKTORNET_POLL_INTERVAL", "1s"),
                ("DOCKTORNET_MACHINE_ID", "m"),
                ("DOCKTORNET_FORWARD_HOST", "10.0.0.10"),
                ("DOCKTORNET_SOCKETS", "docker:/x"),
            ]))
            .unwrap_err();

            let message = err.to_string();
            assert!(
                message.contains("DOCKTORNET_SECRET") && message.contains("openssl rand -hex 32"),
                "expected a weak-secret error for {secret:?}, got {message}"
            );
        }
    }

    #[test]
    fn debug_output_redacts_the_secret() {
        let c = AgentConfig::from_iter(env(&[
            ("DOCKTORNET_SERVER_URL", "u"),
            ("DOCKTORNET_SECRET", SECRET),
            ("DOCKTORNET_POLL_INTERVAL", "1s"),
            ("DOCKTORNET_MACHINE_ID", "m"),
            ("DOCKTORNET_FORWARD_HOST", "10.0.0.10"),
            ("DOCKTORNET_SOCKETS", "docker:/x"),
        ]))
        .unwrap();

        for printed in [format!("{c:?}"), format!("{c:#?}")] {
            assert!(!printed.contains(SECRET), "secret leaked into {printed}");
        }
        assert!(format!("{c:?}").contains("10.0.0.10"));
    }
}
