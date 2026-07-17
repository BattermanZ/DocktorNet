use std::collections::BTreeMap;

use async_trait::async_trait;
use bollard::Docker;
use bollard::container::ListContainersOptions;
use tracing::warn;

use crate::config::SocketSpec;

/// A container as it appears on the wire. Names come from the Docker API
/// stripped of the leading `/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawContainer {
    pub name: String,
    pub labels: BTreeMap<String, String>,
}

#[async_trait]
pub trait ContainerSource: Send + Sync {
    async fn list(&self) -> anyhow::Result<Vec<RawContainer>>;

    /// Human-readable identifier (e.g. `Podman:/run/user/1001/podman/podman.sock`)
    /// used in logs so a failing source can be told apart from the others.
    fn label(&self) -> &str;
}

/// Production implementation using bollard against a Unix socket.
pub struct BollardSource {
    docker: Docker,
    label: String,
}

impl BollardSource {
    pub fn connect(spec: &SocketSpec) -> anyhow::Result<Self> {
        let docker = Docker::connect_with_socket(&spec.path, 120, bollard::API_DEFAULT_VERSION)?;
        Ok(Self {
            docker,
            label: format!("{:?}:{}", spec.runtime, spec.path),
        })
    }
}

#[async_trait]
impl ContainerSource for BollardSource {
    async fn list(&self) -> anyhow::Result<Vec<RawContainer>> {
        let opts = ListContainersOptions::<String> {
            all: false,
            ..Default::default()
        };
        let containers = self.docker.list_containers(Some(opts)).await?;

        let mut out = Vec::with_capacity(containers.len());
        for container in containers {
            let Some(names) = container.names else {
                warn!(source = %self.label, "container with no names, skipping");
                continue;
            };
            let Some(first) = names.into_iter().next() else {
                warn!(source = %self.label, "container with no names, skipping");
                continue;
            };
            let name = first.trim_start_matches('/').to_string();
            let labels = container.labels.unwrap_or_default().into_iter().collect();
            out.push(RawContainer { name, labels });
        }

        Ok(out)
    }

    fn label(&self) -> &str {
        &self.label
    }
}

/// Outcome of enumerating every configured source.
///
/// A source returning `Err` (socket missing, permission denied, daemon
/// restarting, timeout) must never be silently encoded as "zero containers on
/// that socket": doing so makes the merged list a *partial* view that the
/// server would treat as the authoritative complete state for the machine and
/// tear down healthy services. `failed_sources` carries that signal to the
/// caller so a degraded tick can be skipped instead of pushed.
#[derive(Debug, Default)]
pub struct ListOutcome {
    /// Containers merged from every source that enumerated successfully.
    pub containers: Vec<RawContainer>,
    /// Labels of the sources that failed to enumerate this tick.
    pub failed_sources: Vec<String>,
}

impl ListOutcome {
    /// True when at least one configured source failed to enumerate, meaning
    /// `containers` is a partial view and must not be treated as authoritative.
    pub fn any_failed(&self) -> bool {
        !self.failed_sources.is_empty()
    }
}

/// Merge results from every configured source. Duplicate container names from
/// different sockets are kept because the operator controls socket selection.
///
/// Sources that fail to enumerate are recorded in [`ListOutcome::failed_sources`]
/// rather than being dropped, so the caller can tell a partial view apart from a
/// genuinely empty machine.
pub async fn list_all(sources: &[Box<dyn ContainerSource>]) -> ListOutcome {
    let mut outcome = ListOutcome::default();

    for source in sources {
        match source.list().await {
            Ok(mut containers) => outcome.containers.append(&mut containers),
            Err(error) => {
                warn!(source = %source.label(), %error, "source failed to list containers");
                outcome.failed_sources.push(source.label().to_string());
            }
        }
    }

    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake {
        items: Vec<RawContainer>,
    }

    #[async_trait]
    impl ContainerSource for Fake {
        async fn list(&self) -> anyhow::Result<Vec<RawContainer>> {
            Ok(self.items.clone())
        }

        fn label(&self) -> &str {
            "Fake"
        }
    }

    fn rc(name: &str, labels: &[(&str, &str)]) -> RawContainer {
        RawContainer {
            name: name.into(),
            labels: labels
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        }
    }

    #[tokio::test]
    async fn list_all_concatenates_sources_in_order() {
        let a: Box<dyn ContainerSource> = Box::new(Fake {
            items: vec![rc("a", &[])],
        });
        let b: Box<dyn ContainerSource> = Box::new(Fake {
            items: vec![rc("b", &[])],
        });

        let merged = list_all(&[a, b]).await;

        assert_eq!(
            merged
                .containers
                .iter()
                .map(|container| container.name.clone())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    struct Boom;

    #[async_trait]
    impl ContainerSource for Boom {
        async fn list(&self) -> anyhow::Result<Vec<RawContainer>> {
            anyhow::bail!("boom")
        }

        fn label(&self) -> &str {
            "Boom"
        }
    }

    #[tokio::test]
    async fn list_all_continues_when_one_source_fails() {
        let a: Box<dyn ContainerSource> = Box::new(Boom);
        let b: Box<dyn ContainerSource> = Box::new(Fake {
            items: vec![rc("b", &[])],
        });

        let merged = list_all(&[a, b]).await;

        assert_eq!(merged.containers.len(), 1);
        assert_eq!(merged.containers[0].name, "b");
    }

    #[tokio::test]
    async fn list_all_reports_which_sources_failed() {
        let a: Box<dyn ContainerSource> = Box::new(Boom);
        let b: Box<dyn ContainerSource> = Box::new(Fake {
            items: vec![rc("b", &[])],
        });

        let outcome = list_all(&[a, b]).await;

        // The surviving source's container is still enumerated...
        assert_eq!(outcome.containers.len(), 1);
        // ...but the failure MUST be surfaced so the caller can refuse to treat
        // this partial list as the authoritative machine state.
        assert!(
            outcome.any_failed(),
            "a failing source must be reported, not swallowed"
        );
        assert_eq!(outcome.failed_sources, vec!["Boom".to_string()]);
    }

    #[tokio::test]
    async fn list_all_reports_no_failure_when_all_sources_succeed() {
        let a: Box<dyn ContainerSource> = Box::new(Fake {
            items: vec![rc("a", &[])],
        });
        let b: Box<dyn ContainerSource> = Box::new(Fake {
            items: vec![rc("b", &[])],
        });

        let outcome = list_all(&[a, b]).await;

        assert!(!outcome.any_failed());
        assert!(outcome.failed_sources.is_empty());
    }
}
