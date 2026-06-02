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

/// Merge results from every configured source. Duplicate container names from
/// different sockets are kept because the operator controls socket selection.
pub async fn list_all(sources: &[Box<dyn ContainerSource>]) -> Vec<RawContainer> {
    let mut out = Vec::new();

    for source in sources {
        match source.list().await {
            Ok(mut containers) => out.append(&mut containers),
            Err(error) => {
                warn!(source = %source.label(), %error, "source failed to list containers")
            }
        }
    }

    out
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

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].name, "b");
    }
}
