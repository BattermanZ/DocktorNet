use std::path::PathBuf;

use docktornet_core::{labels::GatusMonitor, sanitize_service_id};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GatusFile {
    pub endpoints: Vec<GatusEndpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GatusEndpoint {
    pub name: String,
    pub group: String,
    pub url: String,
    pub interval: String,
    pub conditions: Vec<String>,
}

pub fn service_file_path(config_dir: impl AsRef<std::path::Path>, service_id: &str) -> PathBuf {
    config_dir
        .as_ref()
        .join(format!("{}.yaml", sanitize_service_id(service_id)))
}

pub fn render_gatus_yaml(
    service_id: &str,
    monitors: &[GatusMonitor],
) -> Result<String, serde_yaml::Error> {
    let file = GatusFile {
        endpoints: monitors
            .iter()
            .map(|monitor| GatusEndpoint {
                name: format!("{service_id}/{}", monitor.index),
                group: service_id.to_string(),
                url: monitor.url.clone(),
                interval: monitor.interval.clone(),
                conditions: monitor.conditions.clone(),
            })
            .collect(),
    };

    serde_yaml::to_string(&file)
}

pub async fn write_service_file(
    config_dir: impl AsRef<std::path::Path>,
    service_id: &str,
    monitors: &[GatusMonitor],
) -> anyhow::Result<PathBuf> {
    let target = service_file_path(config_dir, service_id);
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let tmp = target.with_extension("yaml.tmp");
    let yaml = render_gatus_yaml(service_id, monitors)?;
    tokio::fs::write(&tmp, yaml).await?;
    tokio::fs::rename(&tmp, &target).await?;

    Ok(target)
}

pub async fn remove_service_file(
    config_dir: impl AsRef<std::path::Path>,
    service_id: &str,
) -> anyhow::Result<()> {
    let target = service_file_path(config_dir, service_id);
    match tokio::fs::remove_file(&target).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn renders_expected_yaml() {
        let monitors = vec![GatusMonitor {
            index: 0,
            url: "https://app.example.com".to_string(),
            interval: "30s".to_string(),
            conditions: vec!["[STATUS] == 200".to_string()],
        }];

        let yaml = render_gatus_yaml("m1/app", &monitors).unwrap();

        assert!(yaml.contains("endpoints:"));
        assert!(yaml.contains("name: m1/app/0"));
        assert!(yaml.contains("url: https://app.example.com"));
    }

    #[tokio::test]
    async fn writes_and_removes_service_file() {
        let dir = tempdir().unwrap();
        let service_id = "m1/app";
        let monitors = vec![GatusMonitor {
            index: 0,
            url: "https://app.example.com".to_string(),
            interval: "30s".to_string(),
            conditions: vec!["[STATUS] == 200".to_string()],
        }];

        let path = service_file_path(dir.path(), service_id);
        assert_eq!(
            path,
            PathBuf::from(dir.path()).join(format!("{}.yaml", sanitize_service_id(service_id)))
        );

        let written = write_service_file(dir.path(), service_id, &monitors)
            .await
            .unwrap();
        assert_eq!(written, path);
        assert!(written.exists());

        remove_service_file(dir.path(), service_id).await.unwrap();
        assert!(!written.exists());
    }
}
