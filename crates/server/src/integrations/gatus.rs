use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

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

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

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

fn service_temp_file_path(target: &std::path::Path) -> PathBuf {
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let pid = std::process::id();
    let file_name = target
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "gatus.yaml".into());

    target.with_file_name(format!("{file_name}.tmp-{pid}-{nanos}-{counter}"))
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

    let tmp = service_temp_file_path(&target);
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
        let expected = concat!(
            "endpoints:\n",
            "- name: m1/app/0\n",
            "  group: m1/app\n",
            "  url: https://app.example.com\n",
            "  interval: 30s\n",
            "  conditions:\n",
            "  - '[STATUS] == 200'\n",
        );

        assert_eq!(yaml, expected);
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

    #[tokio::test]
    async fn removing_missing_service_file_is_idempotent() {
        let dir = tempdir().unwrap();

        remove_service_file(dir.path(), "m1/app").await.unwrap();
        remove_service_file(dir.path(), "m1/app").await.unwrap();
    }

    #[test]
    fn temp_service_file_path_is_unique_per_call() {
        let dir = tempdir().unwrap();
        let target = service_file_path(dir.path(), "m1/app");
        let first = service_temp_file_path(&target);
        let second = service_temp_file_path(&target);

        assert_eq!(first.parent(), target.parent());
        assert_eq!(second.parent(), target.parent());
        assert_ne!(first, second);
        assert!(first.to_string_lossy().contains(".yaml.tmp-"));
        assert!(second.to_string_lossy().contains(".yaml.tmp-"));
    }
}
