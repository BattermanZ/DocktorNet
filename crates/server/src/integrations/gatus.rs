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
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<GatusAlert>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GatusAlert {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(rename = "send-on-resolved")]
    pub send_on_resolved: bool,
}

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn service_file_path(config_dir: impl AsRef<std::path::Path>, service_id: &str) -> PathBuf {
    config_dir.as_ref().join(format!(
        "{}--{:016x}.yaml",
        sanitize_service_id(service_id),
        stable_service_hash(service_id)
    ))
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
                group: monitor
                    .group
                    .clone()
                    .unwrap_or_else(|| service_id.to_string()),
                url: monitor.url.clone(),
                interval: monitor.interval.clone(),
                conditions: monitor.conditions.clone(),
                alerts: if monitor.telegram {
                    vec![GatusAlert {
                        kind: "telegram".to_string(),
                        send_on_resolved: true,
                    }]
                } else {
                    Vec::new()
                },
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

fn stable_service_hash(service_id: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001b3;

    service_id.as_bytes().iter().fold(FNV_OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
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
    fn renders_expected_yaml_with_default_group_and_telegram_alert() {
        let monitors = vec![GatusMonitor {
            index: 0,
            url: "https://app.example.com".to_string(),
            interval: "30s".to_string(),
            conditions: vec!["[STATUS] == 200".to_string()],
            group: None,
            telegram: true,
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
            "  alerts:\n",
            "  - type: telegram\n",
            "    send-on-resolved: true\n",
        );

        assert_eq!(yaml, expected);
    }

    #[test]
    fn renders_group_override_when_set() {
        let monitors = vec![GatusMonitor {
            index: 0,
            url: "https://app.example.com".to_string(),
            interval: "30s".to_string(),
            conditions: vec!["[STATUS] == 200".to_string()],
            group: Some("media".to_string()),
            telegram: true,
        }];

        let yaml = render_gatus_yaml("m1/app", &monitors).unwrap();
        assert!(yaml.contains("group: media\n"));
        assert!(!yaml.contains("group: m1/app\n"));
    }

    #[test]
    fn omits_alerts_block_when_telegram_is_false() {
        let monitors = vec![GatusMonitor {
            index: 0,
            url: "https://app.example.com".to_string(),
            interval: "30s".to_string(),
            conditions: vec!["[STATUS] == 200".to_string()],
            group: None,
            telegram: false,
        }];

        let yaml = render_gatus_yaml("m1/app", &monitors).unwrap();
        assert!(!yaml.contains("alerts"));
        assert!(!yaml.contains("telegram"));
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
            group: None,
            telegram: true,
        }];

        let path = service_file_path(dir.path(), service_id);
        assert_eq!(
            path,
            PathBuf::from(dir.path()).join(format!(
                "{}--{:016x}.yaml",
                sanitize_service_id(service_id),
                stable_service_hash(service_id)
            ))
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

    #[test]
    fn service_file_path_disambiguates_sanitized_collisions() {
        let dir = tempdir().unwrap();
        let hyphen = service_file_path(dir.path(), "m1/my-app");
        let underscore = service_file_path(dir.path(), "m1/my_app");
        let dot = service_file_path(dir.path(), "m1/my.app");

        assert_ne!(hyphen, underscore);
        assert_ne!(hyphen, dot);
        assert_ne!(underscore, dot);
        assert!(
            hyphen
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("m1__my_app--")
        );
        assert!(
            underscore
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("m1__my_app--")
        );
        assert!(
            dot.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("m1__my_app--")
        );
    }
}
