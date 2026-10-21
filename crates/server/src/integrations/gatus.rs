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
    // service_id is `{machine}/{container}`; fall back to the whole id if there
    // is no slash. The container half is the default endpoint name; the machine
    // half is the default group.
    let (machine, container) = service_id
        .split_once('/')
        .unwrap_or((service_id, service_id));

    // Pass 1: resolve each monitor's display name (explicit name label, else the
    // bare stack/container name).
    let mut names: Vec<String> = monitors
        .iter()
        .map(|monitor| {
            monitor
                .name
                .clone()
                .unwrap_or_else(|| container.to_string())
        })
        .collect();

    // Pass 2: any name shared by 2+ monitors gets " {index}" appended so endpoint
    // names stay unique within the file. The raw monitor index keeps the suffix
    // stable across restarts and label reordering.
    let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for name in &names {
        *counts.entry(name.clone()).or_insert(0) += 1;
    }
    for (monitor, name) in monitors.iter().zip(names.iter_mut()) {
        if counts.get(name.as_str()).copied().unwrap_or(0) > 1 {
            *name = format!("{name} {}", monitor.index);
        }
    }

    let file = GatusFile {
        endpoints: monitors
            .iter()
            .zip(names)
            .map(|(monitor, name)| GatusEndpoint {
                name,
                group: monitor.group.clone().unwrap_or_else(|| machine.to_string()),
                url: monitor.url.clone(),
                interval: monitor.interval.clone(),
                conditions: monitor.conditions.clone(),
                alerts: if monitor.telegram {
                    vec![GatusAlert {
                        kind: "custom".to_string(),
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

/// Returns true if the path looks like a file written by `write_service_file`.
/// The naming pattern is `{sanitized_id}--{16 lowercase hex chars}.yaml`.
/// Files that do not match (e.g. `00-base.yaml`) are left untouched.
pub fn is_managed_service_file(path: &std::path::Path) -> bool {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    let Some(stem) = name.strip_suffix(".yaml") else {
        return false;
    };
    if let Some(pos) = stem.rfind("--") {
        let hash = &stem[pos + 2..];
        hash.len() == 16 && hash.bytes().all(|b| b.is_ascii_hexdigit())
    } else {
        false
    }
}

/// Removes a service's file.  Returns whether a file was deleted, so the caller
/// knows to [`touch_reload_trigger`].
pub async fn remove_service_file(
    config_dir: impl AsRef<std::path::Path>,
    service_id: &str,
) -> anyhow::Result<bool> {
    let target = service_file_path(config_dir, service_id);
    match tokio::fs::remove_file(&target).await {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err.into()),
    }
}

/// A docktornet-owned file in the Gatus config directory whose only job is to
/// make Gatus reload.  Its name has no `--{hash}` suffix, so the orphan sweep
/// never treats it as a service file.
pub fn reload_trigger_path(config_dir: impl AsRef<std::path::Path>) -> PathBuf {
    config_dir.as_ref().join("docktornet-reload.yaml")
}

/// Gatus deep-merges every `.yaml` in the directory, so the trigger is an empty
/// mapping: it parses and adds nothing.
const RELOAD_TRIGGER_YAML: &str =
    "# Rewritten by docktornet after it deletes a service file, so Gatus reloads.\n{}\n";

/// Make Gatus reload after service files were deleted.  Gatus reloads when a
/// `.yaml` file's mtime, in whole seconds, is newer than its last load, and a
/// deletion leaves no such file.  The mtime is set one second ahead so a touch
/// in the same second as Gatus's last load still counts; Gatus sleeps a second
/// inside its reload, so the next load is never older than the trigger.
pub async fn touch_reload_trigger(config_dir: impl AsRef<std::path::Path>) -> anyhow::Result<()> {
    let target = reload_trigger_path(config_dir);
    let tmp = service_temp_file_path(&target);
    tokio::fs::write(&tmp, RELOAD_TRIGGER_YAML).await?;
    tokio::fs::OpenOptions::new()
        .write(true)
        .open(&tmp)
        .await?
        .into_std()
        .await
        .set_modified(SystemTime::now() + std::time::Duration::from_secs(1))?;
    tokio::fs::rename(&tmp, &target).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn renders_expected_yaml_with_host_group_and_custom_alert() {
        let monitors = vec![GatusMonitor {
            index: 0,
            url: "https://app.example.com".to_string(),
            interval: "30s".to_string(),
            conditions: vec!["[STATUS] == 200".to_string()],
            group: None,
            name: None,
            telegram: true,
        }];

        let yaml = render_gatus_yaml("m1/app", &monitors).unwrap();
        let expected = concat!(
            "endpoints:\n",
            "- name: app\n",
            "  group: m1\n",
            "  url: https://app.example.com\n",
            "  interval: 30s\n",
            "  conditions:\n",
            "  - '[STATUS] == 200'\n",
            "  alerts:\n",
            "  - type: custom\n",
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
            name: None,
            telegram: true,
        }];

        let yaml = render_gatus_yaml("m1/app", &monitors).unwrap();
        assert!(yaml.contains("group: media\n"));
        assert!(!yaml.contains("group: m1\n"));
    }

    #[test]
    fn omits_alerts_block_when_telegram_is_false() {
        let monitors = vec![GatusMonitor {
            index: 0,
            url: "https://app.example.com".to_string(),
            interval: "30s".to_string(),
            conditions: vec!["[STATUS] == 200".to_string()],
            group: None,
            name: None,
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
            name: None,
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
    fn reload_trigger_is_not_a_managed_service_file() {
        let dir = tempdir().unwrap();
        assert!(!is_managed_service_file(&reload_trigger_path(dir.path())));
    }

    #[tokio::test]
    async fn reload_trigger_lands_in_a_later_second_than_the_touch() {
        // Gatus reloads only when a file's whole-second mtime is newer than its
        // last load, so a touch in the same second as that load must still count.
        let dir = tempdir().unwrap();
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        touch_reload_trigger(dir.path()).await.unwrap();

        let mtime = std::fs::metadata(reload_trigger_path(dir.path()))
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(
            mtime > before,
            "mtime {mtime} must be after second {before}"
        );
    }

    #[tokio::test]
    async fn reload_trigger_merges_into_gatus_config_as_a_no_op() {
        // Gatus deep-merges every .yaml in the directory; the trigger must parse
        // and contribute no keys.
        let dir = tempdir().unwrap();
        touch_reload_trigger(dir.path()).await.unwrap();

        let body = std::fs::read_to_string(reload_trigger_path(dir.path())).unwrap();
        let parsed: serde_yaml::Mapping = serde_yaml::from_str(&body).unwrap();
        assert!(parsed.is_empty());
        // No temp file is left behind.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn is_managed_service_file_matches_docktornet_filenames() {
        let dir = tempdir().unwrap();
        let managed = service_file_path(dir.path(), "m1/app");
        assert!(is_managed_service_file(&managed));
    }

    #[test]
    fn is_managed_service_file_rejects_base_config() {
        let path = std::path::Path::new("/gatus/config/00-base.yaml");
        assert!(!is_managed_service_file(path));
    }

    #[test]
    fn is_managed_service_file_rejects_short_hash() {
        let path = std::path::Path::new("/gatus/config/m1__app--abc.yaml");
        assert!(!is_managed_service_file(path));
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
    fn uses_name_label_verbatim_when_set() {
        let monitors = vec![GatusMonitor {
            index: 0,
            url: "https://app.example.com".to_string(),
            interval: "30s".to_string(),
            conditions: vec!["[STATUS] == 200".to_string()],
            group: None,
            name: Some("Homepage".to_string()),
            telegram: true,
        }];

        let yaml = render_gatus_yaml("m1/app", &monitors).unwrap();
        assert!(yaml.contains("- name: Homepage\n"));
    }

    #[test]
    fn disambiguates_colliding_default_names_with_index_suffix() {
        let monitors = vec![
            GatusMonitor {
                index: 0,
                url: "https://lan.example.com".to_string(),
                interval: "30s".to_string(),
                conditions: vec!["[STATUS] == 200".to_string()],
                group: None,
                name: None,
                telegram: true,
            },
            GatusMonitor {
                index: 1,
                url: "https://public.example.com".to_string(),
                interval: "30s".to_string(),
                conditions: vec!["[STATUS] == 200".to_string()],
                group: None,
                name: None,
                telegram: true,
            },
        ];

        let yaml = render_gatus_yaml("m1/app", &monitors).unwrap();
        assert!(yaml.contains("- name: app 0\n"));
        assert!(yaml.contains("- name: app 1\n"));
        assert!(!yaml.contains("- name: app\n"));
    }

    #[test]
    fn single_monitor_has_no_index_suffix() {
        let monitors = vec![GatusMonitor {
            index: 0,
            url: "https://app.example.com".to_string(),
            interval: "30s".to_string(),
            conditions: vec!["[STATUS] == 200".to_string()],
            group: None,
            name: None,
            telegram: true,
        }];

        let yaml = render_gatus_yaml("m1/app", &monitors).unwrap();
        assert!(yaml.contains("- name: app\n"));
        assert!(!yaml.contains("app 0"));
    }

    #[test]
    fn disambiguates_identical_explicit_names() {
        let monitors = vec![
            GatusMonitor {
                index: 0,
                url: "https://a.example.com".to_string(),
                interval: "30s".to_string(),
                conditions: vec!["[STATUS] == 200".to_string()],
                group: None,
                name: Some("web".to_string()),
                telegram: true,
            },
            GatusMonitor {
                index: 1,
                url: "https://b.example.com".to_string(),
                interval: "30s".to_string(),
                conditions: vec!["[STATUS] == 200".to_string()],
                group: None,
                name: Some("web".to_string()),
                telegram: true,
            },
        ];

        let yaml = render_gatus_yaml("m1/app", &monitors).unwrap();
        assert!(yaml.contains("- name: web 0\n"));
        assert!(yaml.contains("- name: web 1\n"));
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
