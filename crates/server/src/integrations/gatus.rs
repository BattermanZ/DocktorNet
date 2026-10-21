use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use docktornet_core::{
    labels::{Area, GatusMonitor, Severity},
    sanitize_service_id, stable_service_hash,
};
use serde::{Deserialize, Serialize};

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

/// `service_id` is `{machine}/{container}`; fall back to the whole id if there
/// is no slash.
fn split_service_id(service_id: &str) -> (&str, &str) {
    service_id
        .split_once('/')
        .unwrap_or((service_id, service_id))
}

/// The Gatus endpoints for one service, in monitor order.
fn gatus_endpoints(service_id: &str, monitors: &[GatusMonitor]) -> Vec<GatusEndpoint> {
    // The container half is the default endpoint name; the machine half is the
    // default group.
    let (machine, container) = split_service_id(service_id);

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

    monitors
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
        .collect()
}

pub fn render_gatus_yaml(
    service_id: &str,
    monitors: &[GatusMonitor],
) -> Result<String, yaml_serde::Error> {
    yaml_serde::to_string(&GatusFile {
        endpoints: gatus_endpoints(service_id, monitors),
    })
}

/// Gatus's endpoint key, as `ConvertGroupAndEndpointNameToKey` in
/// `config/endpoint/key.go` builds it.  Gatus names an endpoint by this key in
/// its API and alert state.
pub fn gatus_endpoint_key(group: &str, name: &str) -> String {
    fn sanitize(s: &str) -> String {
        // Lowercase one character at a time, as Go's strings.ToLower does.
        s.trim()
            .chars()
            .map(|c| c.to_lowercase().next().unwrap_or(c))
            .map(|c| match c {
                '/' | '_' | '.' | ',' | ' ' | '#' => '-',
                c => c,
            })
            .collect()
    }
    format!("{}_{}", sanitize(group), sanitize(name))
}

/// One Gatus endpoint's entry in the classification map, as stored for its
/// service.  The alert thresholds are not stored: they come from the base
/// config when the map is written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassifiedEndpoint {
    pub key: String,
    pub name: String,
    pub group: String,
    pub service_id: String,
    pub host: String,
    pub severity: Severity,
    pub severity_defaulted: bool,
    pub area: Area,
    pub area_defaulted: bool,
    /// Whether the endpoint carries Docktornet's `custom` alert.
    pub has_alert: bool,
}

/// The classification map entries for one service's Gatus endpoints.
pub fn classify_endpoints(service_id: &str, monitors: &[GatusMonitor]) -> Vec<ClassifiedEndpoint> {
    // The reporting agent's machine id, never the group label.
    let host = split_service_id(service_id).0.to_lowercase();
    gatus_endpoints(service_id, monitors)
        .into_iter()
        .zip(monitors)
        .map(|(endpoint, monitor)| ClassifiedEndpoint {
            key: gatus_endpoint_key(&endpoint.group, &endpoint.name),
            service_id: service_id.to_string(),
            host: host.clone(),
            severity: monitor.severity.unwrap_or(Severity::DEFAULT),
            severity_defaulted: monitor.severity.is_none(),
            area: monitor.area.unwrap_or(Area::DEFAULT),
            area_defaulted: monitor.area.is_none(),
            has_alert: !endpoint.alerts.is_empty(),
            name: endpoint.name,
            group: endpoint.group,
        })
        .collect()
}

/// The failure and success thresholds Gatus applies to an alert.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AlertThresholds {
    pub failure_threshold: i64,
    pub success_threshold: i64,
}

/// Gatus's own defaults, from `Alert.ValidateAndSetDefaults`.
const GATUS_DEFAULT_THRESHOLDS: AlertThresholds = AlertThresholds {
    failure_threshold: 3,
    success_threshold: 2,
};

/// The thresholds Gatus applies to Docktornet's `custom` alert, which sets
/// neither.  Gatus first copies them from `alerting.custom.default-alert`, then
/// replaces any value that is still zero or negative with its own default.
/// The operator's base config lives in the same directory, so read it from
/// every YAML file there that Docktornet does not write.
pub async fn alert_thresholds(config_dir: impl AsRef<Path>) -> AlertThresholds {
    let mut failure = None;
    let mut success = None;
    for path in operator_config_files(config_dir.as_ref()).await {
        let body = match tokio::fs::read_to_string(&path).await {
            Ok(body) => body,
            Err(err) => {
                tracing::warn!(path = %path.display(), error = %err, "could not read gatus config file for alert thresholds");
                continue;
            }
        };
        let doc: yaml_serde::Value = match yaml_serde::from_str(&body) {
            Ok(doc) => doc,
            Err(err) => {
                tracing::warn!(path = %path.display(), error = %err, "could not parse gatus config file for alert thresholds");
                continue;
            }
        };
        let default_alert = &doc["alerting"]["custom"]["default-alert"];
        for (field, slot) in [
            ("failure-threshold", &mut failure),
            ("success-threshold", &mut success),
        ] {
            let value = &default_alert[field];
            if value.is_null() || slot.is_some() {
                continue;
            }
            match value.as_i64() {
                Some(n) => *slot = Some(n),
                None => tracing::warn!(
                    path = %path.display(),
                    field,
                    "alerting.custom.default-alert threshold is not an integer; the map uses the Gatus default"
                ),
            }
        }
    }
    let positive_or =
        |value: Option<i64>, default: i64| value.filter(|n| *n > 0).unwrap_or(default);
    AlertThresholds {
        failure_threshold: positive_or(failure, GATUS_DEFAULT_THRESHOLDS.failure_threshold),
        success_threshold: positive_or(success, GATUS_DEFAULT_THRESHOLDS.success_threshold),
    }
}

/// The YAML files Gatus loads from `config_dir`, walked recursively as Gatus
/// does, minus the service files and reload trigger Docktornet writes.  Sorted
/// so the first definition found is stable.
async fn operator_config_files(config_dir: &Path) -> Vec<PathBuf> {
    let reload_trigger = reload_trigger_path(config_dir);
    let mut files = Vec::new();
    let mut dirs = vec![config_dir.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
            continue;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            match entry.file_type().await {
                Ok(kind) if kind.is_dir() => dirs.push(path),
                Ok(_) => {
                    let is_yaml = matches!(
                        path.extension().and_then(|ext| ext.to_str()),
                        Some("yaml" | "yml")
                    );
                    if is_yaml && !is_managed_service_file(&path) && path != reload_trigger {
                        files.push(path);
                    }
                }
                Err(_) => {}
            }
        }
    }
    files.sort();
    files
}

/// The classification map Docktornet writes next to the service files, for the
/// alert service that labels Gatus alerts.  Gatus loads only `.yaml` and `.yml`
/// files, so it neither reads this file nor reloads when it changes.
pub fn classification_map_path(config_dir: impl AsRef<Path>) -> PathBuf {
    config_dir.as_ref().join("docktornet-classification.json")
}

/// The map's format version.  A change that breaks a reader bumps it.
const CLASSIFICATION_MAP_VERSION: u32 = 1;

#[derive(Serialize)]
struct ClassificationMap<'a> {
    version: u32,
    endpoints: Vec<ClassificationMapEntry<'a>>,
}

#[derive(Serialize)]
struct ClassificationMapEntry<'a> {
    key: &'a str,
    name: &'a str,
    group: &'a str,
    service_id: &'a str,
    host: &'a str,
    severity: Severity,
    severity_defaulted: bool,
    area: Area,
    area_defaulted: bool,
    /// `None` for an endpoint with no alert.
    alert: Option<AlertThresholds>,
}

pub fn render_classification_map(
    endpoints: &[ClassifiedEndpoint],
    thresholds: AlertThresholds,
) -> String {
    let mut entries: Vec<ClassificationMapEntry<'_>> = endpoints
        .iter()
        .map(|endpoint| ClassificationMapEntry {
            key: &endpoint.key,
            name: &endpoint.name,
            group: &endpoint.group,
            service_id: &endpoint.service_id,
            host: &endpoint.host,
            severity: endpoint.severity,
            severity_defaulted: endpoint.severity_defaulted,
            area: endpoint.area,
            area_defaulted: endpoint.area_defaulted,
            alert: endpoint.has_alert.then_some(thresholds),
        })
        .collect();
    entries.sort_by(|a, b| (a.key, a.service_id).cmp(&(b.key, b.service_id)));
    let mut body = serde_json::to_string_pretty(&ClassificationMap {
        version: CLASSIFICATION_MAP_VERSION,
        endpoints: entries,
    })
    .expect("classification map serializes");
    body.push('\n');
    body
}

/// Write the classification map when its content changed.  Returns whether it
/// wrote the file.
pub async fn write_classification_map(
    config_dir: impl AsRef<Path>,
    endpoints: &[ClassifiedEndpoint],
    thresholds: AlertThresholds,
) -> anyhow::Result<bool> {
    let target = classification_map_path(&config_dir);
    let body = render_classification_map(endpoints, thresholds);
    if tokio::fs::read_to_string(&target).await.ok().as_deref() == Some(body.as_str()) {
        return Ok(false);
    }
    tokio::fs::create_dir_all(config_dir.as_ref()).await?;
    write_atomically(&target, &body, None).await?;
    Ok(true)
}

/// Write `body` to `target` through a temporary file beside it, so Gatus never
/// reads a half-written file.  `modified` sets the file's mtime before it takes
/// `target`'s place.  A failed write removes its temporary file.
async fn write_atomically(
    target: &Path,
    body: &str,
    modified: Option<SystemTime>,
) -> std::io::Result<()> {
    let tmp = service_temp_file_path(target);
    let result = async {
        tokio::fs::write(&tmp, body).await?;
        if let Some(modified) = modified {
            tokio::fs::OpenOptions::new()
                .write(true)
                .open(&tmp)
                .await?
                .into_std()
                .await
                .set_modified(modified)?;
        }
        tokio::fs::rename(&tmp, target).await
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    result
}

/// Returns true if the path looks like a temporary file `write_atomically`
/// made for one of docktornet's own files: `{file name}.tmp-{pid}-{nanos}-{counter}`,
/// where the file name is a service file, the classification map or the
/// reload trigger.  One left behind by a crash is clutter Gatus never loads.
/// Operator files that merely look similar are never matched.
pub fn is_temp_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    let Some((target, suffix)) = name.rsplit_once(".tmp-") else {
        return false;
    };
    let target = Path::new(target);
    let ours = is_managed_service_file(target)
        || target == classification_map_path("")
        || target == reload_trigger_path("");
    let parts: Vec<&str> = suffix.split('-').collect();
    ours && parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
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

    let yaml = render_gatus_yaml(service_id, monitors)?;
    write_atomically(&target, &yaml, None).await?;

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
    write_atomically(
        &target,
        RELOAD_TRIGGER_YAML,
        Some(SystemTime::now() + std::time::Duration::from_secs(1)),
    )
    .await?;
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
            severity: None,
            area: None,
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
            severity: None,
            area: None,
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
            severity: None,
            area: None,
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
            severity: None,
            area: None,
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
        let parsed: yaml_serde::Mapping = yaml_serde::from_str(&body).unwrap();
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
            severity: None,
            area: None,
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
                severity: None,
                area: None,
            },
            GatusMonitor {
                index: 1,
                url: "https://public.example.com".to_string(),
                interval: "30s".to_string(),
                conditions: vec!["[STATUS] == 200".to_string()],
                group: None,
                name: None,
                telegram: true,
                severity: None,
                area: None,
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
            severity: None,
            area: None,
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
                severity: None,
                area: None,
            },
            GatusMonitor {
                index: 1,
                url: "https://b.example.com".to_string(),
                interval: "30s".to_string(),
                conditions: vec!["[STATUS] == 200".to_string()],
                group: None,
                name: Some("web".to_string()),
                telegram: true,
                severity: None,
                area: None,
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

    #[tokio::test]
    async fn alert_thresholds_default_to_gatus_values_without_a_base_config() {
        let dir = tempdir().unwrap();
        assert_eq!(
            alert_thresholds(dir.path().join("missing")).await,
            GATUS_DEFAULT_THRESHOLDS
        );
    }

    #[tokio::test]
    async fn alert_thresholds_read_nested_files_and_skip_docktornet_files() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("base")).unwrap();
        std::fs::write(
            dir.path().join("base/alerting.yml"),
            "alerting:\n  custom:\n    default-alert:\n      success-threshold: 4\n",
        )
        .unwrap();
        // A service file never carries a default alert; if it were read, this
        // one would win.
        std::fs::write(
            service_file_path(dir.path(), "m1/app"),
            "alerting:\n  custom:\n    default-alert:\n      failure-threshold: 9\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("broken.yaml"), "alerting: [").unwrap();

        assert_eq!(
            alert_thresholds(dir.path()).await,
            AlertThresholds {
                failure_threshold: 3,
                success_threshold: 4,
            }
        );
    }

    #[tokio::test]
    async fn alert_thresholds_replace_zero_negative_and_non_integer_values() {
        // Gatus copies the default alert's value, then replaces anything not
        // above zero with its own default.
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path().join("00-base.yaml"),
            "alerting:\n  custom:\n    default-alert:\n      failure-threshold: 0\n      success-threshold: ${N}\n",
        )
        .unwrap();
        assert_eq!(alert_thresholds(dir.path()).await, GATUS_DEFAULT_THRESHOLDS);

        std::fs::write(
            dir.path().join("00-base.yaml"),
            "alerting:\n  custom:\n    default-alert:\n      failure-threshold: -1\n",
        )
        .unwrap();
        assert_eq!(alert_thresholds(dir.path()).await, GATUS_DEFAULT_THRESHOLDS);
    }

    #[test]
    fn endpoint_key_matches_gatus_sanitizing() {
        // Expected values worked by hand from Gatus's key.go.
        assert_eq!(gatus_endpoint_key("m1", "app"), "m1_app");
        assert_eq!(
            gatus_endpoint_key(" Core/Infra ", "a_b.c,d#e"),
            "core-infra_a-b-c-d-e"
        );
        assert_eq!(gatus_endpoint_key("media", "app 1"), "media_app-1");
    }

    #[test]
    fn endpoint_key_lowercases_one_character_at_a_time_like_go() {
        // Go's strings.ToLower maps each rune on its own: U+0130 becomes a plain
        // "i" and a word-final sigma stays σ.  Rust's str::to_lowercase would
        // give "i\u{307}" and ς, a key Gatus never reports.
        assert_eq!(gatus_endpoint_key("İzmir", "ΟΔΟΣ"), "izmir_οδοσ");
    }

    #[tokio::test]
    async fn classification_map_is_not_loaded_by_gatus_or_swept() {
        let dir = tempdir().unwrap();
        let path = classification_map_path(dir.path());
        let ext = path.extension().and_then(|e| e.to_str());
        assert!(!matches!(ext, Some("yaml" | "yml")));
        assert!(!is_managed_service_file(&path));
        // An unchanged map is not rewritten.
        assert!(
            write_classification_map(dir.path(), &[], GATUS_DEFAULT_THRESHOLDS)
                .await
                .unwrap()
        );
        assert!(
            !write_classification_map(dir.path(), &[], GATUS_DEFAULT_THRESHOLDS)
                .await
                .unwrap()
        );
    }

    /// Occupy `target` with a non-empty directory, so renaming onto it fails.
    fn block_rename_onto(target: &Path) {
        std::fs::create_dir_all(target.join("occupied")).unwrap();
    }

    fn temp_files_in(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".tmp-"))
            .collect()
    }

    #[tokio::test]
    async fn failed_rename_leaves_no_temp_file_behind() {
        let dir = tempdir().unwrap();
        let monitors = vec![GatusMonitor {
            index: 0,
            url: "https://app.example.com".to_string(),
            interval: "30s".to_string(),
            conditions: vec!["[STATUS] == 200".to_string()],
            group: None,
            name: None,
            telegram: false,
            severity: None,
            area: None,
        }];
        block_rename_onto(&service_file_path(dir.path(), "m1/app"));
        block_rename_onto(&classification_map_path(dir.path()));
        block_rename_onto(&reload_trigger_path(dir.path()));

        assert!(
            write_service_file(dir.path(), "m1/app", &monitors)
                .await
                .is_err()
        );
        assert!(
            write_classification_map(dir.path(), &[], GATUS_DEFAULT_THRESHOLDS)
                .await
                .is_err()
        );
        assert!(touch_reload_trigger(dir.path()).await.is_err());

        assert_eq!(temp_files_in(dir.path()), Vec::<String>::new());
    }
}
