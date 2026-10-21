//! Severity and area labels in, Gatus config and classification map out.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use docktornet_core::{ContainerEntry, MachineId, Snapshot};
use docktornet_server::integrations::gatus;
use docktornet_server::reconcile::{ReconcileOptions, Reconciler};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};

struct Env {
    _dir: TempDir,
    gatus_dir: PathBuf,
    reconciler: Reconciler,
}

async fn env() -> Env {
    let dir = tempdir().unwrap();
    let gatus_dir = dir.path().join("gatus");
    let pool = docktornet_server::db::connect(&dir.path().join("t.db"))
        .await
        .unwrap();
    let reconciler = Reconciler::new(
        pool,
        ReconcileOptions {
            dry_run: false,
            adopt_existing: false,
            gatus_config_dir: gatus_dir.clone(),
        },
    );
    Env {
        _dir: dir,
        gatus_dir,
        reconciler,
    }
}

fn snapshot(machine: &str, taken_at: u64, containers: &[(&str, &[(&str, &str)])]) -> Snapshot {
    Snapshot {
        machine_id: MachineId::new(machine).unwrap(),
        forward_host: "127.0.0.1".into(),
        taken_at,
        agent_version: None,
        containers: containers
            .iter()
            .map(|(name, labels)| ContainerEntry {
                name: name.to_string(),
                labels: labels
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<BTreeMap<_, _>>(),
            })
            .collect(),
    }
}

fn read_map(gatus_dir: &Path) -> Value {
    let body = std::fs::read_to_string(gatus::classification_map_path(gatus_dir))
        .expect("classification map written");
    serde_json::from_str(&body).unwrap()
}

#[tokio::test]
async fn explicit_severity_and_area_land_in_the_map() {
    let env = env().await;
    env.reconciler
        .reconcile_snapshot(&snapshot(
            "m1",
            1,
            &[(
                "restic",
                &[
                    ("docktornet.gatus.0.url", "https://restic.example.com"),
                    ("docktornet.gatus.0.interval", "30s"),
                    ("docktornet.gatus.0.severity", "critical"),
                    ("docktornet.gatus.0.area", "backups"),
                ],
            )],
        ))
        .await
        .unwrap();

    assert_eq!(
        read_map(&env.gatus_dir),
        json!({
            "version": 1,
            "endpoints": [{
                "key": "m1_restic",
                "name": "restic",
                "group": "m1",
                "service_id": "m1/restic",
                "host": "m1",
                "severity": "critical",
                "severity_defaulted": false,
                "area": "backups",
                "area_defaulted": false,
                "alert": {"failure_threshold": 3, "success_threshold": 2},
            }],
        })
    );
    // The new labels reach the map only; the Gatus file is what it was before.
    let yaml =
        std::fs::read_to_string(gatus::service_file_path(&env.gatus_dir, "m1/restic")).unwrap();
    assert_eq!(
        yaml,
        concat!(
            "endpoints:\n",
            "- name: restic\n",
            "  group: m1\n",
            "  url: https://restic.example.com\n",
            "  interval: 30s\n",
            "  conditions:\n",
            "  - '[STATUS] == 200'\n",
            "  alerts:\n",
            "  - type: custom\n",
            "    send-on-resolved: true\n",
        )
    );
}

#[tokio::test]
async fn absent_labels_default_and_leave_the_gatus_file_unchanged() {
    let env = env().await;
    env.reconciler
        .reconcile_snapshot(&snapshot(
            "m1",
            1,
            &[(
                "app",
                &[
                    ("docktornet.gatus.0.url", "https://app.example.com"),
                    ("docktornet.gatus.0.interval", "30s"),
                ],
            )],
        ))
        .await
        .unwrap();

    // The same bytes the release before these labels wrote.
    let yaml = std::fs::read_to_string(gatus::service_file_path(&env.gatus_dir, "m1/app")).unwrap();
    assert_eq!(
        yaml,
        concat!(
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
        )
    );

    let entry = &read_map(&env.gatus_dir)["endpoints"][0];
    assert_eq!(entry["severity"], "warning");
    assert_eq!(entry["severity_defaulted"], true);
    assert_eq!(entry["area"], "apps");
    assert_eq!(entry["area_defaulted"], true);
}

#[tokio::test]
async fn unknown_value_drops_that_endpoint_instead_of_defaulting() {
    let env = env().await;
    env.reconciler
        .reconcile_snapshot(&snapshot(
            "m1",
            1,
            &[(
                "app",
                &[
                    ("docktornet.gatus.0.url", "https://lan.example.com"),
                    ("docktornet.gatus.0.interval", "30s"),
                    ("docktornet.gatus.0.name", "lan"),
                    ("docktornet.gatus.0.severity", "urgent"),
                    ("docktornet.gatus.1.url", "https://public.example.com"),
                    ("docktornet.gatus.1.interval", "30s"),
                    ("docktornet.gatus.1.name", "public"),
                    ("docktornet.gatus.1.area", "databases"),
                    ("docktornet.gatus.2.url", "https://ok.example.com"),
                    ("docktornet.gatus.2.interval", "30s"),
                    ("docktornet.gatus.2.name", "ok"),
                    ("docktornet.gatus.2.severity", "info"),
                ],
            )],
        ))
        .await
        .unwrap();

    let yaml = std::fs::read_to_string(gatus::service_file_path(&env.gatus_dir, "m1/app")).unwrap();
    assert!(yaml.contains("- name: ok\n"), "{yaml}");
    assert!(!yaml.contains("lan.example.com"), "{yaml}");
    assert!(!yaml.contains("public.example.com"), "{yaml}");

    let map = read_map(&env.gatus_dir);
    let keys: Vec<&str> = map["endpoints"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["key"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["m1_ok"]);
    assert_eq!(map["endpoints"][0]["severity"], "info");
}

#[tokio::test]
async fn host_is_the_reporting_machine_even_when_the_group_differs() {
    let env = env().await;
    env.reconciler
        .reconcile_snapshot(&snapshot(
            "BatterDock",
            1,
            &[(
                "jellyfin",
                &[
                    ("docktornet.gatus.0.url", "https://jf.example.com"),
                    ("docktornet.gatus.0.interval", "30s"),
                    ("docktornet.gatus.0.group", "Media Stack"),
                    ("docktornet.gatus.0.name", "Jelly.fin"),
                ],
            )],
        ))
        .await
        .unwrap();

    let entry = &read_map(&env.gatus_dir)["endpoints"][0];
    assert_eq!(entry["host"], "batterdock");
    assert_eq!(entry["group"], "Media Stack");
    assert_eq!(entry["service_id"], "BatterDock/jellyfin");
    // Gatus's own key for group "Media Stack", name "Jelly.fin".
    assert_eq!(entry["key"], "media-stack_jelly-fin");
}

#[tokio::test]
async fn thresholds_follow_the_base_config_default_alert() {
    let env = env().await;
    std::fs::create_dir_all(&env.gatus_dir).unwrap();
    std::fs::write(
        env.gatus_dir.join("00-base.yaml"),
        concat!(
            "alerting:\n",
            "  custom:\n",
            "    url: http://127.0.0.1:9/alert\n",
            "    default-alert:\n",
            "      send-on-resolved: true\n",
            "      failure-threshold: 5\n",
        ),
    )
    .unwrap();

    env.reconciler
        .reconcile_snapshot(&snapshot(
            "m1",
            1,
            &[(
                "app",
                &[
                    ("docktornet.gatus.0.url", "https://a.example.com"),
                    ("docktornet.gatus.0.interval", "30s"),
                    ("docktornet.gatus.0.name", "alerting"),
                    ("docktornet.gatus.1.url", "https://b.example.com"),
                    ("docktornet.gatus.1.interval", "30s"),
                    ("docktornet.gatus.1.name", "quiet"),
                    ("docktornet.gatus.1.telegram", "false"),
                ],
            )],
        ))
        .await
        .unwrap();

    // The generated alert sets no thresholds, so Gatus takes failure from the
    // default alert and success from its own default of 2.
    let yaml = std::fs::read_to_string(gatus::service_file_path(&env.gatus_dir, "m1/app")).unwrap();
    assert!(!yaml.contains("threshold"), "{yaml}");
    let map = read_map(&env.gatus_dir);
    assert_eq!(map["endpoints"][0]["key"], "m1_alerting");
    assert_eq!(
        map["endpoints"][0]["alert"],
        json!({"failure_threshold": 5, "success_threshold": 2})
    );
    // An endpoint with no alert has no thresholds.
    assert_eq!(map["endpoints"][1]["key"], "m1_quiet");
    assert_eq!(map["endpoints"][1]["alert"], Value::Null);
}

fn monitor(index: u8, name: &'static str) -> Vec<(String, String)> {
    vec![
        (
            format!("docktornet.gatus.{index}.url"),
            format!("https://{name}.example.com"),
        ),
        (format!("docktornet.gatus.{index}.interval"), "30s".into()),
        (format!("docktornet.gatus.{index}.name"), name.into()),
    ]
}

fn map_keys(gatus_dir: &Path) -> Vec<String> {
    read_map(gatus_dir)["endpoints"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["key"].as_str().unwrap().to_string())
        .collect()
}

async fn push(env: &Env, taken_at: u64, containers: &[(&str, Vec<(String, String)>)]) {
    let containers: Vec<(&str, Vec<(&str, &str)>)> = containers
        .iter()
        .map(|(name, labels)| {
            (
                *name,
                labels
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect(),
            )
        })
        .collect();
    let containers: Vec<(&str, &[(&str, &str)])> = containers
        .iter()
        .map(|(name, labels)| (*name, labels.as_slice()))
        .collect();
    env.reconciler
        .reconcile_snapshot(&snapshot("m1", taken_at, &containers))
        .await
        .unwrap();
}

#[tokio::test]
async fn endpoints_removed_from_the_labels_leave_the_map() {
    let env = env().await;
    let app_two = [monitor(0, "a"), monitor(1, "b")].concat();

    push(
        &env,
        1,
        &[
            ("app", app_two),
            ("brief", monitor(0, "brief")),
            ("gone", monitor(0, "gone")),
        ],
    )
    .await;
    assert_eq!(
        map_keys(&env.gatus_dir),
        vec!["m1_a", "m1_b", "m1_brief", "m1_gone"]
    );

    // One monitor dropped from app's labels.
    push(
        &env,
        2,
        &[
            ("app", monitor(0, "a")),
            ("brief", monitor(0, "brief")),
            ("gone", monitor(0, "gone")),
        ],
    )
    .await;
    assert_eq!(
        map_keys(&env.gatus_dir),
        vec!["m1_a", "m1_brief", "m1_gone"]
    );

    // `brief` leaves.  Its Gatus file is kept through the grace period, so its
    // endpoint stays in the map while Gatus still runs it.
    push(
        &env,
        3,
        &[("app", monitor(0, "a")), ("gone", monitor(0, "gone"))],
    )
    .await;
    assert_eq!(
        map_keys(&env.gatus_dir),
        vec!["m1_a", "m1_brief", "m1_gone"]
    );
    assert!(gatus::service_file_path(&env.gatus_dir, "m1/brief").exists());

    // `gone` was last seen past the grace period, so its file and its
    // endpoint go at once.
    sqlx::query("UPDATE services SET last_seen = last_seen - 1000 WHERE id = 'm1/gone'")
        .execute(env.reconciler.pool())
        .await
        .unwrap();
    push(&env, 4, &[("app", monitor(0, "a"))]).await;
    assert!(!gatus::service_file_path(&env.gatus_dir, "m1/gone").exists());
    assert_eq!(map_keys(&env.gatus_dir), vec!["m1_a", "m1_brief"]);
}

#[tokio::test]
async fn full_resync_and_the_sweep_rebuild_a_lost_map() {
    let env = env().await;
    push(&env, 1, &[("app", monitor(0, "a"))]).await;
    let map_path = gatus::classification_map_path(&env.gatus_dir);

    std::fs::remove_file(&map_path).unwrap();
    env.reconciler.full_resync().await.unwrap();
    assert_eq!(map_keys(&env.gatus_dir), vec!["m1_a"]);

    std::fs::remove_file(&map_path).unwrap();
    env.reconciler.sweep_pending_removals().await.unwrap();
    assert_eq!(map_keys(&env.gatus_dir), vec!["m1_a"]);
}
