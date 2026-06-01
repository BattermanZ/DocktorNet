# Gatus custom alert provider + clean endpoint naming — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make docktornet emit clean, human-readable Gatus endpoint names (bare stack name, with safe disambiguation for multi-monitor stacks), default the endpoint group to the host, and switch the emitted alert type from `telegram` to `custom`.

**Architecture:** Two small Rust changes plus docs. (1) `crates/core/src/labels.rs` gains an optional `name` field/label on `GatusMonitor`. (2) `crates/server/src/integrations/gatus.rs` rewrites `render_gatus_yaml` to resolve display names in two passes (name label → bare stack name → ` {index}` collision suffix), default the group to the machine/host portion of `service_id`, and emit `type: custom`. The per-service file path is keyed by `service_id` and does not change, so existing files rewrite themselves in place on the next reconcile.

**Tech Stack:** Rust, `serde_yaml`, `cargo test`.

**Spec:** `docs/superpowers/specs/2026-06-01-gatus-custom-alert-naming-design.md`

**Rollout note (not code):** The host-side `00-base.yaml` must define an `alerting.custom` provider BEFORE this binary is deployed, or Gatus fails to load config once files reference `type: custom`. Task 3 documents this.

---

## File structure

- `crates/core/src/labels.rs` — `GatusMonitor` struct + label parsing. Add `name: Option<String>`.
- `crates/server/src/integrations/gatus.rs` — YAML renderer. Rewrite `render_gatus_yaml`; update tests.
- `README.md` — gatus label table. Add `name` row; fix `group` default.
- `docs/deployment.md` — base Gatus config example. Replace `alerting.telegram` with `alerting.custom`.

---

## Task 1: Add optional `name` field/label to `GatusMonitor`

**Files:**
- Modify: `crates/core/src/labels.rs` (struct at lines 45–53, `build_gatus` at 221–250, `is_gatus_field` at 252–257)
- Test: same file (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Add the field to the struct**

In `crates/core/src/labels.rs`, change the `GatusMonitor` struct to add a `name` field after `group`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatusMonitor {
    pub index: u8,
    pub url: String,
    pub interval: String,
    pub conditions: Vec<String>,
    pub group: Option<String>,
    pub name: Option<String>,
    pub telegram: bool,
}
```

This will break compilation of `gatus.rs` test literals (fixed in Task 2) and `build_gatus` (next step). That is expected.

- [ ] **Step 2: Parse the label in `build_gatus`**

In `build_gatus`, after the `group` line (`let group = ...`), add a `name` parse, and add `name` to the returned struct:

```rust
    let group = fields.get("group").cloned().filter(|g| !g.is_empty());
    let name = fields.get("name").cloned().filter(|n| !n.is_empty());
    let telegram = match fields.get("telegram") {
        Some(value) => parse_bool(value)?,
        None => true,
    };

    Ok(GatusMonitor {
        index,
        url,
        interval,
        conditions,
        group,
        name,
        telegram,
    })
```

- [ ] **Step 3: Add `name` to the field allowlist**

In `is_gatus_field`, add `"name"`:

```rust
fn is_gatus_field(field: &str) -> bool {
    matches!(
        field,
        "url" | "interval" | "conditions" | "group" | "name" | "telegram"
    )
}
```

- [ ] **Step 4: Write the failing tests**

Add these three tests inside the existing `#[cfg(test)] mod tests` block in `crates/core/src/labels.rs`. They mirror the existing `gatus_group_*` tests, which build the label map with the `lbl(&[...])` helper and call `parse(...)`:

```rust
    #[test]
    fn gatus_name_parsed_when_set() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.0.name", "Homepage"),
        ]));
        assert_eq!(p.gatus[0].name, Some("Homepage".to_string()));
    }

    #[test]
    fn gatus_name_defaults_to_none_when_absent() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
        ]));
        assert_eq!(p.gatus[0].name, None);
    }

    #[test]
    fn gatus_empty_name_is_treated_as_none() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.0.name", ""),
        ]));
        assert_eq!(p.gatus[0].name, None);
    }
```

- [ ] **Step 5: Run the new tests (they should now pass, since impl is already in place)**

Run: `cargo test -p docktornet-core gatus_name`
Expected: 3 tests pass (`gatus_name_parsed_when_set`, `gatus_name_defaults_to_none_when_absent`, `gatus_empty_name_is_treated_as_none`).

> Scope to `-p docktornet-core` here: `gatus.rs` (in `docktornet-server`) still has stale `GatusMonitor` literals that won't compile until Task 2, so a full `cargo test --workspace` will fail to build at this point. That is expected and resolved in Task 2.

- [ ] **Step 6: Commit**

```bash
git add crates/core/src/labels.rs
git commit -m "feat(gatus): add optional name label to GatusMonitor

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Rewrite the YAML renderer (name resolution, group default, custom alert)

**Files:**
- Modify: `crates/server/src/integrations/gatus.rs` (`render_gatus_yaml` at lines 42–71; tests at 150–222)
- Test: same file

- [ ] **Step 1: Update existing test struct literals to include `name: None`**

Every `GatusMonitor { ... }` literal in `gatus.rs` tests currently omits `name` and will fail to compile after Task 1. Add `name: None,` (after the `group:` line) to each literal in these tests:
- `renders_expected_yaml_with_default_group_and_telegram_alert` (~line 152)
- `renders_group_override_when_set` (~line 180)
- `omits_alerts_block_when_telegram_is_false` (~line 195)
- `writes_and_removes_service_file` (~line 214)

Example for the first one:

```rust
        let monitors = vec![GatusMonitor {
            index: 0,
            url: "https://app.example.com".to_string(),
            interval: "30s".to_string(),
            conditions: vec!["[STATUS] == 200".to_string()],
            group: None,
            name: None,
            telegram: true,
        }];
```

- [ ] **Step 2: Update the existing renderer tests to the new expected output**

Replace the body of `renders_expected_yaml_with_default_group_and_telegram_alert`. Service id `"m1/app"` now yields name `app` (container), group `m1` (machine), and `type: custom`. Rename the test to reflect `custom`:

```rust
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
```

In `renders_group_override_when_set`, the group label still overrides, but the default it must NOT equal is now `m1`. Update the negative assertion:

```rust
        let yaml = render_gatus_yaml("m1/app", &monitors).unwrap();
        assert!(yaml.contains("group: media\n"));
        assert!(!yaml.contains("group: m1\n"));
```

`omits_alerts_block_when_telegram_is_false` keeps its assertions (`!yaml.contains("alerts")`, `!yaml.contains("telegram")`) — still valid. No change needed beyond the `name: None` added in Step 1. (It never referenced `telegram` in output anyway.)

- [ ] **Step 3: Add new tests for name resolution and collisions**

Add these tests to the same `mod tests` block:

```rust
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
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cargo test -p docktornet-server --lib integrations::gatus`
Expected: FAIL — the renderer still emits `name: m1/app/0`, `group: m1/app`, and `type: telegram`, so the new and updated assertions fail.

- [ ] **Step 5: Rewrite `render_gatus_yaml`**

Replace the whole `render_gatus_yaml` function (lines 42–71) with:

```rust
pub fn render_gatus_yaml(
    service_id: &str,
    monitors: &[GatusMonitor],
) -> Result<String, serde_yaml::Error> {
    // service_id is `{machine}/{container}`; fall back to the whole id if there
    // is no slash. The container half is the default endpoint name; the machine
    // half is the default group.
    let (machine, container) = service_id.split_once('/').unwrap_or((service_id, service_id));

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
                group: monitor
                    .group
                    .clone()
                    .unwrap_or_else(|| machine.to_string()),
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
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p docktornet-server --lib integrations::gatus`
Expected: PASS — all renderer tests (existing + new) green.

- [ ] **Step 7: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS. (Catches any other test that constructed a `GatusMonitor` literal or asserted on the old YAML shape — fix any such literal by adding `name: None` and updating expected strings to the new shape.)

- [ ] **Step 8: Commit**

```bash
git add crates/server/src/integrations/gatus.rs
git commit -m "feat(gatus): clean endpoint names, host group default, custom alert type

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Update documentation

**Files:**
- Modify: `README.md` (gatus label table at lines 58–64)
- Modify: `docs/deployment.md` (base config at lines 80–94)

- [ ] **Step 1: Add the `name` label row and fix the `group` default in README.md**

In `README.md`, replace the `group` and `telegram` rows (lines 63–64) and insert a `name` row, so the relevant rows read:

```markdown
| `docktornet.gatus.<index>.group` | no | string | host/machine id | `media` | Gatus dashboard group. Defaults to the machine this container runs on. |
| `docktornet.gatus.<index>.name` | no | string | container name | `Homepage` | Display name shown in alerts/dashboard. Defaults to the container name; colliding defaults get a ` <index>` suffix. |
| `docktornet.gatus.<index>.telegram` | no | bool | `true` | `false` | Emit a Gatus `custom` alert (used for Telegram). |
```

- [ ] **Step 2: Replace the base Gatus config example in docs/deployment.md**

In `docs/deployment.md`, replace the YAML block at lines 82–92 (the `web/storage/alerting.telegram` example) with the `alerting.custom` shape:

````markdown
```yaml
web:
  port: 8080
storage:
  type: sqlite
  path: /data/gatus.db
alerting:
  custom:
    url: "https://api.telegram.org/bot${GATUS_TELEGRAM_TOKEN}/sendMessage"
    method: "POST"
    headers:
      Content-Type: application/json
    body: |
      {
        "chat_id": "${GATUS_TELEGRAM_CHAT_ID}",
        "parse_mode": "Markdown",
        "text": "[ALERT_TRIGGERED_OR_RESOLVED] *[ENDPOINT_NAME]*\n🔗 [ENDPOINT_URL]"
      }
    placeholders:
      ALERT_TRIGGERED_OR_RESOLVED:
        TRIGGERED: "🔴 DOWN —"
        RESOLVED: "🟢 BACK UP —"
    default-alert:
      send-on-resolved: true
```
````

- [ ] **Step 3: Replace the "required" note (line 94)**

Replace line 94 with:

```markdown
`alerting.custom` is **required** if any service has `docktornet.gatus.N.telegram=true` (the default), because docktornet emits `type: custom` alert references. **The `alerting.custom` block must exist before the new docktornet binary runs** — Gatus validates each endpoint's alert type against a configured provider at config load, so a missing `alerting.custom` block makes Gatus fail to load once per-service files reference it. The env vars `GATUS_TELEGRAM_TOKEN` and `GATUS_TELEGRAM_CHAT_ID` must be available to the Gatus container.
```

- [ ] **Step 4: Sanity-check the docs render**

Run: `git diff --stat README.md docs/deployment.md`
Expected: both files show as modified. Eyeball the diff for the YAML fences being balanced.

- [ ] **Step 5: Commit**

```bash
git add README.md docs/deployment.md
git commit -m "docs(gatus): document custom alert provider and name label

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Final verification

- [ ] Run the full suite once more: `cargo test --workspace` → all green.
- [ ] `cargo build --workspace` → clean.
- [ ] Confirm no other source/test references the old shape:
  `grep -rn "type: telegram\|kind: \"telegram\"\|/{}\", monitor.index" crates` → no hits in non-test code (the `telegram` bool/label name intentionally remains).
