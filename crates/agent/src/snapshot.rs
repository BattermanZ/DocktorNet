use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use docktornet_core::{ContainerEntry, MachineId, Snapshot};
use tracing::warn;

use crate::sockets::RawContainer;

/// Build a Snapshot from the raw container list, keeping only containers that
/// declare at least one label starting with `docktornet.`. Labels without the
/// prefix are dropped to keep payloads small.
pub fn build(machine_id: &MachineId, forward_host: &str, raw: &[RawContainer]) -> Snapshot {
    let taken_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);

    let mut candidates = Vec::new();
    for container in raw {
        let labels: BTreeMap<String, String> = container
            .labels
            .iter()
            .filter(|(key, _)| key.starts_with("docktornet."))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();

        if labels.is_empty() {
            continue;
        }

        candidates.push(ContainerEntry {
            name: container.name.clone(),
            labels,
        });
    }

    // The server keys each service by `{machine_id}/{container_name}`, so two
    // labelled containers sharing a name (e.g. a docker and a podman container
    // both named `app` across different sockets) would collapse to one service
    // id, silently overwriting one container's public DNS/proxy config with the
    // other's. Rather than let one win non-deterministically, drop every entry
    // whose name is ambiguous and warn loudly so the operator can rename.
    let mut name_counts: BTreeMap<&str, usize> = BTreeMap::new();
    for candidate in &candidates {
        *name_counts.entry(candidate.name.as_str()).or_insert(0) += 1;
    }

    let mut containers = Vec::with_capacity(candidates.len());
    for candidate in &candidates {
        if name_counts[candidate.name.as_str()] > 1 {
            continue;
        }
        containers.push(candidate.clone());
    }

    for (name, count) in &name_counts {
        if *count > 1 {
            warn!(
                container = %name,
                count = *count,
                "multiple labelled containers share a name across sockets; \
                 dropping all of them from the snapshot to avoid silently \
                 applying one container's public config to another. Rename so \
                 each labelled container has a unique name."
            );
        }
    }

    containers.sort_by(|a, b| a.name.cmp(&b.name));

    Snapshot {
        machine_id: machine_id.clone(),
        forward_host: forward_host.to_string(),
        taken_at,
        containers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rc(name: &str, labels: &[(&str, &str)]) -> RawContainer {
        RawContainer {
            name: name.into(),
            labels: labels
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        }
    }

    #[test]
    fn keeps_only_docktornet_prefixed_labels_and_drops_unlabelled_containers() {
        let snap = build(
            &MachineId::new("m1"),
            "10.0.0.10",
            &[
                rc(
                    "withlabel",
                    &[("docktornet.gatus.0.url", "u"), ("com.docker.x", "y")],
                ),
                rc("nolabels", &[("com.docker.x", "y")]),
            ],
        );

        assert_eq!(snap.containers.len(), 1);
        assert_eq!(snap.containers[0].name, "withlabel");
        assert_eq!(snap.containers[0].labels.len(), 1);
        assert!(
            snap.containers[0]
                .labels
                .contains_key("docktornet.gatus.0.url")
        );
    }

    #[test]
    fn duplicate_names_across_sockets_are_dropped_not_silently_merged() {
        // Two distinct containers named `app` (e.g. one docker, one podman)
        // each carry their own docktornet config. Keying a service purely by
        // name means one would silently overwrite the other's public
        // DNS/proxy config on the server. Rather than let one win at random,
        // build must drop the ambiguous entries entirely.
        let snap = build(
            &MachineId::new("m1"),
            "10.0.0.10",
            &[
                rc("app", &[("docktornet.npm.0.domain", "docker.example.com")]),
                rc("app", &[("docktornet.npm.0.domain", "podman.example.com")]),
                rc("unique", &[("docktornet.x", "1")]),
            ],
        );

        assert!(
            snap.containers.iter().all(|c| c.name != "app"),
            "ambiguous duplicate-named containers must not collapse to a single \
             service; neither should silently win. Got: {:?}",
            snap.containers
        );
        assert_eq!(
            snap.containers.len(),
            1,
            "only the unambiguous container should survive"
        );
        assert_eq!(snap.containers[0].name, "unique");
    }

    #[test]
    fn containers_sorted_by_name_for_stable_diffs() {
        let snap = build(
            &MachineId::new("m1"),
            "10.0.0.10",
            &[
                rc("zebra", &[("docktornet.x", "1")]),
                rc("apple", &[("docktornet.x", "1")]),
            ],
        );

        assert_eq!(
            snap.containers
                .iter()
                .map(|container| container.name.clone())
                .collect::<Vec<_>>(),
            vec!["apple", "zebra"]
        );
    }
}
