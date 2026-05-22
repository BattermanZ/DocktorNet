use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use docktornet_core::{ContainerEntry, MachineId, Snapshot};

use crate::sockets::RawContainer;

/// Build a Snapshot from the raw container list, keeping only containers that
/// declare at least one label starting with `docktornet.`. Labels without the
/// prefix are dropped to keep payloads small.
pub fn build(machine_id: &MachineId, raw: &[RawContainer]) -> Snapshot {
    let taken_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);

    let mut containers = Vec::new();
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

        containers.push(ContainerEntry {
            name: container.name.clone(),
            labels,
        });
    }

    containers.sort_by(|a, b| a.name.cmp(&b.name));

    Snapshot {
        machine_id: machine_id.clone(),
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
    fn containers_sorted_by_name_for_stable_diffs() {
        let snap = build(
            &MachineId::new("m1"),
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
