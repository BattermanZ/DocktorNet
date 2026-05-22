use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ids::MachineId;

/// A single container reported by an agent. `labels` is the subset of the
/// container's Docker labels whose keys start with `docktornet.`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerEntry {
    pub name: String,
    pub labels: BTreeMap<String, String>,
}

/// A snapshot of all docktornet-relevant containers on one machine at one
/// point in time. Sent in the body of `POST /api/v1/snapshot`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub machine_id: MachineId,
    pub taken_at: u64,
    pub containers: Vec<ContainerEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_roundtrips_json() {
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "https://x".into());

        let snap = Snapshot {
            machine_id: MachineId::new("batterdock"),
            taken_at: 1716300000,
            containers: vec![ContainerEntry {
                name: "myapp".into(),
                labels,
            }],
        };

        let s = serde_json::to_string(&snap).unwrap();
        let back: Snapshot = serde_json::from_str(&s).unwrap();
        assert_eq!(snap, back);
    }

    #[test]
    fn empty_snapshot_is_valid() {
        let snap = Snapshot {
            machine_id: MachineId::new("batterdock"),
            taken_at: 1716300000,
            containers: vec![],
        };
        let s = serde_json::to_string(&snap).unwrap();
        assert!(s.contains("\"containers\":[]"));
    }
}
