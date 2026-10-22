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
    /// This machine's address that NPM should forward proxied traffic to.
    pub forward_host: String,
    /// When the agent built the snapshot, in Unix seconds by the agent's
    /// clock, or 0 if it could not read it.  The server times grace periods by
    /// its own clock and does not rely on this.
    pub taken_at: u64,
    pub containers: Vec<ContainerEntry>,
    /// The agent's release.  Absent from agents that predate version
    /// reporting.
    #[serde(default)]
    pub agent_version: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_roundtrips_json() {
        let mut labels = BTreeMap::new();
        labels.insert("docktornet.gatus.0.url".into(), "https://x".into());

        let snap = Snapshot {
            machine_id: MachineId::new("batterdock").unwrap(),
            forward_host: "10.0.0.10".into(),
            taken_at: 1716300000,
            agent_version: None,
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
    fn a_snapshot_from_an_agent_without_version_reporting_parses() {
        let json = r#"{"machine_id":"batterdock","forward_host":"10.0.0.10","taken_at":1716300000,"containers":[]}"#;
        let snap: Snapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snap.agent_version, None);
    }

    #[test]
    fn agent_version_roundtrips_json() {
        let json = r#"{"machine_id":"batterdock","forward_host":"10.0.0.10","taken_at":1716300000,"containers":[],"agent_version":"1.2.0"}"#;
        let snap: Snapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snap.agent_version.as_deref(), Some("1.2.0"));
        let back: Snapshot = serde_json::from_str(&serde_json::to_string(&snap).unwrap()).unwrap();
        assert_eq!(snap, back);
    }

    #[test]
    fn empty_snapshot_is_valid() {
        let snap = Snapshot {
            machine_id: MachineId::new("batterdock").unwrap(),
            forward_host: "10.0.0.10".into(),
            taken_at: 1716300000,
            agent_version: None,
            containers: vec![],
        };
        let s = serde_json::to_string(&snap).unwrap();
        assert!(s.contains("\"containers\":[]"));
    }
}
