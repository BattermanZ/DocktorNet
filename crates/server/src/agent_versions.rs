use std::collections::HashMap;
use std::sync::Mutex;

use docktornet_core::MachineId;

/// What to tell the operator about an agent's reported version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionNote {
    /// The agent runs a different release from the server.
    Mismatch { agent_version: String },
    /// The agent sent no version.
    Predates,
}

/// Remembers the version each machine last reported, so a mismatch is noted
/// once per change rather than on every push.  In memory only: a restarted
/// server notes each agent once more.
pub struct AgentVersionTracker {
    server_version: String,
    last_reported: Mutex<HashMap<MachineId, Option<String>>>,
}

impl AgentVersionTracker {
    pub fn new(server_version: impl Into<String>) -> Self {
        Self {
            server_version: server_version.into(),
            last_reported: Mutex::new(HashMap::new()),
        }
    }

    pub fn server_version(&self) -> &str {
        &self.server_version
    }

    /// Record that `machine_id` pushed with `agent_version`.  Returns a note
    /// when the version changed since its last push (or this is its first)
    /// and differs from the server's by exact string comparison.
    pub fn observe(
        &self,
        machine_id: &MachineId,
        agent_version: Option<&str>,
    ) -> Option<VersionNote> {
        let mut last_reported = self
            .last_reported
            .lock()
            .expect("agent version mutex poisoned");
        let current = agent_version.map(str::to_string);
        if last_reported.get(machine_id) == Some(&current) {
            return None;
        }
        last_reported.insert(machine_id.clone(), current);
        match agent_version {
            None => Some(VersionNote::Predates),
            Some(version) if version != self.server_version => Some(VersionNote::Mismatch {
                agent_version: version.to_string(),
            }),
            Some(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mismatch(version: &str) -> Option<VersionNote> {
        Some(VersionNote::Mismatch {
            agent_version: version.into(),
        })
    }

    #[test]
    fn a_mismatched_agent_is_noted_once_until_its_version_changes() {
        let tracker = AgentVersionTracker::new("1.2.0");
        let m1 = MachineId::new("m1").unwrap();
        assert_eq!(tracker.observe(&m1, Some("1.1.0")), mismatch("1.1.0"));
        assert_eq!(tracker.observe(&m1, Some("1.1.0")), None);
        assert_eq!(tracker.observe(&m1, Some("1.1.1")), mismatch("1.1.1"));
        assert_eq!(tracker.observe(&m1, Some("1.1.1")), None);
    }

    #[test]
    fn a_matching_agent_is_not_noted() {
        let tracker = AgentVersionTracker::new("1.2.0");
        let m1 = MachineId::new("m1").unwrap();
        assert_eq!(tracker.observe(&m1, Some("1.2.0")), None);
        assert_eq!(tracker.observe(&m1, Some("1.1.0")), mismatch("1.1.0"));
        assert_eq!(tracker.observe(&m1, Some("1.2.0")), None);
    }

    #[test]
    fn versions_compare_as_exact_strings() {
        let tracker = AgentVersionTracker::new("1.2.0");
        let m1 = MachineId::new("m1").unwrap();
        assert_eq!(tracker.observe(&m1, Some("v1.2.0")), mismatch("v1.2.0"));
    }

    #[test]
    fn an_agent_without_a_version_is_noted_once() {
        let tracker = AgentVersionTracker::new("1.2.0");
        let m1 = MachineId::new("m1").unwrap();
        assert_eq!(tracker.observe(&m1, None), Some(VersionNote::Predates));
        assert_eq!(tracker.observe(&m1, None), None);
    }

    #[test]
    fn machines_are_tracked_separately() {
        let tracker = AgentVersionTracker::new("1.2.0");
        assert_eq!(
            tracker.observe(&MachineId::new("m1").unwrap(), Some("1.1.0")),
            mismatch("1.1.0")
        );
        assert_eq!(
            tracker.observe(&MachineId::new("m2").unwrap(), Some("1.1.0")),
            mismatch("1.1.0")
        );
    }
}
