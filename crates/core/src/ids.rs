use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MachineId(String);

impl MachineId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Returns the canonical service id `{machine_id}/{container}`.
pub fn service_id(machine: &MachineId, container: &str) -> String {
    format!("{}/{}", machine.as_str(), container)
}

/// Returns a filename-safe version of a service id.
/// Both components are slugified (any non-alphanumeric becomes `_`) and joined with `__`.
pub fn sanitize_service_id(id: &str) -> String {
    let (machine, container) = match id.split_once('/') {
        Some(pair) => pair,
        None => return slug(id),
    };
    format!("{}__{}", slug(machine), slug(container))
}

fn slug(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_id_joins_machine_and_container() {
        let m = MachineId::new("batterdock");
        assert_eq!(service_id(&m, "myapp"), "batterdock/myapp");
    }

    #[test]
    fn sanitize_replaces_slash_and_dots() {
        let id = sanitize_service_id("batter-dock/my.app");
        assert_eq!(id, "batter_dock__my_app");
    }

    #[test]
    fn sanitize_handles_missing_slash() {
        assert_eq!(sanitize_service_id("foo"), "foo");
    }

    #[test]
    fn machine_id_roundtrips_json() {
        let m = MachineId::new("batterdock");
        let s = serde_json::to_string(&m).unwrap();
        let back: MachineId = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
    }
}
