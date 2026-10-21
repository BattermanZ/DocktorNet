use serde::{Deserialize, Serialize};

/// The name an agent reports its machine under.  It is the first half of every
/// service id (`machine/container`), so it is non-empty and holds no `/`, no
/// whitespace and no control characters.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct MachineId(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidMachineId {
    #[error("machine id is empty")]
    Empty,
    #[error("machine id {0:?} contains '/', whitespace or a control character")]
    BadChar(String),
}

impl MachineId {
    pub fn new(s: impl Into<String>) -> Result<Self, InvalidMachineId> {
        let s = s.into();
        if s.is_empty() {
            return Err(InvalidMachineId::Empty);
        }
        if s.chars()
            .any(|c| c == '/' || c.is_whitespace() || c.is_control())
        {
            return Err(InvalidMachineId::BadChar(s));
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for MachineId {
    type Error = InvalidMachineId;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
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
        let m = MachineId::new("batterdock").unwrap();
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
    fn machine_id_rejects_ids_that_break_the_service_id() {
        for bad in ["", "a/b", "a b", "a\u{7}b", "a\tb", "a\u{a0}b"] {
            assert!(MachineId::new(bad).is_err(), "{bad:?} must be rejected");
        }
        assert_eq!(
            MachineId::new("batter-dock.1").unwrap().as_str(),
            "batter-dock.1"
        );
    }

    #[test]
    fn deserializing_an_invalid_machine_id_fails() {
        for bad in [r#""""#, r#""a/b""#, r#""a b""#, r#""a\u0001b""#] {
            assert!(
                serde_json::from_str::<MachineId>(bad).is_err(),
                "{bad} must not deserialize"
            );
        }
    }

    #[test]
    fn machine_id_roundtrips_json() {
        let m = MachineId::new("batterdock").unwrap();
        let s = serde_json::to_string(&m).unwrap();
        let back: MachineId = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
    }
}
