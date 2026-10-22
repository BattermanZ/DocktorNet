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

/// A 64-bit FNV-1a hash of the exact service id.  It tells apart service ids
/// that [`sanitize_service_id`] slugs the same, in Gatus filenames and Caddy
/// route markers, so it must never change.
pub fn stable_service_hash(service_id: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001b3;

    service_id.as_bytes().iter().fold(FNV_OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

/// How every Caddy route marker starts.  The audit finds docktornet's routes
/// by it.
pub const CADDY_ROUTE_MARKER_PREFIX: &str = "docktornet--";

/// The ownership marker of a service's Caddy route at `index`: the route's
/// `@id` and its address under Caddy's `/id/` admin endpoint (ADR 0001).  It
/// holds only ASCII alphanumerics, `_` and `-`, so it is a URL path segment
/// as it stands.  It can't be read back into a service id.
pub fn caddy_route_marker(service_id: &str, index: u8) -> String {
    format!(
        "{CADDY_ROUTE_MARKER_PREFIX}{}--{:016x}--{index}",
        sanitize_service_id(service_id),
        stable_service_hash(service_id)
    )
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
    fn caddy_route_marker_slugs_the_service_and_appends_hash_and_index() {
        // FNV-1a 64 of "batter-dock/my.app", the hash Gatus filenames use.
        assert_eq!(
            caddy_route_marker("batter-dock/my.app", 3),
            "docktornet--batter_dock__my_app--124aa377e42a99c1--3"
        );
    }

    #[test]
    fn caddy_route_markers_differ_for_service_ids_that_slug_the_same() {
        let a = caddy_route_marker("m-1/app", 0);
        let b = caddy_route_marker("m.1/app", 0);
        assert!(a.starts_with("docktornet--m_1__app--"), "{a}");
        assert!(b.starts_with("docktornet--m_1__app--"), "{b}");
        assert_ne!(a, b);
    }

    #[test]
    fn caddy_route_marker_is_a_plain_path_segment_for_any_machine_id() {
        for machine in ["a#b", "a?b", "a%2Fb", "a:b", "ü"] {
            let marker = caddy_route_marker(&format!("{machine}/app"), 0);
            assert!(
                marker
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
                "{machine}: {marker}"
            );
        }
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
