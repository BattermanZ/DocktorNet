use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use docktornet_core::MachineId;

/// Decides which agent owns a machine id.  Within the window, the first
/// source IP and forward host seen for a machine id own it.  A push from
/// another IP that carries another forward host is a second agent claiming
/// the same id, a cloned VM for example, and is rejected: accepting it would
/// rewrite the machine's proxy hosts and DNS records to its forward host on
/// every push.
pub struct FlappingTracker {
    owners: Mutex<HashMap<MachineId, Owner>>,
    window: Duration,
}

struct Owner {
    ip: IpAddr,
    forward_host: String,
    /// When the owner last pushed.  Pushes from other IPs never move it.
    seen: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Accept the push.  `prev_ip` is the owner's source IP when the push came
    /// from another IP with the same forward host.
    Accept { prev_ip: Option<IpAddr> },
    /// Reject the push: another agent owns the machine id.
    Reject {
        owner_ip: IpAddr,
        owner_forward_host: String,
    },
}

impl FlappingTracker {
    pub fn new(window: Duration) -> Self {
        Self {
            owners: Mutex::new(HashMap::new()),
            window,
        }
    }

    /// Judge a push for `machine_id` from `ip` carrying `forward_host` at
    /// `now`.  A push from the owner's IP, or one that takes ownership,
    /// becomes the owner's latest; any other push leaves the owner as it was.
    pub fn observe(
        &self,
        machine_id: &MachineId,
        ip: IpAddr,
        forward_host: &str,
        now: Instant,
    ) -> Verdict {
        let mut owners = self.owners.lock().expect("flapping mutex poisoned");
        // Evict owners silent for the window, so the map cannot grow
        // unbounded as distinct machine ids come and go over time, and so the
        // next push for an evicted id takes ownership.
        owners.retain(|_, owner| now.duration_since(owner.seen) < self.window);
        match owners.get(machine_id) {
            Some(owner) if owner.ip != ip && owner.forward_host != forward_host => {
                return Verdict::Reject {
                    owner_ip: owner.ip,
                    owner_forward_host: owner.forward_host.clone(),
                };
            }
            // Accepted, but the owner stays the first pair: only its own
            // pushes keep the id or change its forward host.
            Some(owner) if owner.ip != ip => {
                return Verdict::Accept {
                    prev_ip: Some(owner.ip),
                };
            }
            _ => {}
        }
        owners.insert(
            machine_id.clone(),
            Owner {
                ip,
                forward_host: forward_host.to_string(),
                seen: now,
            },
        );
        Verdict::Accept { prev_ip: None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn m1() -> MachineId {
        MachineId::new("m1").unwrap()
    }

    const ACCEPT: Verdict = Verdict::Accept { prev_ip: None };

    #[test]
    fn first_observation_is_accepted() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        assert_eq!(
            t.observe(&m1(), ip("10.0.0.1"), "fh1", Instant::now()),
            ACCEPT
        );
    }

    #[test]
    fn same_ip_is_accepted() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        let now = Instant::now();
        t.observe(&m1(), ip("10.0.0.1"), "fh1", now);
        assert_eq!(
            t.observe(&m1(), ip("10.0.0.1"), "fh1", now + Duration::from_secs(10)),
            ACCEPT
        );
    }

    #[test]
    fn new_ip_with_same_forward_host_is_accepted_naming_the_previous_ip() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        let now = Instant::now();
        t.observe(&m1(), ip("10.0.0.1"), "fh1", now);
        assert_eq!(
            t.observe(&m1(), ip("10.0.0.2"), "fh1", now + Duration::from_secs(30)),
            Verdict::Accept {
                prev_ip: Some(ip("10.0.0.1"))
            }
        );
    }

    #[test]
    fn a_new_ip_with_the_same_forward_host_does_not_take_the_id() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        let now = Instant::now();
        t.observe(&m1(), ip("10.0.0.1"), "fh1", now);
        t.observe(&m1(), ip("10.0.0.2"), "fh1", now + Duration::from_secs(5));
        // The first pair still owns the id, so its own change is accepted.
        assert_eq!(
            t.observe(&m1(), ip("10.0.0.1"), "fh2", now + Duration::from_secs(10)),
            ACCEPT
        );
    }

    #[test]
    fn new_ip_with_new_forward_host_within_window_is_rejected() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        let now = Instant::now();
        t.observe(&m1(), ip("10.0.0.1"), "fh1", now);
        assert_eq!(
            t.observe(&m1(), ip("10.0.0.2"), "fh2", now + Duration::from_secs(30)),
            Verdict::Reject {
                owner_ip: ip("10.0.0.1"),
                owner_forward_host: "fh1".into()
            }
        );
        // Ownership did not change: the owner is still accepted.
        assert_eq!(
            t.observe(&m1(), ip("10.0.0.1"), "fh1", now + Duration::from_secs(31)),
            ACCEPT
        );
    }

    #[test]
    fn owner_changing_its_forward_host_is_accepted() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        let now = Instant::now();
        t.observe(&m1(), ip("10.0.0.1"), "fh1", now);
        assert_eq!(
            t.observe(&m1(), ip("10.0.0.1"), "fh2", now + Duration::from_secs(5)),
            ACCEPT
        );
    }

    #[test]
    fn rejected_pushes_do_not_keep_the_window_open() {
        let window = Duration::from_secs(300);
        let t = FlappingTracker::new(window);
        let now = Instant::now();
        t.observe(&m1(), ip("10.0.0.1"), "fh1", now);
        for secs in [10, 100, 200, 299] {
            assert!(matches!(
                t.observe(
                    &m1(),
                    ip("10.0.0.2"),
                    "fh2",
                    now + Duration::from_secs(secs)
                ),
                Verdict::Reject { .. }
            ));
        }
        // The owner has been silent for a full window: the other agent takes over.
        assert_eq!(
            t.observe(&m1(), ip("10.0.0.2"), "fh2", now + window),
            ACCEPT
        );
        assert!(matches!(
            t.observe(
                &m1(),
                ip("10.0.0.1"),
                "fh1",
                now + window + Duration::from_secs(1)
            ),
            Verdict::Reject { .. }
        ));
    }

    #[test]
    fn an_owner_that_keeps_pushing_keeps_the_id() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        let now = Instant::now();
        t.observe(&m1(), ip("10.0.0.1"), "fh1", now);
        t.observe(&m1(), ip("10.0.0.1"), "fh1", now + Duration::from_secs(200));
        assert!(matches!(
            t.observe(&m1(), ip("10.0.0.2"), "fh2", now + Duration::from_secs(400)),
            Verdict::Reject { .. }
        ));
    }

    #[test]
    fn stale_entries_are_evicted() {
        let window = Duration::from_secs(60);
        let t = FlappingTracker::new(window);
        let base = Instant::now();
        // Observe 100 distinct machine ids, each spaced well beyond the
        // window so every prior entry is stale by the time the next arrives.
        for i in 0..100u64 {
            let id = MachineId::new(format!("m{i}")).unwrap();
            t.observe(
                &id,
                ip("10.0.0.1"),
                "fh",
                base + Duration::from_secs(i * 120),
            );
        }
        let len = t.owners.lock().unwrap().len();
        assert!(
            len <= 2,
            "expected stale entries to be evicted, but map holds {len} entries"
        );
    }

    #[test]
    fn machines_are_isolated() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        let now = Instant::now();
        t.observe(&m1(), ip("10.0.0.1"), "fh1", now);
        assert_eq!(
            t.observe(
                &MachineId::new("m2").unwrap(),
                ip("10.0.0.2"),
                "fh2",
                now + Duration::from_secs(1)
            ),
            ACCEPT
        );
    }
}
