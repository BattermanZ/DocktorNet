use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use docktornet_core::MachineId;

pub struct FlappingTracker {
    last_seen: Mutex<HashMap<MachineId, (IpAddr, Instant)>>,
    window: Duration,
}

impl FlappingTracker {
    pub fn new(window: Duration) -> Self {
        Self {
            last_seen: Mutex::new(HashMap::new()),
            window,
        }
    }

    /// Record an observation of `machine_id` from `ip` at `now`.
    /// Returns `Some(previous_ip)` if a different IP was seen for the
    /// same machine within the window, else `None`.
    pub fn observe(&self, machine_id: &MachineId, ip: IpAddr, now: Instant) -> Option<IpAddr> {
        let mut map = self.last_seen.lock().expect("flapping mutex poisoned");
        // Evict entries older than the window so the map cannot grow
        // unbounded as distinct machine ids come and go over time.
        map.retain(|_, (_, seen)| now.duration_since(*seen) < self.window);
        let prev = map.insert(machine_id.clone(), (ip, now));
        match prev {
            Some((prev_ip, prev_seen))
                if prev_ip != ip && now.duration_since(prev_seen) < self.window =>
            {
                Some(prev_ip)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn first_observation_returns_none() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        assert_eq!(
            t.observe(&MachineId::new("m1"), ip("10.0.0.1"), Instant::now()),
            None
        );
    }

    #[test]
    fn same_ip_returns_none() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        let id = MachineId::new("m1");
        let now = Instant::now();
        t.observe(&id, ip("10.0.0.1"), now);
        assert_eq!(
            t.observe(&id, ip("10.0.0.1"), now + Duration::from_secs(10)),
            None
        );
    }

    #[test]
    fn different_ip_within_window_returns_previous() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        let id = MachineId::new("m1");
        let now = Instant::now();
        t.observe(&id, ip("10.0.0.1"), now);
        let prev = t.observe(&id, ip("10.0.0.2"), now + Duration::from_secs(30));
        assert_eq!(prev, Some(ip("10.0.0.1")));
    }

    #[test]
    fn different_ip_after_window_returns_none() {
        let t = FlappingTracker::new(Duration::from_secs(60));
        let id = MachineId::new("m1");
        let now = Instant::now();
        t.observe(&id, ip("10.0.0.1"), now);
        assert_eq!(
            t.observe(&id, ip("10.0.0.2"), now + Duration::from_secs(120)),
            None
        );
    }

    #[test]
    fn stale_entries_are_evicted() {
        let window = Duration::from_secs(60);
        let t = FlappingTracker::new(window);
        let base = Instant::now();
        // Observe 100 distinct machine ids, each spaced well beyond the
        // window so every prior entry is stale by the time the next arrives.
        for i in 0..100u64 {
            let id = MachineId::new(format!("m{i}"));
            t.observe(&id, ip("10.0.0.1"), base + Duration::from_secs(i * 120));
        }
        // The map must not retain every machine id ever seen; stale entries
        // (older than the window) should have been evicted.
        let len = t.last_seen.lock().unwrap().len();
        assert!(
            len <= 2,
            "expected stale entries to be evicted, but map holds {len} entries"
        );
    }

    #[test]
    fn machines_are_isolated() {
        let t = FlappingTracker::new(Duration::from_secs(300));
        let now = Instant::now();
        t.observe(&MachineId::new("m1"), ip("10.0.0.1"), now);
        assert_eq!(
            t.observe(
                &MachineId::new("m2"),
                ip("10.0.0.2"),
                now + Duration::from_secs(1)
            ),
            None
        );
    }
}
