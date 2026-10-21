use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A minimal circuit breaker for protecting a shared downstream (NPM).
///
/// It trips after `threshold` consecutive failures and stays "open" for
/// `cooldown`, during which callers should skip the protected work instead of
/// piling more load onto a struggling dependency.  Any success resets the
/// failure count.  Time is passed in explicitly so the logic is testable
/// without sleeping.
pub struct CircuitBreaker {
    threshold: u32,
    cooldown: Duration,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    consecutive_failures: u32,
    open_until: Option<Instant>,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, cooldown: Duration) -> Self {
        Self {
            threshold,
            cooldown,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// True while the breaker is tripped and the cooldown has not yet elapsed.
    pub fn is_open(&self, now: Instant) -> bool {
        self.open_until(now).is_some()
    }

    /// When the cooldown ends while the breaker is open, or `None` when it is
    /// closed.  Reads the state without changing it.
    pub fn open_until(&self, now: Instant) -> Option<Instant> {
        let inner = self.inner.lock().expect("breaker mutex poisoned");
        inner.open_until.filter(|until| now < *until)
    }

    /// Record a successful call: clears any failures and closes the breaker.
    pub fn record_success(&self) {
        let mut inner = self.inner.lock().expect("breaker mutex poisoned");
        inner.consecutive_failures = 0;
        inner.open_until = None;
    }

    /// Record a failed call.  Once `threshold` consecutive failures accumulate,
    /// the breaker opens for `cooldown` (and the counter resets, so a fresh run
    /// of failures is needed to re-open after the cooldown elapses).
    pub fn record_failure(&self, now: Instant) {
        let mut inner = self.inner.lock().expect("breaker mutex poisoned");
        inner.consecutive_failures += 1;
        if inner.consecutive_failures >= self.threshold {
            inner.open_until = Some(now + self.cooldown);
            inner.consecutive_failures = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stays_closed_below_threshold() {
        let b = CircuitBreaker::new(3, Duration::from_secs(60));
        let t0 = Instant::now();
        b.record_failure(t0);
        b.record_failure(t0);
        assert!(
            !b.is_open(t0),
            "two failures with threshold 3 must stay closed"
        );
    }

    #[test]
    fn opens_after_threshold_consecutive_failures() {
        let b = CircuitBreaker::new(3, Duration::from_secs(60));
        let t0 = Instant::now();
        b.record_failure(t0);
        b.record_failure(t0);
        b.record_failure(t0);
        assert!(b.is_open(t0), "third failure must open the breaker");
    }

    #[test]
    fn closes_again_after_cooldown_elapses() {
        let b = CircuitBreaker::new(2, Duration::from_secs(60));
        let t0 = Instant::now();
        b.record_failure(t0);
        b.record_failure(t0);
        assert!(b.is_open(t0 + Duration::from_secs(59)));
        assert!(!b.is_open(t0 + Duration::from_secs(61)), "cooldown elapsed");
    }

    #[test]
    fn reports_when_the_cooldown_ends_while_open() {
        let b = CircuitBreaker::new(2, Duration::from_secs(60));
        let t0 = Instant::now();
        assert_eq!(b.open_until(t0), None, "a fresh breaker is closed");
        b.record_failure(t0);
        b.record_failure(t0);
        assert_eq!(
            b.open_until(t0 + Duration::from_secs(10)),
            Some(t0 + Duration::from_secs(60))
        );
        assert_eq!(b.open_until(t0 + Duration::from_secs(61)), None);
    }

    #[test]
    fn success_resets_failure_count() {
        let b = CircuitBreaker::new(3, Duration::from_secs(60));
        let t0 = Instant::now();
        b.record_failure(t0);
        b.record_failure(t0);
        b.record_success();
        // Only two failures since the reset: still closed.
        b.record_failure(t0);
        b.record_failure(t0);
        assert!(!b.is_open(t0));
    }
}
