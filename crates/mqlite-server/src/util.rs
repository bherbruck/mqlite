//! Utility types and helpers.

use std::time::{Duration, Instant};

/// A counter that rate-limits based on time intervals.
///
/// Accumulates counts and only signals "ready to log" after the interval passes.
/// Useful for rate-limiting log spam while preserving total counts.
#[derive(Debug)]
pub struct RateLimitedCounter {
    count: u64,
    last_reset: Instant,
    interval: Duration,
}

impl RateLimitedCounter {
    /// Create a new counter with the given interval.
    pub fn new(interval: Duration) -> Self {
        Self {
            count: 0,
            last_reset: Instant::now(),
            interval,
        }
    }

    /// Increment by 1 and return Some(count) if interval has passed.
    /// Resets count and timestamp when returning Some.
    #[inline]
    #[allow(dead_code)]
    pub fn increment(&mut self) -> Option<u64> {
        self.increment_by(1)
    }

    /// Increment by N and return Some(count) if interval has passed.
    /// Resets count and timestamp when returning Some.
    #[inline]
    pub fn increment_by(&mut self, n: u64) -> Option<u64> {
        self.count += n;

        if self.last_reset.elapsed() >= self.interval {
            let count = self.count;
            self.count = 0;
            self.last_reset = Instant::now();
            Some(count)
        } else {
            None
        }
    }

    /// Get current accumulated count without resetting.
    #[inline]
    #[allow(dead_code)]
    pub fn current_count(&self) -> u64 {
        self.count
    }
}

/// Tracks outgoing quota for MQTT 5 flow control.
///
/// Limits concurrent QoS 1/2 messages that can be sent to a client.
/// Quota is consumed when sending, restored when ACK received.
#[derive(Debug, Clone)]
pub struct QuotaTracker {
    /// Maximum quota (from client's receive_maximum).
    max_quota: u16,
    /// Current available quota.
    current: u16,
}

impl QuotaTracker {
    /// Create a new quota tracker with the given maximum.
    /// A max of 0 is treated as 65535 (MQTT spec default).
    pub fn new(max_quota: u16) -> Self {
        let max = if max_quota == 0 { 65535 } else { max_quota };
        Self {
            max_quota: max,
            current: max,
        }
    }

    /// Update the maximum quota (e.g., from CONNECT properties).
    /// Resets current quota to the new maximum.
    pub fn set_max(&mut self, max_quota: u16) {
        let max = if max_quota == 0 { 65535 } else { max_quota };
        self.max_quota = max;
        self.current = max;
    }

    /// Check if quota is available.
    #[inline]
    #[allow(dead_code)]
    pub fn has_quota(&self) -> bool {
        self.current > 0
    }

    /// Consume one quota slot. Returns false if no quota available.
    #[inline]
    pub fn consume(&mut self) -> bool {
        if self.current > 0 {
            self.current -= 1;
            true
        } else {
            false
        }
    }

    /// Restore one quota slot (on ACK received).
    #[inline]
    pub fn restore(&mut self) {
        if self.current < self.max_quota {
            self.current += 1;
        }
    }

    /// Get current available quota.
    #[inline]
    #[allow(dead_code)]
    pub fn current(&self) -> u16 {
        self.current
    }

    /// Get maximum quota.
    #[inline]
    #[allow(dead_code)]
    pub fn max(&self) -> u16 {
        self.max_quota
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_increment_before_interval() {
        let mut counter = RateLimitedCounter::new(Duration::from_secs(10));
        assert!(counter.increment().is_none());
        assert!(counter.increment().is_none());
        assert_eq!(counter.current_count(), 2);
    }

    #[test]
    fn test_increment_after_interval() {
        let mut counter = RateLimitedCounter::new(Duration::ZERO);
        assert_eq!(counter.increment(), Some(1));
        assert_eq!(counter.increment(), Some(1));
    }

    #[test]
    fn test_increment_by() {
        let mut counter = RateLimitedCounter::new(Duration::ZERO);
        assert_eq!(counter.increment_by(5), Some(5));
        counter.increment_by(3);
        counter.increment_by(2);
        // With 0 interval, each call triggers
        assert_eq!(counter.current_count(), 0); // Was just reset
    }

    #[test]
    fn test_accumulation() {
        let mut counter = RateLimitedCounter::new(Duration::from_secs(1));
        counter.increment();
        counter.increment();
        counter.increment();
        assert_eq!(counter.current_count(), 3);

        // Wait for interval
        sleep(Duration::from_millis(1100));
        assert_eq!(counter.increment(), Some(4)); // 3 + 1
        assert_eq!(counter.current_count(), 0);
    }

    // QuotaTracker tests

    #[test]
    fn test_quota_new_default() {
        let quota = QuotaTracker::new(65535);
        assert_eq!(quota.max(), 65535);
        assert_eq!(quota.current(), 65535);
        assert!(quota.has_quota());
    }

    #[test]
    fn test_quota_zero_treated_as_default() {
        // MQTT spec: receive_maximum of 0 is protocol error, treat as 65535
        let quota = QuotaTracker::new(0);
        assert_eq!(quota.max(), 65535);
        assert_eq!(quota.current(), 65535);
    }

    #[test]
    fn test_quota_consume_and_restore() {
        let mut quota = QuotaTracker::new(3);
        assert_eq!(quota.current(), 3);

        // Consume all quota
        assert!(quota.consume());
        assert_eq!(quota.current(), 2);
        assert!(quota.consume());
        assert_eq!(quota.current(), 1);
        assert!(quota.consume());
        assert_eq!(quota.current(), 0);

        // No more quota
        assert!(!quota.has_quota());
        assert!(!quota.consume());
        assert_eq!(quota.current(), 0);

        // Restore one
        quota.restore();
        assert_eq!(quota.current(), 1);
        assert!(quota.has_quota());

        // Restore beyond max has no effect
        quota.restore();
        quota.restore();
        quota.restore();
        assert_eq!(quota.current(), 3);
        assert_eq!(quota.max(), 3);
    }

    #[test]
    fn test_quota_set_max() {
        let mut quota = QuotaTracker::new(10);
        quota.consume();
        quota.consume();
        assert_eq!(quota.current(), 8);

        // set_max resets current to new max
        quota.set_max(5);
        assert_eq!(quota.max(), 5);
        assert_eq!(quota.current(), 5);
    }

    #[test]
    fn test_quota_set_max_zero() {
        let mut quota = QuotaTracker::new(10);
        quota.set_max(0);
        assert_eq!(quota.max(), 65535);
        assert_eq!(quota.current(), 65535);
    }
}
