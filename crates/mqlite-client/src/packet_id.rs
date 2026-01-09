//! Packet identifier allocation and tracking.
//!
//! Implements requirements from MQTT spec:
//! - [MQTT-2.3.1-2] Each time a Client sends a new packet it MUST assign a currently unused Packet Identifier.
//! - [MQTT-2.3.1-3] If a Client re-sends a particular Control Packet, then it MUST use the same Packet Identifier.

use std::collections::HashSet;

/// Manages packet identifier allocation.
///
/// Packet identifiers are 16-bit non-zero values used for QoS 1/2 PUBLISH,
/// SUBSCRIBE, and UNSUBSCRIBE packets. This allocator ensures identifiers
/// are not reused while still in-flight.
#[derive(Debug)]
pub struct PacketIdAllocator {
    /// Next ID to try allocating
    next_id: u16,
    /// Set of currently in-use IDs
    in_use: HashSet<u16>,
}

impl Default for PacketIdAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl PacketIdAllocator {
    /// Create a new packet ID allocator.
    pub fn new() -> Self {
        Self {
            next_id: 1,
            in_use: HashSet::new(),
        }
    }

    /// Allocate a new unused packet identifier.
    ///
    /// Returns `None` if all 65535 possible IDs are in use (extremely unlikely).
    pub fn allocate(&mut self) -> Option<u16> {
        // Fast path: next_id is available
        if !self.in_use.contains(&self.next_id) {
            let id = self.next_id;
            self.in_use.insert(id);
            self.advance_next();
            return Some(id);
        }

        // Slow path: search for an available ID
        let start = self.next_id;
        loop {
            self.advance_next();
            if self.next_id == start {
                // Wrapped around completely, all IDs in use
                return None;
            }
            if !self.in_use.contains(&self.next_id) {
                let id = self.next_id;
                self.in_use.insert(id);
                self.advance_next();
                return Some(id);
            }
        }
    }

    /// Release a packet identifier after the transaction completes.
    ///
    /// Call this when:
    /// - QoS 1: PUBACK received
    /// - QoS 2: PUBCOMP received
    /// - SUBSCRIBE: SUBACK received
    /// - UNSUBSCRIBE: UNSUBACK received
    pub fn release(&mut self, id: u16) {
        self.in_use.remove(&id);
    }

    /// Check if a packet identifier is currently in use.
    pub fn is_in_use(&self, id: u16) -> bool {
        self.in_use.contains(&id)
    }

    /// Get the number of packet IDs currently in use.
    pub fn in_use_count(&self) -> usize {
        self.in_use.len()
    }

    /// Clear all allocations (used on clean session connect).
    pub fn clear(&mut self) {
        self.in_use.clear();
        self.next_id = 1;
    }

    /// Advance next_id, skipping 0.
    fn advance_next(&mut self) {
        self.next_id = self.next_id.wrapping_add(1);
        if self.next_id == 0 {
            self.next_id = 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequential_allocation() {
        let mut alloc = PacketIdAllocator::new();
        assert_eq!(alloc.allocate(), Some(1));
        assert_eq!(alloc.allocate(), Some(2));
        assert_eq!(alloc.allocate(), Some(3));
    }

    #[test]
    fn test_release_and_reuse() {
        let mut alloc = PacketIdAllocator::new();
        let id1 = alloc.allocate().unwrap();
        let id2 = alloc.allocate().unwrap();

        assert!(alloc.is_in_use(id1));
        assert!(alloc.is_in_use(id2));

        alloc.release(id1);
        assert!(!alloc.is_in_use(id1));
        assert!(alloc.is_in_use(id2));
    }

    #[test]
    fn test_skips_zero() {
        let mut alloc = PacketIdAllocator::new();
        alloc.next_id = 65535;
        let id = alloc.allocate().unwrap();
        assert_eq!(id, 65535);
        let id = alloc.allocate().unwrap();
        assert_eq!(id, 1); // Skipped 0
    }

    #[test]
    fn test_clear() {
        let mut alloc = PacketIdAllocator::new();
        alloc.allocate();
        alloc.allocate();
        assert_eq!(alloc.in_use_count(), 2);

        alloc.clear();
        assert_eq!(alloc.in_use_count(), 0);
        assert_eq!(alloc.allocate(), Some(1));
    }
}
