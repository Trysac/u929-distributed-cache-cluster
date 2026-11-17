# Deterministic Virtual-Time Transport and SWIM Gossip Membership

Status: accepted (generated, pending human review)

## Context

Testing distributed cache membership, quorum replication, and rebalancing over real sockets and wall-clock timers is slow and non-deterministic under packet loss, delays, partitions, and node crashes.

## Decision

Decouple CacheNode protocol state from I/O through a synchronous Transport interface and drive all timeouts, SWIM probe rounds, indirect ping-req probes, and 100-key rebalance batches from explicit virtual ticks in SimulatedNetwork seeded by a deterministic PRNG.

## Consequences

Every fault scenario—including 200-seed Jepsen-lite runs with packet drops, delays, crashes, and partitions—executes deterministically in milliseconds with bit-identical event traces for a given seed, at the cost of requiring an external transport adapter for real network deployment.
