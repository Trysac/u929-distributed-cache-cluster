# Architecture

## Overview

`io.github.trysac.distributed_cache_cluster` implements a deterministic, in-process replicated cache cluster. It is decoupled from I/O via the `Transport` interface.

## Components

1. **`HashRing`**: Implements 64-bit FNV-1a hashing on `nodeId + '#' + index` to place nodes on the ring. It calculates the preference list of physical nodes for a given key.
2. **`SimulatedNetwork` & `Transport`**: A discrete-event simulator that provides virtual time. The `tick()` method drives the protocol, delivering messages and triggering rebalancing/probing.
3. **`CacheNode`**:
   - **SWIM Membership**: Uses `PING`/`PING_REQ` probes and piggybacked gossip updates to track node states (`alive`, `suspect`, `dead`).
   - **Quorum Replication**: Implements (N, W, R) quorums with last-writer-wins versioning (Lamport timestamp + writer ID). 
   - **Rebalancing**: Queues keys for migration in batches of 100 per tick when ring membership changes.