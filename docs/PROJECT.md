# Project record

## Objective

Replicated in-memory cache cluster with consistent hashing, SWIM-style gossip membership, quorum reads and writes and rebalancing, tested under a seeded fault-injecting network simulator.

## Scope

- The cluster logic, including `CacheNode` membership (SWIM), replication (quorum), and rebalancing.
- An in-process simulated network (`SimulatedNetwork`) with virtual time, fault injection, and deterministic event traces.
- The public API documented in [README.md](../README.md).

## Out-of-scope

- A TCP or UDP transport (this is library-only).
- Persistence to disk.
- Deletes with tombstones.
- Hinted handoff or Merkle tree anti-entropy.
- Byzantine fault tolerance.
- A client protocol.
- Linearizability (only eventual consistency via last-writer-wins).

## Factory run u929 (measured from API usage)

| Date (UTC) | Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|---|
| 2026-11-17 | generation | google/gemini-4-argon | yes | 7401 | 28303 | 0 | 0 | 0.5957 |
| 2026-11-17 | fix-2 | google/gemini-3.8-flash | no | 262480 | 1786 | 0 | 1926189 | 0.3480 |
| 2026-11-17 | fix-4 | google/gemini-3.8-flash | no | 178169 | 1225 | 0 | 1082823 | 0.2194 |
| 2026-11-17 | audit | google/gemini-3.8-flash | no | 107158 | 1783 | 0 | 740194 | 0.1426 |
| 2026-11-17 | docs | google/gemini-3.1-flash-lite | no | 23031 | 1997 | 0 | 0 | 0.0088 |

Total: $1.3144. Prices: vertex line, as measured by its adapter. Providers serve the current version of each model id, so the date tells which version answered; versions, launch stages and price sources per line are in the factory's model register (docs/MODELS.md).
