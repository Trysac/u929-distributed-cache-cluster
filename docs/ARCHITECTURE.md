# Architecture

## Purpose

`io.github.trysac:distributed-cache-cluster` is a small, dependency-free Java library: Replicated in-memory cache cluster with consistent hashing, SWIM-style gossip membership, quorum reads and writes and rebalancing, tested under a seeded fault-injecting network simulator

## Shape

```text
caller code --> package io.github.trysac.distributed_cache_cluster (src/main/java)
                    |
                    +-- final classes with static, pure functions; no I/O, no global mutable state
                    +-- package-private helpers (optional)
```

- **Public API**: every `public` class and member of the package. Package-private classes may
  change without a major version.
- **Errors**: invalid input throws an `IllegalArgumentException` subclass of the library (such as
  `BlankInputException`) documented with `@throws`; null arguments throw `NullPointerException`.
  The library never prints or exits.
- **Side effects**: none. If a feature needs I/O, accept an `InputStream`, a `Reader` or a small
  interface instead of opening resources, and record the choice in an ADR.

## Quality contract

`scripts/Check.java` is the single definition of the gates; `fabrica.json` tells the factory and CI
how to run it. See [TESTING.md](TESTING.md).

## Decisions

Architecture decisions are recorded in [decisions/](decisions/) as numbered ADRs.
