# Testing

## Coverage

- `ClusterAcceptanceTest`: Tests the cluster against acceptance criteria defined in the specification. Covers the Jepsen-lite property checker for various seeds, verifying 0 lost writes and 0 stale reads.
- Unit and property tests (`jqwik`): Verify consistent hashing invariants and the `HashRing` logic.
- Simulation tests: Reproducibility tests ensuring identical seeds produce identical traces (SHA-256 validation).

## Policy

- Line coverage must be at or above 80%.
- Tests are deterministic: no wall-clock time and no threading. All interaction with the cluster logic is via `SimulatedNetwork.tick()`.