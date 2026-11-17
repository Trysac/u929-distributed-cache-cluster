# distributed-cache-cluster

Teams that build a replicated cache learn that the hard part is not the hash map but membership, replica placement and rebalancing when a node dies, and those bugs only show under partitions and message loss. Testing that on real machines is slow and not reproducible. This library implements the cluster logic against a transport interface and ships a deterministic simulated network, so every fault scenario is a seeded, replayable test.

## Installation

```xml
<dependency>
  <groupId>io.github.trysac</groupId>
  <artifactId>distributed-cache-cluster</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Usage

```java
SimulatedNetwork net = new SimulatedNetwork(42L, new NetworkFaults(0.0, 0));
ClusterConfig cfg = new ClusterConfig(3, 2, 2, 128, 3, 5, 100_000, 10);
CacheNode n1 = new CacheNode("n1", net, cfg);
CacheNode n2 = new CacheNode("n2", net, cfg);
CacheNode n3 = new CacheNode("n3", net, cfg);
n1.join("n1");
n2.join("n1");
n3.join("n1");
for (int i = 0; i < 10; i++) net.tick();
```

## Public API

- **`CacheNode`**: Core interface for cache operations. 
    - `put(byte[] key, byte[] value)`: Writes data (limited to 1,024 byte keys and 1,048,576 byte values). Throws `CacheException` (e.g., `key_too_large`, `value_too_large`, `quorum_unavailable`, `node_full`, `not_joined`).
    - `get(byte[] key)`: Reads data, returns `Optional<byte[]>`. 
- **`SimulatedNetwork`**: Deterministic transport implementation. Use `tick()` to advance simulation time, `partition()` to create splits, and `crash()` to simulate failures.
- **`HashRing`**: Helper for consistent hashing management.
- **`ClusterConfig`**: Immutable configuration for N, W, R, virtual nodes, and timeout settings.

## Why not Hazelcast or Infinispan?

They are powerful, heavy, and complex. This is a small, readable implementation whose fault scenarios are deterministic tests, ideal for understanding distributed systems internals.

See [docs/SECURITY.md](docs/SECURITY.md) for vulnerability reporting and security model.