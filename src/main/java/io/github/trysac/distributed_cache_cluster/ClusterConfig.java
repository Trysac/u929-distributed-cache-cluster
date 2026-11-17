package io.github.trysac.distributed_cache_cluster;

/**
 * Configuration parameters for a {@link CacheNode}.
 *
 * @param replicas replication factor N (typically 3)
 * @param writeQuorum write quorum W (typically 2)
 * @param readQuorum read quorum R (typically 2)
 * @param virtualNodes virtual nodes per physical node on the hash ring (typically 128)
 * @param indirectProbes number of peers asked to send {@code ping-req} on probe failure (typically
 *     3)
 * @param suspicionRounds number of SWIM probe rounds a suspect node stays suspect before being
 *     marked dead (typically 5)
 * @param maxEntries maximum number of distinct keys stored on a single node (typically 100,000)
 * @param timeoutTicks number of virtual time ticks before an operation times out (typically 10)
 */
public record ClusterConfig(
    int replicas,
    int writeQuorum,
    int readQuorum,
    int virtualNodes,
    int indirectProbes,
    int suspicionRounds,
    int maxEntries,
    int timeoutTicks) {

  /** Validates configuration bounds. */
  public ClusterConfig {
    if (replicas < 1) {
      throw new IllegalArgumentException("replicas must be >= 1");
    }
    if (writeQuorum < 1 || writeQuorum > replicas) {
      throw new IllegalArgumentException("writeQuorum must be between 1 and replicas");
    }
    if (readQuorum < 1 || readQuorum > replicas) {
      throw new IllegalArgumentException("readQuorum must be between 1 and replicas");
    }
    if (virtualNodes < 1) {
      throw new IllegalArgumentException("virtualNodes must be >= 1");
    }
    if (indirectProbes < 0) {
      throw new IllegalArgumentException("indirectProbes must be >= 0");
    }
    if (suspicionRounds < 1) {
      throw new IllegalArgumentException("suspicionRounds must be >= 1");
    }
    if (maxEntries < 1) {
      throw new IllegalArgumentException("maxEntries must be >= 1");
    }
    if (timeoutTicks < 1) {
      throw new IllegalArgumentException("timeoutTicks must be >= 1");
    }
  }

  /**
   * Returns the default cluster configuration (N=3, W=2, R=2, virtualNodes=128, indirectProbes=3,
   * suspicionRounds=5, maxEntries=100_000, timeoutTicks=10).
   *
   * @return default {@link ClusterConfig}
   */
  public static ClusterConfig defaults() {
    return new ClusterConfig(3, 2, 2, 128, 3, 5, 100_000, 10);
  }
}
