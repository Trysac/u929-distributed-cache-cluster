package io.github.trysac.distributed_cache_cluster;

/**
 * Fault injection parameters for {@link SimulatedNetwork}.
 *
 * @param dropRate probability in {@code [0.0, 1.0]} that any non-local message is dropped
 * @param maxDelayTicks maximum extra virtual time ticks added to message delivery ({@code >= 0})
 */
public record NetworkFaults(double dropRate, int maxDelayTicks) {

  /** Validates fault parameters. */
  public NetworkFaults {
    if (Double.isNaN(dropRate) || dropRate < 0.0 || dropRate > 1.0) {
      throw new IllegalArgumentException("dropRate must be between 0.0 and 1.0");
    }
    if (maxDelayTicks < 0) {
      throw new IllegalArgumentException("maxDelayTicks must be >= 0");
    }
  }

  /**
   * Returns a fault configuration with zero message loss and zero extra delay.
   *
   * @return reliable {@link NetworkFaults}
   */
  public static NetworkFaults none() {
    return new NetworkFaults(0.0, 0);
  }
}
