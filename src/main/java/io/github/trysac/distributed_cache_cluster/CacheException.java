package io.github.trysac.distributed_cache_cluster;

import java.io.Serial;
import java.util.Objects;

/**
 * Runtime exception thrown by cache cluster operations with a stable machine-readable error code.
 */
public final class CacheException extends IllegalArgumentException {

  @Serial private static final long serialVersionUID = 1L;

  /** Error code when fewer than the required quorum of replicas respond or are available. */
  public static final String QUORUM_UNAVAILABLE = "quorum_unavailable";

  /** Error code when an operation exceeds its configured timeout in ticks. */
  public static final String TIMEOUT = "timeout";

  /** Error code when a key exceeds 1,024 bytes. */
  public static final String KEY_TOO_LARGE = "key_too_large";

  /** Error code when a value exceeds 1,048,576 bytes. */
  public static final String VALUE_TOO_LARGE = "value_too_large";

  /** Error code when a node has reached its configured {@code maxEntries} limit for new keys. */
  public static final String NODE_FULL = "node_full";

  /** Error code when a node has not joined a cluster yet. */
  public static final String NOT_JOINED = "not_joined";

  private final String code;

  /**
   * Creates a new {@link CacheException} with the given code and message.
   *
   * @param code stable machine-readable error code
   * @param message human-readable description (truncated to 200 characters)
   */
  public CacheException(String code, String message) {
    super(truncate(Objects.requireNonNull(message, "message")));
    this.code = Objects.requireNonNull(code, "code");
  }

  /**
   * Returns the machine-readable error code.
   *
   * @return one of {@code quorum_unavailable}, {@code timeout}, {@code key_too_large}, {@code
   *     value_too_large}, {@code node_full}, or {@code not_joined}
   */
  public String code() {
    return code;
  }

  private static String truncate(String msg) {
    return msg.length() <= 200 ? msg : msg.substring(0, 200);
  }
}
