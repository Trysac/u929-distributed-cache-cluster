package io.github.trysac.distributed_cache_cluster;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable protocol message exchanged between {@link CacheNode} instances over a {@link
 * Transport}.
 */
public final class Message {

  /** Type of protocol message. */
  public enum Type {
    /** Initial join request sent to a seed node. */
    JOIN,
    /** Join acknowledgment carrying current cluster membership. */
    JOIN_ACK,
    /** Direct SWIM probe. */
    PING,
    /** Direct SWIM probe acknowledgment. */
    ACK,
    /** Indirect SWIM probe request through an intermediary node. */
    PING_REQ,
    /** Replicated write request to a replica. */
    PUT_REQ,
    /** Replicated write response from a replica. */
    PUT_ACK,
    /** Replicated read request to a replica. */
    GET_REQ,
    /** Replicated read response from a replica. */
    GET_RESP,
    /** Asynchronous read-repair or rebalance key push. */
    REPAIR
  }

  /** Versioned value entry carried inside messages; immutable, with defensive copies of bytes. */
  public static final class Entry {
    private final byte[] key;
    private final byte[] value;
    private final long lamport;
    private final String writer;

    /**
     * Creates an entry, copying the given byte arrays.
     *
     * @param key key bytes
     * @param value value bytes, or {@code null} when the entry has no value
     * @param lamport Lamport timestamp of the write
     * @param writer nodeId that coordinated the write
     */
    public Entry(byte[] key, byte[] value, long lamport, String writer) {
      this.key = Objects.requireNonNull(key, "key").clone();
      this.value = value == null ? null : value.clone();
      this.lamport = lamport;
      this.writer = Objects.requireNonNull(writer, "writer");
    }

    /**
     * Returns the Lamport timestamp of the write.
     *
     * @return the Lamport timestamp
     */
    public long lamport() {
      return lamport;
    }

    /**
     * Returns the nodeId that coordinated the write.
     *
     * @return the writer nodeId
     */
    public String writer() {
      return writer;
    }

    /**
     * Returns a defensive copy of the key bytes.
     *
     * @return the key bytes
     */
    public byte[] key() {
      return key.clone();
    }

    /**
     * Returns a defensive copy of the value bytes, or {@code null} when the entry has no value.
     *
     * @return the value bytes, or {@code null}
     */
    public byte[] value() {
      return value == null ? null : value.clone();
    }

    /**
     * Compares two entries by version: higher {@code lamport} wins, ties broken by
     * lexicographically higher {@code writer}.
     *
     * @param other other entry
     * @return positive if this entry is newer, negative if older, 0 if identical version
     */
    public int compareVersion(Entry other) {
      int cmp = Long.compare(this.lamport, other.lamport);
      if (cmp != 0) {
        return cmp;
      }
      return this.writer.compareTo(other.writer);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Entry that)) {
        return false;
      }
      return lamport == that.lamport
          && Arrays.equals(key, that.key)
          && Arrays.equals(value, that.value)
          && writer.equals(that.writer);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(lamport, writer);
      result = 31 * result + Arrays.hashCode(key);
      result = 31 * result + Arrays.hashCode(value);
      return result;
    }

    @Override
    public String toString() {
      return "Entry[key="
          + Arrays.toString(key)
          + ", value="
          + Arrays.toString(value)
          + ", lamport="
          + lamport
          + ", writer="
          + writer
          + "]";
    }
  }

  /**
   * Piggybacked SWIM membership record.
   *
   * @param nodeId member node identifier
   * @param state {@code alive}, {@code suspect}, or {@code dead}
   * @param incarnation SWIM incarnation number
   */
  public record GossipUpdate(String nodeId, String state, long incarnation) {
    /** Validates non-null fields. */
    public GossipUpdate {
      Objects.requireNonNull(nodeId, "nodeId");
      Objects.requireNonNull(state, "state");
    }
  }

  private final Type type;
  private final String from;
  private final long requestId;
  private final String targetNodeId;
  private final List<Entry> entries;
  private final List<GossipUpdate> gossip;
  private final boolean ok;
  private final String errorCode;
  private final long clock;

  /**
   * Constructs a protocol message.
   *
   * @param type message type
   * @param from sender node identifier
   * @param requestId correlation identifier
   * @param targetNodeId target or subject node identifier (for indirect probes)
   * @param entries payload key/value entries
   * @param gossip piggybacked membership updates
   * @param ok whether the operation succeeded
   * @param errorCode optional error code when {@code !ok}
   */
  public Message(
      Type type,
      String from,
      long requestId,
      String targetNodeId,
      List<Entry> entries,
      List<GossipUpdate> gossip,
      boolean ok,
      String errorCode) {
    this(type, from, requestId, targetNodeId, entries, gossip, ok, errorCode, 0L);
  }

  private Message(
      Type type,
      String from,
      long requestId,
      String targetNodeId,
      List<Entry> entries,
      List<GossipUpdate> gossip,
      boolean ok,
      String errorCode,
      long clock) {
    this.type = Objects.requireNonNull(type, "type");
    this.from = Objects.requireNonNull(from, "from");
    this.requestId = requestId;
    this.targetNodeId = targetNodeId;
    this.entries = entries == null ? List.of() : List.copyOf(entries);
    this.gossip = gossip == null ? List.of() : List.copyOf(gossip);
    this.ok = ok;
    this.errorCode = errorCode;
    this.clock = clock;
  }

  /**
   * Returns a copy of this message stamped with the sender's Lamport clock at send time.
   *
   * @param senderClock the sender's Lamport clock
   * @return the stamped copy
   */
  Message withClock(long senderClock) {
    return new Message(
        type, from, requestId, targetNodeId, entries, gossip, ok, errorCode, senderClock);
  }

  /**
   * Returns the sender's Lamport clock when the message was sent, or {@code 0} when unstamped.
   *
   * @return the sender's Lamport clock
   */
  public long clock() {
    return clock;
  }

  /**
   * Returns the message type.
   *
   * @return message type
   */
  public Type type() {
    return type;
  }

  /**
   * Returns the sender node ID.
   *
   * @return sender node ID
   */
  public String from() {
    return from;
  }

  /**
   * Returns the correlation request ID.
   *
   * @return request ID
   */
  public long requestId() {
    return requestId;
  }

  /**
   * Returns the target node ID for indirect probes, or {@code null}.
   *
   * @return target node ID
   */
  public String targetNodeId() {
    return targetNodeId;
  }

  /**
   * Returns the immutable list of versioned key/value entries.
   *
   * @return entries
   */
  public List<Entry> entries() {
    return entries;
  }

  /**
   * Returns the piggybacked SWIM gossip updates.
   *
   * @return gossip updates
   */
  public List<GossipUpdate> gossip() {
    return gossip;
  }

  /**
   * Returns whether a replica response succeeded.
   *
   * @return true if ok
   */
  public boolean ok() {
    return ok;
  }

  /**
   * Returns the optional error code on failure.
   *
   * @return error code or {@code null}
   */
  public String errorCode() {
    return errorCode;
  }

  /**
   * Formats a compact deterministic summary for network traces.
   *
   * @return summary string
   */
  public String summary() {
    return type
        + "(req="
        + requestId
        + ",entries="
        + entries.size()
        + ",gossip="
        + gossip.size()
        + ",ok="
        + ok
        + (targetNodeId != null ? ",target=" + targetNodeId : "")
        + ")";
  }

  /**
   * Helper to convert a membership map and incarnation map into gossip updates.
   *
   * @param states node states
   * @param incarnations node incarnations
   * @return list of gossip updates
   */
  static List<GossipUpdate> toGossip(Map<String, String> states, Map<String, Long> incarnations) {
    return states.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(
            e ->
                new GossipUpdate(
                    e.getKey(), e.getValue(), incarnations.getOrDefault(e.getKey(), 0L)))
        .toList();
  }
}
