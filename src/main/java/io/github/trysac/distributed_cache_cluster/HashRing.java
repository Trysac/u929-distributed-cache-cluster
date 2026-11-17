package io.github.trysac.distributed_cache_cluster;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Consistent hash ring using 64-bit FNV-1a hashing of {@code nodeId + '#' + index}.
 *
 * <p>Each physical node is placed at {@code virtualNodes} positions on the 64-bit ring.
 */
public final class HashRing {

  private static final long FNV_64_OFFSET_BASIS = 0xcbf29ce484222325L;
  private static final long FNV_64_PRIME = 0x100000001b3L;

  private final int virtualNodes;
  private final int replicas;
  private final NavigableMap<Long, String> ring = new TreeMap<>();
  private final Set<String> nodes = new LinkedHashSet<>();

  /**
   * Creates a new consistent hash ring.
   *
   * @param virtualNodes number of virtual nodes per physical node ({@code >= 1})
   * @param replicas replication factor ({@code >= 1})
   */
  public HashRing(int virtualNodes, int replicas) {
    if (virtualNodes < 1) {
      throw new IllegalArgumentException("virtualNodes must be >= 1");
    }
    if (replicas < 1) {
      throw new IllegalArgumentException("replicas must be >= 1");
    }
    this.virtualNodes = virtualNodes;
    this.replicas = replicas;
  }

  /**
   * Adds a physical node to the hash ring.
   *
   * @param nodeId non-blank identifier of the node
   */
  public void add(String nodeId) {
    Objects.requireNonNull(nodeId, "nodeId");
    if (nodeId.isBlank()) {
      throw new BlankInputException();
    }
    if (!nodes.add(nodeId)) {
      return;
    }
    for (int i = 0; i < virtualNodes; i++) {
      long hash = fnv1a64((nodeId + "#" + i).getBytes(StandardCharsets.UTF_8));
      ring.putIfAbsent(hash, nodeId);
    }
  }

  /**
   * Removes a physical node from the hash ring.
   *
   * @param nodeId identifier of the node to remove
   */
  public void remove(String nodeId) {
    Objects.requireNonNull(nodeId, "nodeId");
    if (!nodes.remove(nodeId)) {
      return;
    }
    ring.entrySet().removeIf(e -> e.getValue().equals(nodeId));
  }

  /**
   * Returns the ordered preference list of distinct physical nodes responsible for {@code key}, up
   * to {@code replicas} nodes.
   *
   * @param key key bytes
   * @return immutable list of distinct node identifiers
   */
  public List<String> preferenceList(byte[] key) {
    Objects.requireNonNull(key, "key");
    if (nodes.isEmpty()) {
      return List.of();
    }
    int targetCount = Math.min(replicas, nodes.size());
    List<String> result = new ArrayList<>(targetCount);
    long keyHash = fnv1a64(key);

    for (Map.Entry<Long, String> entry : ring.tailMap(keyHash, true).entrySet()) {
      String node = entry.getValue();
      if (!result.contains(node)) {
        result.add(node);
        if (result.size() == targetCount) {
          return List.copyOf(result);
        }
      }
    }
    for (Map.Entry<Long, String> entry : ring.entrySet()) {
      String node = entry.getValue();
      if (!result.contains(node)) {
        result.add(node);
        if (result.size() == targetCount) {
          return List.copyOf(result);
        }
      }
    }
    return List.copyOf(result);
  }

  /**
   * Computes the 64-bit FNV-1a hash of the given byte array.
   *
   * @param data input bytes
   * @return 64-bit FNV-1a hash
   */
  static long fnv1a64(byte[] data) {
    long hash = FNV_64_OFFSET_BASIS;
    for (byte b : data) {
      hash ^= (b & 0xffL);
      hash *= FNV_64_PRIME;
    }
    return hash;
  }
}
