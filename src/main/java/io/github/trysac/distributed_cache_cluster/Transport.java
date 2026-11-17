package io.github.trysac.distributed_cache_cluster;

import java.util.function.Consumer;

/** Message-passing transport abstraction used by {@link CacheNode} instances. */
public interface Transport {

  /**
   * Sends a message from {@code from} to {@code to}.
   *
   * @param from sender node ID
   * @param to recipient node ID
   * @param message protocol message
   */
  void send(String from, String to, Message message);

  /**
   * Registers or replaces the message inbox callback for {@code nodeId}.
   *
   * @param nodeId node identifier
   * @param inbox consumer invoked when a message is delivered to {@code nodeId}
   */
  void register(String nodeId, Consumer<Message> inbox);
}
