package io.github.trysac.distributed_cache_cluster;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Deterministic, in-process simulated network with virtual time, seeded message loss and delay,
 * network partitions, and node crashes.
 */
public final class SimulatedNetwork implements Transport {

  private static final int MAX_TRACE_ENTRIES = 100_000;

  private record Envelope(long seq, long deliverAtTick, String from, String to, Message message) {}

  private final Random rng;
  private final NetworkFaults faults;
  private final Map<String, Consumer<Message>> inboxes = new LinkedHashMap<>();
  private final Map<String, CacheNode> nodes = new LinkedHashMap<>();
  private final Set<String> crashedNodes = new LinkedHashSet<>();
  private final List<Envelope> inFlight = new ArrayList<>();
  // Bounded FIFO: dropping the oldest event is O(1) once the trace is full.
  private final ArrayDeque<String> eventTrace = new ArrayDeque<>();
  private Set<String> partitionA = Set.of();
  private Set<String> partitionB = Set.of();
  private boolean partitioned = false;
  private long currentTick = 0;
  private long nextSeq = 0;

  /**
   * Creates a new deterministic simulated network.
   *
   * @param seed random seed for message drops, delays, and node PRNG derivation
   * @param faults fault injection configuration
   */
  public SimulatedNetwork(long seed, NetworkFaults faults) {
    this.rng = new Random(seed);
    this.faults = Objects.requireNonNull(faults, "faults");
    recordTrace(
        "INIT seed="
            + seed
            + " dropRate="
            + faults.dropRate()
            + " maxDelay="
            + faults.maxDelayTicks());
  }

  /**
   * Attaches a {@link CacheNode} so that virtual ticks advance its protocol state.
   *
   * @param node the cache node
   */
  void attachNode(CacheNode node) {
    nodes.put(node.id(), node);
    crashedNodes.remove(node.id());
  }

  /**
   * Derives a deterministic seed for a node joining this network.
   *
   * @param nodeId node identifier
   * @return deterministic seed
   */
  long deriveNodeSeed(String nodeId) {
    return rng.nextLong()
        ^ HashRing.fnv1a64(nodeId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  @Override
  public void register(String nodeId, Consumer<Message> inbox) {
    Objects.requireNonNull(nodeId, "nodeId");
    Objects.requireNonNull(inbox, "inbox");
    inboxes.put(nodeId, inbox);
    crashedNodes.remove(nodeId);
    recordTrace("REGISTER tick=" + currentTick + " node=" + nodeId);
  }

  @Override
  public void send(String from, String to, Message message) {
    Objects.requireNonNull(from, "from");
    Objects.requireNonNull(to, "to");
    Objects.requireNonNull(message, "message");

    if (crashedNodes.contains(from) || crashedNodes.contains(to)) {
      recordTrace(
          "DROP_CRASH tick=" + currentTick + " " + from + "->" + to + " " + message.summary());
      return;
    }
    if (isBlockedByPartition(from, to)) {
      recordTrace(
          "DROP_PARTITION tick=" + currentTick + " " + from + "->" + to + " " + message.summary());
      return;
    }
    if (!from.equals(to) && faults.dropRate() > 0.0 && rng.nextDouble() < faults.dropRate()) {
      recordTrace(
          "DROP_FAULT tick=" + currentTick + " " + from + "->" + to + " " + message.summary());
      return;
    }
    int extraDelay =
        (!from.equals(to) && faults.maxDelayTicks() > 0)
            ? rng.nextInt(faults.maxDelayTicks() + 1)
            : 0;
    long deliverAt = currentTick + (from.equals(to) ? 0 : 1 + extraDelay);
    long seq = ++nextSeq;
    inFlight.add(new Envelope(seq, deliverAt, from, to, message));
    recordTrace(
        "SEND tick="
            + currentTick
            + " deliverAt="
            + deliverAt
            + " "
            + from
            + "->"
            + to
            + " "
            + message.summary());
    if (from.equals(to)) {
      deliverDueMessages();
    }
  }

  /**
   * Advances virtual time by one tick, delivering due messages and running each live node's tick
   * handler.
   */
  public void tick() {
    currentTick++;
    recordTrace("TICK " + currentTick);
    deliverDueMessages();
    List<String> nodeIds = new ArrayList<>(nodes.keySet());
    Collections.sort(nodeIds);
    for (String nodeId : nodeIds) {
      if (!crashedNodes.contains(nodeId)) {
        CacheNode node = nodes.get(nodeId);
        if (node != null) {
          node.onTick(currentTick);
        }
        deliverDueMessages();
      }
    }
  }

  private void deliverDueMessages() {
    boolean deliveredAny = true;
    while (deliveredAny) {
      deliveredAny = false;
      List<Envelope> due = new ArrayList<>();
      for (Envelope env : inFlight) {
        if (env.deliverAtTick <= currentTick) {
          due.add(env);
        }
      }
      if (due.isEmpty()) {
        break;
      }
      due.sort((a, b) -> Long.compare(a.seq, b.seq));
      for (Envelope env : due) {
        inFlight.remove(env);
        if (crashedNodes.contains(env.to) || isBlockedByPartition(env.from, env.to)) {
          recordTrace(
              "DROP_ON_DELIVER tick="
                  + currentTick
                  + " "
                  + env.from
                  + "->"
                  + env.to
                  + " "
                  + env.message.summary());
          continue;
        }
        Consumer<Message> inbox = inboxes.get(env.to);
        if (inbox != null) {
          recordTrace(
              "DELIVER tick="
                  + currentTick
                  + " "
                  + env.from
                  + "->"
                  + env.to
                  + " "
                  + env.message.summary());
          inbox.accept(env.message);
          deliveredAny = true;
        }
      }
    }
  }

  /**
   * Splits the network into two isolated sets of nodes {@code a} and {@code b}.
   *
   * @param a first partition side
   * @param b second partition side
   */
  public void partition(Set<String> a, Set<String> b) {
    Objects.requireNonNull(a, "a");
    Objects.requireNonNull(b, "b");
    this.partitionA = Set.copyOf(a);
    this.partitionB = Set.copyOf(b);
    this.partitioned = true;
    List<String> sortedA = new ArrayList<>(a);
    List<String> sortedB = new ArrayList<>(b);
    Collections.sort(sortedA);
    Collections.sort(sortedB);
    recordTrace("PARTITION tick=" + currentTick + " A=" + sortedA + " B=" + sortedB);
  }

  /** Heals any active network partition so all live nodes can communicate again. */
  public void heal() {
    this.partitioned = false;
    this.partitionA = Set.of();
    this.partitionB = Set.of();
    recordTrace("HEAL tick=" + currentTick);
  }

  /**
   * Simulates an abrupt crash of {@code nodeId}: stops message delivery and tick execution for that
   * node.
   *
   * @param nodeId identifier of the node to crash
   */
  public void crash(String nodeId) {
    Objects.requireNonNull(nodeId, "nodeId");
    crashedNodes.add(nodeId);
    inFlight.removeIf(env -> env.from.equals(nodeId) || env.to.equals(nodeId));
    CacheNode node = nodes.get(nodeId);
    if (node != null) {
      node.onCrash();
    }
    recordTrace("CRASH tick=" + currentTick + " node=" + nodeId);
  }

  /**
   * Returns an immutable snapshot of the network event trace.
   *
   * @return ordered list of trace events
   */
  public List<String> trace() {
    return List.copyOf(eventTrace);
  }

  /**
   * Returns the current virtual time tick.
   *
   * @return current tick
   */
  public long currentTick() {
    return currentTick;
  }

  private boolean isBlockedByPartition(String from, String to) {
    if (!partitioned || from.equals(to)) {
      return false;
    }
    boolean fromInA = partitionA.contains(from);
    boolean fromInB = partitionB.contains(from);
    boolean toInA = partitionA.contains(to);
    boolean toInB = partitionB.contains(to);
    if ((fromInA && toInB) || (fromInB && toInA)) {
      return true;
    }
    if ((fromInA && !toInA) || (toInA && !fromInA)) {
      return true;
    }
    if ((fromInB && !toInB) || (toInB && !fromInB)) {
      return true;
    }
    return false;
  }

  private void recordTrace(String event) {
    if (eventTrace.size() >= MAX_TRACE_ENTRIES) {
      eventTrace.removeFirst();
    }
    eventTrace.add(event);
  }
}
