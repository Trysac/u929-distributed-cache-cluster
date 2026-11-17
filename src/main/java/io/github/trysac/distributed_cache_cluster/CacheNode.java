package io.github.trysac.distributed_cache_cluster;

import io.github.trysac.distributed_cache_cluster.Message.Entry;
import io.github.trysac.distributed_cache_cluster.Message.GossipUpdate;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A node in the distributed cache cluster participating in SWIM membership, quorum replication,
 * read repair, and batch rebalancing.
 */
public final class CacheNode {

  /** Maximum key size in bytes (1 KiB). */
  public static final int MAX_KEY_BYTES = 1_024;

  /** Maximum value size in bytes (1 MiB). */
  public static final int MAX_VALUE_BYTES = 1_048_576;

  /** Number of keys rebalanced per tick. */
  private static final int REBALANCE_BATCH_SIZE = 100;

  /** Membership state constant for a live node. */
  public static final String STATE_ALIVE = "alive";

  /** Membership state constant for a suspected node. */
  public static final String STATE_SUSPECT = "suspect";

  /** Membership state constant for a dead node. */
  public static final String STATE_DEAD = "dead";

  /**
   * Internal {@code PUT_ACK} error code: the replica already holds a newer version of the key, so
   * it did not apply the write. The reply carries that newer entry so the coordinator can re-issue
   * the write with a higher Lamport timestamp.
   */
  private static final String STALE_VERSION = "stale_version";

  private static final class PendingPut {
    final long startTick;
    final int requiredQuorum;
    final Set<String> targets;
    final Set<String> acks = new LinkedHashSet<>();
    final Set<String> failures = new LinkedHashSet<>();
    String failureCode;
    Entry entry;
    final CompletableFuture<Void> future = new CompletableFuture<>();

    PendingPut(long startTick, int requiredQuorum, Set<String> targets, Entry entry) {
      this.startTick = startTick;
      this.requiredQuorum = requiredQuorum;
      this.targets = targets;
      this.entry = entry;
    }
  }

  private static final class PendingGet {
    final long startTick;
    final int requiredQuorum;
    final Map<String, Optional<Entry>> responses = new LinkedHashMap<>();
    final CompletableFuture<Optional<byte[]>> future = new CompletableFuture<>();

    PendingGet(long startTick, int requiredQuorum) {
      this.startTick = startTick;
      this.requiredQuorum = requiredQuorum;
    }
  }

  private static final class ProbeRound {
    final long probeId;
    final String target;
    final long startTick;
    boolean acked;
    boolean indirectSent;

    ProbeRound(long probeId, String target, long startTick) {
      this.probeId = probeId;
      this.target = target;
      this.startTick = startTick;
    }
  }

  private record IndirectWait(String requester, long originalProbeId, long expireTick) {}

  private record RebalanceTask(String targetNode, ByteBuffer keyBuf, boolean removeAfterPush) {}

  private final String id;
  private final Transport transport;
  private final ClusterConfig config;
  private final HashRing ring;
  private final Random rng;

  private final Map<ByteBuffer, Entry> store = new LinkedHashMap<>();
  private final Map<String, String> memberState = new LinkedHashMap<>();
  private final Map<String, Long> memberIncarnation = new LinkedHashMap<>();
  private final Map<String, Integer> suspectRounds = new LinkedHashMap<>();
  private final Map<Long, PendingPut> pendingPuts = new LinkedHashMap<>();
  private final Map<Long, PendingGet> pendingGets = new LinkedHashMap<>();
  private final Map<Long, IndirectWait> indirectWaits = new LinkedHashMap<>();
  private final Deque<RebalanceTask> rebalanceQueue = new ArrayDeque<>();

  private ProbeRound activeProbe;
  private Set<String> lastRingMembers = new LinkedHashSet<>();
  private boolean joined = false;
  private long lamport = 0;
  private long nextRequestId = 1;
  private long localTick = 0;
  private int rrCursor = 0;

  /**
   * Creates a new cache node and registers its inbox with {@code transport}.
   *
   * @param id non-blank node identifier
   * @param transport network transport
   * @param config cluster configuration
   */
  public CacheNode(String id, Transport transport, ClusterConfig config) {
    Objects.requireNonNull(id, "id");
    if (id.isBlank()) {
      throw new BlankInputException();
    }
    this.id = id;
    this.transport = Objects.requireNonNull(transport, "transport");
    this.config = Objects.requireNonNull(config, "config");
    this.ring = new HashRing(config.virtualNodes(), config.replicas());
    long seed =
        (transport instanceof SimulatedNetwork sim)
            ? sim.deriveNodeSeed(id)
            : HashRing.fnv1a64(id.getBytes(StandardCharsets.UTF_8));
    this.rng = new Random(seed);
    this.transport.register(id, this::onMessage);
    if (transport instanceof SimulatedNetwork sim) {
      sim.attachNode(this);
    }
  }

  /**
   * Returns this node's identifier.
   *
   * @return node ID
   */
  public String id() {
    return id;
  }

  /**
   * Joins the cluster through {@code seedNodeId}.
   *
   * <p>If {@code seedNodeId} equals this node's ID, this node initializes a new cluster with itself
   * as the initial member. If this node is rejoining after a crash, its incarnation number is
   * incremented so other members accept it as {@code alive}.
   *
   * @param seedNodeId identifier of the seed node
   */
  public void join(String seedNodeId) {
    Objects.requireNonNull(seedNodeId, "seedNodeId");
    if (seedNodeId.isBlank()) {
      throw new BlankInputException();
    }
    if (transport instanceof SimulatedNetwork sim) {
      // A node rejoining after a simulated crash is reachable again.
      sim.attachNode(this);
    }
    long nextInc = memberIncarnation.getOrDefault(id, -1L) + 1L;
    if (nextInc < 1L && !joined) {
      nextInc = 0L;
    }
    memberState.put(id, STATE_ALIVE);
    memberIncarnation.put(id, nextInc);
    suspectRounds.remove(id);
    ring.add(id);
    joined = true;
    rebuildRingIfNeeded();

    if (!seedNodeId.equals(id)) {
      memberState.putIfAbsent(seedNodeId, STATE_ALIVE);
      memberIncarnation.putIfAbsent(seedNodeId, 0L);
      ring.add(seedNodeId);
      rebuildRingIfNeeded();
      Message joinMsg =
          new Message(
              Message.Type.JOIN,
              id,
              nextRequestId++,
              null,
              List.of(),
              Message.toGossip(memberState, memberIncarnation),
              true,
              null);
      send(seedNodeId, joinMsg);
    }
  }

  /**
   * Writes a key/value pair to the cluster using configured write quorum {@code W} and replication
   * factor {@code N}.
   *
   * @param key key bytes (at most 1,024 bytes)
   * @param value value bytes (at most 1,048,576 bytes)
   * @return future that completes when {@code W} replicas acknowledge, or completes exceptionally
   *     with {@link CacheException}
   */
  public CompletableFuture<Void> put(byte[] key, byte[] value) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    if (!joined) {
      return CompletableFuture.failedFuture(
          new CacheException(CacheException.NOT_JOINED, "Node " + id + " has not joined cluster"));
    }
    if (key.length > MAX_KEY_BYTES) {
      return CompletableFuture.failedFuture(
          new CacheException(
              CacheException.KEY_TOO_LARGE, "Key size " + key.length + " exceeds 1024 bytes"));
    }
    if (value.length > MAX_VALUE_BYTES) {
      return CompletableFuture.failedFuture(
          new CacheException(
              CacheException.VALUE_TOO_LARGE,
              "Value size " + value.length + " exceeds 1048576 bytes"));
    }

    List<String> targets = livePreferenceList(key);
    if (targets.size() < config.writeQuorum()) {
      return CompletableFuture.failedFuture(
          new CacheException(
              CacheException.QUORUM_UNAVAILABLE,
              "Fewer than " + config.writeQuorum() + " live replicas available"));
    }

    lamport++;
    long reqId = nextRequestId++;
    Entry entry = new Entry(key.clone(), value.clone(), lamport, id);
    PendingPut pending =
        new PendingPut(localTick, config.writeQuorum(), new LinkedHashSet<>(targets), entry);
    pendingPuts.put(reqId, pending);
    sendPutRequests(reqId, pending);
    return pending.future;
  }

  private void sendPutRequests(long reqId, PendingPut pending) {
    // A request delivered to this node itself is handled synchronously and may re-issue the write,
    // so the entry of this round is captured once.
    Entry entry = pending.entry;
    List<GossipUpdate> gossip = Message.toGossip(memberState, memberIncarnation);
    for (String target : pending.targets) {
      Message req =
          new Message(Message.Type.PUT_REQ, id, reqId, null, List.of(entry), gossip, true, null);
      send(target, req);
    }
  }

  /**
   * Reads a key from the cluster using configured read quorum {@code R}, returning the highest
   * version and asynchronously repairing stale replicas.
   *
   * @param key key bytes (at most 1,024 bytes)
   * @return future completing with the latest value (or {@link Optional#empty()} if not found)
   */
  public CompletableFuture<Optional<byte[]>> get(byte[] key) {
    Objects.requireNonNull(key, "key");
    if (!joined) {
      return CompletableFuture.failedFuture(
          new CacheException(CacheException.NOT_JOINED, "Node " + id + " has not joined cluster"));
    }
    if (key.length > MAX_KEY_BYTES) {
      return CompletableFuture.failedFuture(
          new CacheException(
              CacheException.KEY_TOO_LARGE, "Key size " + key.length + " exceeds 1024 bytes"));
    }

    List<String> targets = livePreferenceList(key);
    if (targets.size() < config.readQuorum()) {
      return CompletableFuture.failedFuture(
          new CacheException(
              CacheException.QUORUM_UNAVAILABLE,
              "Fewer than " + config.readQuorum() + " live replicas available"));
    }

    long reqId = nextRequestId++;
    PendingGet pending = new PendingGet(localTick, config.readQuorum());
    pendingGets.put(reqId, pending);

    Entry keyOnly = new Entry(key.clone(), null, 0L, id);
    List<GossipUpdate> gossip = Message.toGossip(memberState, memberIncarnation);
    for (String target : targets) {
      Message req =
          new Message(Message.Type.GET_REQ, id, reqId, null, List.of(keyOnly), gossip, true, null);
      send(target, req);
    }
    return pending.future;
  }

  /**
   * Returns an immutable snapshot of this node's current membership table mapping each known {@code
   * nodeId} to {@code "alive"}, {@code "suspect"}, or {@code "dead"}.
   *
   * @return membership map
   */
  public Map<String, String> membership() {
    return Map.copyOf(memberState);
  }

  /**
   * Returns the number of entries currently stored locally on this node (package-private for
   * testing/inspection).
   *
   * @return local key count
   */
  int localStoreSize() {
    return store.size();
  }

  /**
   * Checks whether this node locally holds {@code key} (package-private for testing).
   *
   * @param key key bytes
   * @return true if stored locally
   */
  boolean hasLocalKey(byte[] key) {
    return store.containsKey(ByteBuffer.wrap(key));
  }

  /**
   * Resets volatile state when this node crashes in the simulator (clears store and pending
   * operations, increments incarnation on next join).
   */
  void onCrash() {
    store.clear();
    for (PendingPut put : pendingPuts.values()) {
      if (!put.future.isDone()) {
        put.future.completeExceptionally(
            new CacheException(CacheException.QUORUM_UNAVAILABLE, "Node crashed"));
      }
    }
    pendingPuts.clear();
    for (PendingGet get : pendingGets.values()) {
      if (!get.future.isDone()) {
        get.future.completeExceptionally(
            new CacheException(CacheException.QUORUM_UNAVAILABLE, "Node crashed"));
      }
    }
    pendingGets.clear();
    indirectWaits.clear();
    rebalanceQueue.clear();
    activeProbe = null;
    long currentInc = memberIncarnation.getOrDefault(id, 0L);
    memberIncarnation.put(id, currentInc + 10L);
  }

  /**
   * Invoked once per virtual time unit by {@link SimulatedNetwork#tick()}.
   *
   * @param tick current virtual tick
   */
  void onTick(long tick) {
    this.localTick = tick;
    if (!joined) {
      return;
    }
    expireTimedOutOperations(tick);
    stepSwimProtocol(tick);
    processRebalanceBatch();
  }

  private void expireTimedOutOperations(long tick) {
    var putIt = pendingPuts.entrySet().iterator();
    while (putIt.hasNext()) {
      PendingPut p = putIt.next().getValue();
      if (tick - p.startTick >= config.timeoutTicks()) {
        putIt.remove();
        if (!p.future.isDone()) {
          String code = p.failureCode != null ? p.failureCode : CacheException.QUORUM_UNAVAILABLE;
          p.future.completeExceptionally(
              new CacheException(code, "Write quorum unavailable within timeout"));
        }
      }
    }

    var getIt = pendingGets.entrySet().iterator();
    while (getIt.hasNext()) {
      PendingGet g = getIt.next().getValue();
      if (tick - g.startTick >= config.timeoutTicks()) {
        getIt.remove();
        if (!g.future.isDone()) {
          g.future.completeExceptionally(
              new CacheException(
                  CacheException.QUORUM_UNAVAILABLE, "Read quorum unavailable within timeout"));
        }
      }
    }

    indirectWaits.entrySet().removeIf(e -> tick >= e.getValue().expireTick());
  }

  private void stepSwimProtocol(long tick) {
    if (activeProbe != null) {
      long elapsed = tick - activeProbe.startTick;
      if (activeProbe.acked) {
        activeProbe = null;
      } else if (elapsed == 1 && !activeProbe.indirectSent) {
        activeProbe.indirectSent = true;
        List<String> intermediaries = selectIntermediaries(activeProbe.target);
        if (!intermediaries.isEmpty()) {
          List<GossipUpdate> gossip = Message.toGossip(memberState, memberIncarnation);
          for (String peer : intermediaries) {
            Message pingReq =
                new Message(
                    Message.Type.PING_REQ,
                    id,
                    activeProbe.probeId,
                    activeProbe.target,
                    List.of(),
                    gossip,
                    true,
                    null);
            send(peer, pingReq);
          }
        }
      } else if (elapsed >= 2) {
        String target = activeProbe.target;
        activeProbe = null;
        if (STATE_ALIVE.equals(memberState.get(target))) {
          memberState.put(target, STATE_SUSPECT);
          suspectRounds.put(target, 0);
        }
        advanceSuspectRounds();
        startNewProbeRound(tick);
        return;
      }
    }

    if (activeProbe == null) {
      advanceSuspectRounds();
      startNewProbeRound(tick);
    }
  }

  private void advanceSuspectRounds() {
    boolean membershipChanged = false;
    List<String> suspects = new ArrayList<>();
    for (Map.Entry<String, String> e : memberState.entrySet()) {
      if (!e.getKey().equals(id) && STATE_SUSPECT.equals(e.getValue())) {
        suspects.add(e.getKey());
      }
    }
    for (String node : suspects) {
      int rounds = suspectRounds.getOrDefault(node, 0) + 1;
      if (rounds > config.suspicionRounds()) {
        memberState.put(node, STATE_DEAD);
        suspectRounds.remove(node);
        membershipChanged = true;
      } else {
        suspectRounds.put(node, rounds);
      }
    }
    if (membershipChanged) {
      rebuildRingIfNeeded();
    }
  }

  private void startNewProbeRound(long tick) {
    List<String> candidates = new ArrayList<>();
    for (Map.Entry<String, String> e : memberState.entrySet()) {
      String peer = e.getKey();
      String st = e.getValue();
      if (!peer.equals(id) && (STATE_ALIVE.equals(st) || STATE_SUSPECT.equals(st))) {
        candidates.add(peer);
      }
    }
    if (candidates.isEmpty()) {
      return;
    }
    Collections.sort(candidates);
    String target = candidates.get(rrCursor % candidates.size());
    rrCursor = (rrCursor + 1) % Math.max(1, candidates.size());

    long probeId = nextRequestId++;
    activeProbe = new ProbeRound(probeId, target, tick);
    Message ping =
        new Message(
            Message.Type.PING,
            id,
            probeId,
            target,
            List.of(),
            Message.toGossip(memberState, memberIncarnation),
            true,
            null);
    send(target, ping);
  }

  private List<String> selectIntermediaries(String excludedTarget) {
    List<String> peers = new ArrayList<>();
    for (Map.Entry<String, String> e : memberState.entrySet()) {
      String node = e.getKey();
      if (!node.equals(id) && !node.equals(excludedTarget) && !STATE_DEAD.equals(e.getValue())) {
        peers.add(node);
      }
    }
    Collections.sort(peers);
    Collections.shuffle(peers, rng);
    int count = Math.min(config.indirectProbes(), peers.size());
    return peers.subList(0, count);
  }

  /** Sends a message stamped with this node's Lamport clock, so every receipt advances clocks. */
  private void send(String to, Message msg) {
    transport.send(id, to, msg.withClock(lamport));
  }

  private void onMessage(Message msg) {
    if (!joined) {
      return;
    }
    lamport = Math.max(lamport, msg.clock());
    mergeGossip(msg.gossip());

    switch (msg.type()) {
      case JOIN -> handleJoin(msg);
      case JOIN_ACK -> handleJoinAck();
      case PING -> handlePing(msg);
      case PING_REQ -> handlePingReq(msg);
      case ACK -> handleAck(msg);
      case PUT_REQ -> handlePutReq(msg);
      case PUT_ACK -> handlePutAck(msg);
      case GET_REQ -> handleGetReq(msg);
      case GET_RESP -> handleGetResp(msg);
      case REPAIR -> handleRepair(msg);
    }
  }

  private void handleJoin(Message msg) {
    String joiner = msg.from();
    if (!STATE_ALIVE.equals(memberState.get(joiner))) {
      memberState.put(joiner, STATE_ALIVE);
      memberIncarnation.putIfAbsent(joiner, 0L);
      suspectRounds.remove(joiner);
      rebuildRingIfNeeded();
    }
    Message ack =
        new Message(
            Message.Type.JOIN_ACK,
            id,
            msg.requestId(),
            null,
            List.of(),
            Message.toGossip(memberState, memberIncarnation),
            true,
            null);
    send(joiner, ack);
  }

  private void handleJoinAck() {
    rebuildRingIfNeeded();
  }

  private void handlePing(Message msg) {
    Message ack =
        new Message(
            Message.Type.ACK,
            id,
            msg.requestId(),
            msg.targetNodeId(),
            List.of(),
            Message.toGossip(memberState, memberIncarnation),
            true,
            null);
    send(msg.from(), ack);
  }

  private void handlePingReq(Message msg) {
    String target = msg.targetNodeId();
    if (target == null) {
      return;
    }
    long subProbeId = nextRequestId++;
    indirectWaits.put(subProbeId, new IndirectWait(msg.from(), msg.requestId(), localTick + 4));
    Message forwardPing =
        new Message(
            Message.Type.PING,
            id,
            subProbeId,
            target,
            List.of(),
            Message.toGossip(memberState, memberIncarnation),
            true,
            null);
    send(target, forwardPing);
  }

  private void handleAck(Message msg) {
    if (activeProbe != null && activeProbe.probeId == msg.requestId()) {
      activeProbe.acked = true;
      activeProbe = null;
    }
    IndirectWait wait = indirectWaits.remove(msg.requestId());
    if (wait != null) {
      Message relayAck =
          new Message(
              Message.Type.ACK,
              /* from= */ id,
              wait.originalProbeId(),
              /* targetNodeId= */ msg.from(),
              List.of(),
              Message.toGossip(memberState, memberIncarnation),
              true,
              null);
      send(wait.requester(), relayAck);
    }
  }

  private void handlePutReq(Message msg) {
    List<GossipUpdate> gossip = Message.toGossip(memberState, memberIncarnation);
    if (msg.entries().isEmpty()) {
      return;
    }
    Entry incoming = msg.entries().get(0);
    lamport = Math.max(lamport, incoming.lamport()) + 1;
    ByteBuffer keyBuf = ByteBuffer.wrap(incoming.key());
    Entry existing = store.get(keyBuf);
    if (existing == null && store.size() >= config.maxEntries()) {
      Message nack =
          new Message(
              Message.Type.PUT_ACK,
              id,
              msg.requestId(),
              null,
              List.of(),
              gossip,
              false,
              CacheException.NODE_FULL);
      send(msg.from(), nack);
      return;
    }
    if (existing != null && incoming.compareVersion(existing) < 0) {
      // A newer version is already stored: acknowledging would report a write that is not visible.
      Message stale =
          new Message(
              Message.Type.PUT_ACK,
              id,
              msg.requestId(),
              null,
              List.of(existing),
              gossip,
              false,
              STALE_VERSION);
      send(msg.from(), stale);
      return;
    }
    store.put(keyBuf, incoming);
    Message ack =
        new Message(Message.Type.PUT_ACK, id, msg.requestId(), null, List.of(), gossip, true, null);
    send(msg.from(), ack);
  }

  private void handlePutAck(Message msg) {
    PendingPut pending = pendingPuts.get(msg.requestId());
    if (pending == null) {
      return;
    }
    if (!msg.ok() && STALE_VERSION.equals(msg.errorCode()) && !msg.entries().isEmpty()) {
      supersedeStaleReplica(msg.requestId(), pending, msg.entries().get(0));
      return;
    }
    if (pending.future.isDone()) {
      return;
    }
    if (msg.ok()) {
      pending.acks.add(msg.from());
      if (pending.acks.size() >= pending.requiredQuorum) {
        pending.future.complete(null);
      }
    } else {
      pending.failures.add(msg.from());
      if (msg.errorCode() != null) {
        pending.failureCode = msg.errorCode();
      }
      int remainingPossible = pending.targets.size() - pending.failures.size();
      if (remainingPossible < pending.requiredQuorum) {
        pendingPuts.remove(msg.requestId());
        String code =
            pending.failureCode != null ? pending.failureCode : CacheException.QUORUM_UNAVAILABLE;
        pending.future.completeExceptionally(
            new CacheException(code, "Write rejected by replica with code " + code));
      }
    }
  }

  /**
   * A replica holds a newer version than the write in flight, so the write was not applied there.
   * The write was issued after that version reached the replica and must be ordered after it:
   * re-issue the same value with a Lamport timestamp above the replica's, under a new request id so
   * acknowledgements of the superseded version no longer count. Once the write has completed, a
   * newer version comes from a later write and is left alone.
   */
  private void supersedeStaleReplica(long reqId, PendingPut pending, Entry newer) {
    if (pending.future.isDone() || pending.entry.compareVersion(newer) >= 0) {
      return;
    }
    lamport = Math.max(lamport, newer.lamport()) + 1;
    pending.entry = new Entry(pending.entry.key(), pending.entry.value(), lamport, id);
    pendingPuts.remove(reqId);
    pending.acks.clear();
    pending.failures.clear();
    long retryId = nextRequestId++;
    pendingPuts.put(retryId, pending);
    sendPutRequests(retryId, pending);
  }

  private void handleGetReq(Message msg) {
    if (msg.entries().isEmpty()) {
      return;
    }
    byte[] key = msg.entries().get(0).key();
    Entry local = store.get(ByteBuffer.wrap(key));
    List<Entry> payload = local != null ? List.of(local) : List.of();
    Message resp =
        new Message(
            Message.Type.GET_RESP,
            id,
            msg.requestId(),
            null,
            payload,
            Message.toGossip(memberState, memberIncarnation),
            true,
            null);
    send(msg.from(), resp);
  }

  private void handleGetResp(Message msg) {
    PendingGet pending = pendingGets.get(msg.requestId());
    if (pending == null || pending.future.isDone()) {
      return;
    }
    Optional<Entry> entryOpt =
        msg.entries().isEmpty() ? Optional.empty() : Optional.of(msg.entries().get(0));
    pending.responses.put(msg.from(), entryOpt);

    if (pending.responses.size() >= pending.requiredQuorum) {
      pendingGets.remove(msg.requestId());
      Entry winner = null;
      for (Optional<Entry> opt : pending.responses.values()) {
        if (opt.isPresent()) {
          Entry candidate = opt.get();
          if (winner == null || candidate.compareVersion(winner) > 0) {
            winner = candidate;
          }
        }
      }
      if (winner != null) {
        lamport = Math.max(lamport, winner.lamport()) + 1;
        List<GossipUpdate> gossip = Message.toGossip(memberState, memberIncarnation);
        for (Map.Entry<String, Optional<Entry>> respEntry : pending.responses.entrySet()) {
          String replica = respEntry.getKey();
          Optional<Entry> replicaVal = respEntry.getValue();
          if (replicaVal.isEmpty() || winner.compareVersion(replicaVal.get()) > 0) {
            Message repair =
                new Message(
                    Message.Type.REPAIR,
                    id,
                    nextRequestId++,
                    null,
                    List.of(winner),
                    gossip,
                    true,
                    null);
            send(replica, repair);
          }
        }
        pending.future.complete(Optional.of(winner.value()));
      } else {
        pending.future.complete(Optional.empty());
      }
    }
  }

  private void handleRepair(Message msg) {
    for (Entry incoming : msg.entries()) {
      lamport = Math.max(lamport, incoming.lamport()) + 1;
      ByteBuffer keyBuf = ByteBuffer.wrap(incoming.key());
      Entry existing = store.get(keyBuf);
      if (existing == null) {
        if (store.size() < config.maxEntries()) {
          store.put(keyBuf, incoming);
        }
      } else if (incoming.compareVersion(existing) > 0) {
        store.put(keyBuf, incoming);
      }
    }
  }

  private void mergeGossip(List<GossipUpdate> updates) {
    if (updates == null || updates.isEmpty()) {
      return;
    }
    boolean changed = false;
    for (GossipUpdate u : updates) {
      String node = u.nodeId();
      if (node.equals(id)) {
        long myInc = memberIncarnation.getOrDefault(id, 0L);
        if ((STATE_SUSPECT.equals(u.state()) || STATE_DEAD.equals(u.state()))
            && u.incarnation() >= myInc) {
          memberIncarnation.put(id, u.incarnation() + 1L);
          memberState.put(id, STATE_ALIVE);
        }
        continue;
      }
      String curState = memberState.get(node);
      long curInc = memberIncarnation.getOrDefault(node, -1L);
      if (curState == null || u.incarnation() > curInc) {
        memberState.put(node, u.state());
        memberIncarnation.put(node, u.incarnation());
        if (STATE_SUSPECT.equals(u.state())) {
          suspectRounds.putIfAbsent(node, 0);
        } else {
          suspectRounds.remove(node);
        }
        changed = true;
      } else if (u.incarnation() == curInc && stateSeverity(u.state()) > stateSeverity(curState)) {
        memberState.put(node, u.state());
        if (STATE_SUSPECT.equals(u.state())) {
          suspectRounds.putIfAbsent(node, 0);
        } else {
          suspectRounds.remove(node);
        }
        changed = true;
      }
    }
    if (changed) {
      rebuildRingIfNeeded();
    }
  }

  private static int stateSeverity(String state) {
    return switch (state) {
      case STATE_ALIVE -> 0;
      case STATE_SUSPECT -> 1;
      case STATE_DEAD -> 2;
      default -> -1;
    };
  }

  private List<String> livePreferenceList(byte[] key) {
    return ring.preferenceList(key);
  }

  private void rebuildRingIfNeeded() {
    Set<String> activeMembers = new LinkedHashSet<>();
    for (Map.Entry<String, String> e : memberState.entrySet()) {
      if (!STATE_DEAD.equals(e.getValue())) {
        activeMembers.add(e.getKey());
      }
    }
    if (activeMembers.equals(lastRingMembers)) {
      return;
    }
    for (String prev : lastRingMembers) {
      if (!activeMembers.contains(prev)) {
        ring.remove(prev);
      }
    }
    for (String curr : activeMembers) {
      if (!lastRingMembers.contains(curr)) {
        ring.add(curr);
      }
    }
    lastRingMembers = activeMembers;
    scheduleRebalance();
  }

  private void scheduleRebalance() {
    rebalanceQueue.clear();
    for (Map.Entry<ByteBuffer, Entry> e : store.entrySet()) {
      ByteBuffer keyBuf = e.getKey();
      byte[] keyBytes = e.getValue().key();
      List<String> owners = ring.preferenceList(keyBytes);
      boolean stillOwner = owners.contains(id);
      for (String owner : owners) {
        if (!owner.equals(id)) {
          rebalanceQueue.addLast(new RebalanceTask(owner, keyBuf, !stillOwner));
        }
      }
    }
  }

  private void processRebalanceBatch() {
    if (rebalanceQueue.isEmpty()) {
      return;
    }
    Map<String, List<Entry>> batchByTarget = new LinkedHashMap<>();
    Set<ByteBuffer> keysToRemove = new LinkedHashSet<>();
    int processedKeys = 0;
    Set<ByteBuffer> seenInBatch = new LinkedHashSet<>();

    while (!rebalanceQueue.isEmpty() && processedKeys < REBALANCE_BATCH_SIZE) {
      RebalanceTask task = rebalanceQueue.pollFirst();
      Entry entry = store.get(task.keyBuf());
      if (entry == null) {
        continue;
      }
      if (seenInBatch.add(task.keyBuf())) {
        processedKeys++;
      }
      batchByTarget.computeIfAbsent(task.targetNode(), k -> new ArrayList<>()).add(entry);
      if (task.removeAfterPush()) {
        List<String> currentOwners = ring.preferenceList(entry.key());
        if (!currentOwners.contains(id)) {
          keysToRemove.add(task.keyBuf());
        }
      }
    }

    List<GossipUpdate> gossip = Message.toGossip(memberState, memberIncarnation);
    for (Map.Entry<String, List<Entry>> batch : batchByTarget.entrySet()) {
      Message repair =
          new Message(
              Message.Type.REPAIR, id, nextRequestId++, null, batch.getValue(), gossip, true, null);
      send(batch.getKey(), repair);
    }
    for (ByteBuffer k : keysToRemove) {
      store.remove(k);
    }
  }
}
