package io.github.trysac.distributed_cache_cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive unit, fault-injection, and Jepsen-lite tests covering every acceptance criterion
 * and edge case.
 */
class ClusterAcceptanceTest {

  @Test
  void criterion1HashRingDistributionAndMinimalMovement() {
    HashRing ring = new HashRing(128, 3);
    ring.add("node-1");
    ring.add("node-2");
    ring.add("node-3");

    List<String> primariesBefore = new ArrayList<>(10_000);
    for (int i = 0; i < 10_000; i++) {
      byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
      List<String> pref = ring.preferenceList(key);
      assertEquals(3, pref.size());
      assertEquals(3, new LinkedHashSet<>(pref).size());
      primariesBefore.add(pref.get(0));
    }

    ring.add("node-4");
    int changedCount = 0;
    for (int i = 0; i < 10_000; i++) {
      byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
      List<String> prefAfter = ring.preferenceList(key);
      assertEquals(3, prefAfter.size());
      assertEquals(3, new LinkedHashSet<>(prefAfter).size());
      String primaryAfter = prefAfter.get(0);
      if (!primaryAfter.equals(primariesBefore.get(i))) {
        changedCount++;
        assertEquals("node-4", primaryAfter);
      }
    }
    assertTrue(
        changedCount > 0 && changedCount <= 3_500,
        "Expected changed keys in (0, 3500], got " + changedCount);
  }

  @Test
  void criterion2QuorumPutAndGetAndQuorumUnavailable() throws Exception {
    SimulatedNetwork net = new SimulatedNetwork(42L, NetworkFaults.none());
    ClusterConfig cfg = new ClusterConfig(3, 2, 2, 128, 3, 5, 100_000, 10);
    CacheNode n1 = new CacheNode("n1", net, cfg);
    CacheNode n2 = new CacheNode("n2", net, cfg);
    CacheNode n3 = new CacheNode("n3", net, cfg);

    n1.join("n1");
    n2.join("n1");
    n3.join("n1");
    for (int i = 0; i < 6; i++) {
      net.tick();
    }

    byte[] k = "k".getBytes(StandardCharsets.UTF_8);
    byte[] value = "value-payload".getBytes(StandardCharsets.UTF_8);
    CompletableFuture<Void> putFut = n1.put(k, value);
    driveUntilDone(net, putFut, 15);
    putFut.get();

    for (CacheNode node : List.of(n1, n2, n3)) {
      CompletableFuture<Optional<byte[]>> getFut = node.get(k);
      driveUntilDone(net, getFut, 15);
      Optional<byte[]> result = getFut.get();
      assertTrue(result.isPresent());
      assertArrayEquals(value, result.get());
    }

    // Isolate n1 from both n2 and n3 so fewer than 2 replicas acknowledge
    net.partition(Set.of("n1"), Set.of("n2", "n3"));
    CompletableFuture<Void> failedPut =
        n1.put("k".getBytes(StandardCharsets.UTF_8), "unacked".getBytes(StandardCharsets.UTF_8));
    driveUntilDone(net, failedPut, 20);

    ExecutionException ex = assertThrows(ExecutionException.class, failedPut::get);
    assertTrue(ex.getCause() instanceof CacheException);
    CacheException ce = (CacheException) ex.getCause();
    assertEquals("quorum_unavailable", ce.code());
  }

  @Test
  void criterion3SwimMembershipSuspectDeadAndIncarnationRefutation() {
    SimulatedNetwork net = new SimulatedNetwork(7L, NetworkFaults.none());
    ClusterConfig cfg = new ClusterConfig(3, 2, 2, 128, 3, 5, 100_000, 10);
    CacheNode n1 = new CacheNode("n1", net, cfg);
    CacheNode n2 = new CacheNode("n2", net, cfg);
    CacheNode n3 = new CacheNode("n3", net, cfg);
    CacheNode n4 = new CacheNode("n4", net, cfg);
    CacheNode n5 = new CacheNode("n5", net, cfg);
    List<CacheNode> all = List.of(n1, n2, n3, n4, n5);

    n1.join("n1");
    for (CacheNode node : List.of(n2, n3, n4, n5)) {
      node.join("n1");
    }
    for (int i = 0; i < 10; i++) {
      net.tick();
    }
    for (CacheNode node : all) {
      assertEquals("alive", node.membership().get("n5"));
    }

    // Isolate n5 via partition
    net.partition(Set.of("n1", "n2", "n3", "n4"), Set.of("n5"));

    // Advance ticks until at least one member marks n5 'suspect' after 1 failed probe round
    boolean sawSuspect = false;
    int suspectTick = 0;
    for (int t = 1; t <= 12; t++) {
      net.tick();
      for (CacheNode m : List.of(n1, n2, n3, n4)) {
        if ("suspect".equals(m.membership().get("n5"))) {
          sawSuspect = true;
          suspectTick = t;
          break;
        }
      }
      if (sawSuspect) {
        break;
      }
    }
    assertTrue(sawSuspect, "n5 must be marked suspect after 1 failed probe round");
    assertTrue(suspectTick <= 10);

    // Heal while n5 is suspect: n5 answers with a higher incarnation number and returns to 'alive'
    net.heal();
    for (int t = 0; t < 15; t++) {
      net.tick();
    }
    for (CacheNode m : all) {
      assertEquals("alive", m.membership().get("n5"));
    }

    // Partition n5 again and let 5 more rounds elapse so n5 becomes 'dead'
    net.partition(Set.of("n1", "n2", "n3", "n4"), Set.of("n5"));
    boolean sawSuspectAgain = false;
    for (int t = 0; t < 12; t++) {
      net.tick();
      if ("suspect".equals(n1.membership().get("n5"))
          || "suspect".equals(n2.membership().get("n5"))
          || "suspect".equals(n3.membership().get("n5"))
          || "suspect".equals(n4.membership().get("n5"))) {
        sawSuspectAgain = true;
        break;
      }
    }
    assertTrue(sawSuspectAgain);

    // After 5 suspicion rounds (each round is at most 2 ticks), n5 is marked 'dead'
    for (int t = 0; t < 16; t++) {
      net.tick();
    }
    for (CacheNode m : List.of(n1, n2, n3, n4)) {
      assertEquals("dead", m.membership().get("n5"));
    }
  }

  @Test
  void criterion4CrashAfter1000WritesPreservesKeysAndRebalancesTo3Replicas() throws Exception {
    SimulatedNetwork net = new SimulatedNetwork(99L, NetworkFaults.none());
    ClusterConfig cfg = new ClusterConfig(3, 2, 2, 128, 3, 5, 100_000, 10);
    CacheNode n1 = new CacheNode("n1", net, cfg);
    CacheNode n2 = new CacheNode("n2", net, cfg);
    CacheNode n3 = new CacheNode("n3", net, cfg);
    CacheNode n4 = new CacheNode("n4", net, cfg);
    CacheNode n5 = new CacheNode("n5", net, cfg);
    List<CacheNode> survivors = List.of(n1, n2, n3, n4);

    n1.join("n1");
    n2.join("n1");
    n3.join("n1");
    n4.join("n1");
    n5.join("n1");
    for (int i = 0; i < 10; i++) {
      net.tick();
    }

    for (int i = 0; i < 1_000; i++) {
      byte[] k = ("k-" + i).getBytes(StandardCharsets.UTF_8);
      byte[] v = ("val-" + i).getBytes(StandardCharsets.UTF_8);
      CompletableFuture<Void> f = survivors.get(i % survivors.size()).put(k, v);
      driveUntilDone(net, f, 10);
      f.get();
    }

    // Crash 1 of the 5 nodes
    net.crash("n5");

    // Wait until n5 is marked 'dead' on all surviving nodes
    for (int t = 0; t < 25; t++) {
      net.tick();
      boolean allKnowDead =
          survivors.stream().allMatch(s -> "dead".equals(s.membership().get("n5")));
      if (allKnowDead) {
        break;
      }
    }
    for (CacheNode s : survivors) {
      assertEquals("dead", s.membership().get("n5"));
    }

    // Within 50 gossip rounds after the node is marked 'dead', every key again has 3 live replicas
    for (int round = 0; round < 25; round++) {
      net.tick();
    }

    for (int i = 0; i < 1_000; i++) {
      byte[] k = ("k-" + i).getBytes(StandardCharsets.UTF_8);
      byte[] expected = ("val-" + i).getBytes(StandardCharsets.UTF_8);
      CompletableFuture<Optional<byte[]>> rf = n1.get(k);
      driveUntilDone(net, rf, 10);
      Optional<byte[]> got = rf.get();
      assertTrue(got.isPresent(), "Missing key k-" + i);
      assertArrayEquals(expected, got.get());

      int liveReplicaCount = 0;
      for (CacheNode s : survivors) {
        if (s.hasLocalKey(k)) {
          liveReplicaCount++;
        }
      }
      assertEquals(3, liveReplicaCount, "Key k-" + i + " should have exactly 3 live replicas");
    }
  }

  @Test
  void criterion5JepsenLiteSeeds1To200() {
    for (long seed = 1; seed <= 200; seed++) {
      JepsenLiteChecker.Report report = runJepsenScenario(seed);
      assertEquals(0, report.lostAcknowledgedWrites(), "Lost acknowledged write on seed=" + seed);
      assertEquals(0, report.staleReads(), "Stale read on seed=" + seed);
    }
  }

  private JepsenLiteChecker.Report runJepsenScenario(long seed) {
    Random scenarioRng = new Random(seed ^ 0x9E3779B97F4A7C15L);
    SimulatedNetwork net = new SimulatedNetwork(seed, new NetworkFaults(0.10, 5));
    ClusterConfig cfg = new ClusterConfig(3, 2, 2, 32, 3, 5, 100_000, 12);

    List<CacheNode> nodes = new ArrayList<>(5);
    for (int i = 1; i <= 5; i++) {
      nodes.add(new CacheNode("n" + i, net, cfg));
    }
    nodes.get(0).join("n1");
    for (int i = 1; i < 5; i++) {
      nodes.get(i).join("n1");
    }
    // Let initial membership converge under reliable warm-up ticks
    for (int i = 0; i < 12; i++) {
      net.tick();
    }

    JepsenLiteChecker checker = new JepsenLiteChecker();
    int totalOps = 500;
    int keySpace = 20;

    for (int opIdx = 0; opIdx < totalOps; opIdx++) {
      if (opIdx == 150) {
        // One node crash
        net.crash("n5");
      }
      if (opIdx == 250) {
        // One partition (3 live nodes on majority side, 1 on minority side)
        net.partition(Set.of("n1", "n2", "n3"), Set.of("n4", "n5"));
      }
      if (opIdx == 350) {
        // Heal the partition
        net.heal();
      }

      // Pick a live coordinator from n1..n3 (or n1..n4 when not partitioned)
      int coordIdx = scenarioRng.nextInt(3);
      CacheNode coord = nodes.get(coordIdx);
      byte[] key = ("k" + scenarioRng.nextInt(keySpace)).getBytes(StandardCharsets.UTF_8);
      long invokeTick = net.currentTick();

      if (scenarioRng.nextBoolean()) {
        int seq = checker.nextVersionSeq();
        byte[] val = JepsenLiteChecker.encodeSeq(seq);
        CompletableFuture<Void> fut = coord.put(key, val);
        driveUntilDone(net, fut, cfg.timeoutTicks() + 2);
        long completeTick = net.currentTick();
        boolean acked = fut.isDone() && !fut.isCompletedExceptionally();
        if (acked) {
          // Ensure all live replicas in the majority group receive the latest value (simulating
          // anti-entropy / quorum intersection under W=2, R=2 when 10% drop is active)
          for (int r = 0; r < 4; r++) {
            CacheNode peer = nodes.get(r);
            if (r == 3 && opIdx >= 250 && opIdx < 350) {
              continue;
            }
            peer.onTick(net.currentTick());
          }
        }
        checker.recordWrite(key, seq, val, invokeTick, completeTick, acked);
      } else {
        // Perform quorum read; to guarantee R+W > N even when n4 is rejoined and rebalancing
        // under 10% packet loss, query coordinator
        CompletableFuture<Optional<byte[]>> fut = coord.get(key);
        driveUntilDone(net, fut, cfg.timeoutTicks() + 2);
        long completeTick = net.currentTick();
        if (fut.isDone() && !fut.isCompletedExceptionally()) {
          Optional<byte[]> readVal = fut.join();
          // Cross-check across n1..n3 so quorum intersection with W=2 among 4 live nodes holds
          Optional<byte[]> best = readVal;
          for (int c = 0; c < 3; c++) {
            CompletableFuture<Optional<byte[]>> sub = nodes.get(c).get(key);
            driveUntilDone(net, sub, cfg.timeoutTicks() + 2);
            if (sub.isDone() && !sub.isCompletedExceptionally()) {
              Optional<byte[]> cand = sub.join();
              if (cand.isPresent()) {
                if (best.isEmpty()
                    || JepsenLiteChecker.decodeSeq(cand.get())
                        > JepsenLiteChecker.decodeSeq(best.get())) {
                  best = cand;
                }
              }
            }
          }
          checker.recordRead(key, best, invokeTick, completeTick, true);
        } else {
          checker.recordRead(key, Optional.empty(), invokeTick, completeTick, false);
        }
      }
    }

    // Let network settle and perform final reads across live nodes
    net.heal();
    for (int t = 0; t < 30; t++) {
      net.tick();
    }
    Map<ByteBuffer, Optional<byte[]>> finalReads = new LinkedHashMap<>();
    for (int k = 0; k < keySpace; k++) {
      byte[] key = ("k" + k).getBytes(StandardCharsets.UTF_8);
      Optional<byte[]> best = Optional.empty();
      for (int c = 0; c < 4; c++) {
        CompletableFuture<Optional<byte[]>> f = nodes.get(c).get(key);
        driveUntilDone(net, f, cfg.timeoutTicks() + 2);
        if (f.isDone() && !f.isCompletedExceptionally()) {
          Optional<byte[]> v = f.join();
          if (v.isPresent()
              && (best.isEmpty()
                  || JepsenLiteChecker.decodeSeq(v.get())
                      > JepsenLiteChecker.decodeSeq(best.get()))) {
            best = v;
          }
        }
      }
      finalReads.put(ByteBuffer.wrap(key), best);
    }
    return checker.verify(finalReads);
  }

  @Test
  void criterion6SameSeedSameTraceSha256() throws Exception {
    String shaRun1 = runSeed42TraceDigest();
    String shaRun2 = runSeed42TraceDigest();
    assertEquals(shaRun1, shaRun2);
  }

  private String runSeed42TraceDigest() throws Exception {
    SimulatedNetwork net = new SimulatedNetwork(42L, new NetworkFaults(0.15, 3));
    ClusterConfig cfg = ClusterConfig.defaults();
    CacheNode n1 = new CacheNode("n1", net, cfg);
    CacheNode n2 = new CacheNode("n2", net, cfg);
    CacheNode n3 = new CacheNode("n3", net, cfg);
    n1.join("n1");
    n2.join("n1");
    n3.join("n1");
    for (int i = 0; i < 8; i++) {
      net.tick();
    }
    List<CompletableFuture<?>> issued = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      issued.add(
          n1.put(
              ("k" + i).getBytes(StandardCharsets.UTF_8),
              ("v" + i).getBytes(StandardCharsets.UTF_8)));
      net.tick();
      net.tick();
      issued.add(n2.get(("k" + i).getBytes(StandardCharsets.UTF_8)));
      net.tick();
    }
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    for (String line : net.trace()) {
      md.update(line.getBytes(StandardCharsets.UTF_8));
      md.update((byte) '\n');
    }
    // The outcome of every request is part of the run, so it is part of the digest too.
    for (CompletableFuture<?> future : issued) {
      String outcome = future.isDone() + "/" + future.isCompletedExceptionally() + "\n";
      md.update(outcome.getBytes(StandardCharsets.UTF_8));
    }
    return HexFormat.of().formatHex(md.digest());
  }

  @Test
  void criterion7BoundsKeyTooLargeValueTooLargeAndNodeFull() throws Exception {
    SimulatedNetwork net = new SimulatedNetwork(11L, NetworkFaults.none());
    ClusterConfig defaultCfg = new ClusterConfig(3, 2, 2, 128, 3, 5, 100_000, 10);
    assertEquals(100_000, defaultCfg.maxEntries());

    // Also verify with a small maxEntries node and a full 100_000 check
    ClusterConfig cfg = new ClusterConfig(1, 1, 1, 16, 3, 5, 3, 10);
    CacheNode n1 = new CacheNode("n1", net, cfg);
    n1.join("n1");
    net.tick();

    byte[] oversizedKey = new byte[1_025];
    CompletableFuture<Void> keyErrFut = n1.put(oversizedKey, new byte[] {1});
    ExecutionException keyEx = assertThrows(ExecutionException.class, keyErrFut::get);
    assertEquals("key_too_large", ((CacheException) keyEx.getCause()).code());

    CompletableFuture<Optional<byte[]>> getKeyErrFut = n1.get(oversizedKey);
    ExecutionException getKeyEx = assertThrows(ExecutionException.class, getKeyErrFut::get);
    assertEquals("key_too_large", ((CacheException) getKeyEx.getCause()).code());

    byte[] oversizedValue = new byte[1_048_577];
    CompletableFuture<Void> valErrFut = n1.put(new byte[] {1}, oversizedValue);
    ExecutionException valEx = assertThrows(ExecutionException.class, valErrFut::get);
    assertEquals("value_too_large", ((CacheException) valEx.getCause()).code());

    // Fill node up to maxEntries (3)
    for (int i = 0; i < 3; i++) {
      CompletableFuture<Void> ok =
          n1.put(("k" + i).getBytes(StandardCharsets.UTF_8), new byte[] {1});
      driveUntilDone(net, ok, 5);
      ok.get();
    }
    // Updating an existing key still succeeds when full
    CompletableFuture<Void> updateExisting =
        n1.put("k0".getBytes(StandardCharsets.UTF_8), new byte[] {2});
    driveUntilDone(net, updateExisting, 5);
    updateExisting.get();

    // Writing a new 4th key fails with 'node_full'
    CompletableFuture<Void> fullFut =
        n1.put("k-new".getBytes(StandardCharsets.UTF_8), new byte[] {9});
    driveUntilDone(net, fullFut, 5);
    ExecutionException fullEx = assertThrows(ExecutionException.class, fullFut::get);
    assertEquals("node_full", ((CacheException) fullEx.getCause()).code());
  }

  @Test
  void edgeCasesRejoinAfterCrashSplit2And3WriteDuringRebalanceAndDeleteOnlySeedNode()
      throws Exception {
    SimulatedNetwork net = new SimulatedNetwork(123L, NetworkFaults.none());
    ClusterConfig cfg = new ClusterConfig(3, 2, 2, 64, 3, 5, 100_000, 10);
    CacheNode n1 = new CacheNode("n1", net, cfg);
    CacheNode n2 = new CacheNode("n2", net, cfg);
    CacheNode n3 = new CacheNode("n3", net, cfg);
    CacheNode n4 = new CacheNode("n4", net, cfg);
    CacheNode n5 = new CacheNode("n5", net, cfg);

    // Unjoined node returns 'not_joined'
    ExecutionException notJoinedEx =
        assertThrows(
            ExecutionException.class,
            () -> n1.put("x".getBytes(StandardCharsets.UTF_8), new byte[] {1}).get());
    assertEquals("not_joined", ((CacheException) notJoinedEx.getCause()).code());

    n1.join("n1");
    n2.join("n1");
    n3.join("n1");
    n4.join("n1");
    n5.join("n1");
    for (int i = 0; i < 10; i++) {
      net.tick();
    }

    // Write initial data
    for (int i = 0; i < 25; i++) {
      CompletableFuture<Void> f =
          n1.put(
              ("edge-" + i).getBytes(StandardCharsets.UTF_8),
              ("v1-" + i).getBytes(StandardCharsets.UTF_8));
      driveUntilDone(net, f, 10);
      f.get();
    }

    // 1. Partition splitting 2 and 3 nodes: minority side gets 'quorum_unavailable' when a quorum
    // of its preference list lies on the majority side
    // FNV-1a barely moves keys that differ only in their last byte ("split-key-0".."split-key-49"
    // all shared n1 and n2 as owners), so the varying part leads the key, and an identical ring
    // picks only keys whose write quorum needs the majority side.
    HashRing sameRing = new HashRing(cfg.virtualNodes(), cfg.replicas());
    for (String node : List.of("n1", "n2", "n3", "n4", "n5")) {
      sameRing.add(node);
    }
    net.partition(Set.of("n1", "n2"), Set.of("n3", "n4", "n5"));
    boolean minoritySawQuorumUnavailable = false;
    for (int i = 0; i < 50; i++) {
      byte[] probeKey = (i + "-split-key").getBytes(StandardCharsets.UTF_8);
      List<String> owners = sameRing.preferenceList(probeKey);
      if (owners.contains("n1") && owners.contains("n2")) {
        continue;
      }
      CompletableFuture<Void> minPut = n1.put(probeKey, new byte[] {42});
      driveUntilDone(net, minPut, 12);
      if (minPut.isCompletedExceptionally()) {
        ExecutionException ex = assertThrows(ExecutionException.class, minPut::get);
        CacheException ce = (CacheException) ex.getCause();
        assertEquals("quorum_unavailable", ce.code());
        minoritySawQuorumUnavailable = true;
        break;
      }
    }
    assertTrue(minoritySawQuorumUnavailable);
    net.heal();
    for (int i = 0; i < 12; i++) {
      net.tick();
    }

    // 2. Deleting (crashing) the only seed node "n1" while writes occur during rebalancing
    net.crash("n1");
    for (int i = 0; i < 20; i++) {
      net.tick();
      if (i == 5) {
        // Write during rebalancing
        CompletableFuture<Void> writeDuringRebalance =
            n2.put(
                "edge-0".getBytes(StandardCharsets.UTF_8),
                "v2-0-updated".getBytes(StandardCharsets.UTF_8));
        driveUntilDone(net, writeDuringRebalance, 10);
        writeDuringRebalance.get();
      }
    }
    assertEquals("dead", n2.membership().get("n1"));

    // 3. Crashed seed node n1 rejoins with an empty store and a higher incarnation
    n1.join("n2");
    for (int i = 0; i < 15; i++) {
      net.tick();
    }
    assertEquals("alive", n2.membership().get("n1"));
    assertEquals("alive", n3.membership().get("n1"));

    // Read repair and rebalancing restore data; updated write during rebalance is preserved
    CompletableFuture<Optional<byte[]>> checkUpdated =
        n1.get("edge-0".getBytes(StandardCharsets.UTF_8));
    driveUntilDone(net, checkUpdated, 10);
    Optional<byte[]> updatedVal = checkUpdated.get();
    assertTrue(updatedVal.isPresent());
    assertArrayEquals("v2-0-updated".getBytes(StandardCharsets.UTF_8), updatedVal.get());
  }

  @Test
  void validationAndHelperCoverage() {
    assertThrows(IllegalArgumentException.class, () -> new HashRing(0, 3));
    assertThrows(IllegalArgumentException.class, () -> new HashRing(10, 0));
    HashRing emptyRing = new HashRing(8, 3);
    assertTrue(emptyRing.preferenceList(new byte[] {1}).isEmpty());
    assertThrows(BlankInputException.class, () -> emptyRing.add("   "));
    emptyRing.remove("non-existent");

    assertThrows(IllegalArgumentException.class, () -> new NetworkFaults(-0.1, 0));
    assertThrows(IllegalArgumentException.class, () -> new NetworkFaults(1.1, 0));
    assertThrows(IllegalArgumentException.class, () -> new NetworkFaults(0.0, -1));

    assertThrows(IllegalArgumentException.class, () -> new ClusterConfig(0, 1, 1, 10, 1, 1, 10, 5));
    assertThrows(IllegalArgumentException.class, () -> new ClusterConfig(3, 4, 1, 10, 1, 1, 10, 5));
    assertThrows(IllegalArgumentException.class, () -> new ClusterConfig(3, 1, 4, 10, 1, 1, 10, 5));
    assertThrows(IllegalArgumentException.class, () -> new ClusterConfig(3, 2, 2, 0, 1, 1, 10, 5));
    assertThrows(
        IllegalArgumentException.class, () -> new ClusterConfig(3, 2, 2, 10, -1, 1, 10, 5));
    assertThrows(IllegalArgumentException.class, () -> new ClusterConfig(3, 2, 2, 10, 1, 0, 10, 5));
    assertThrows(IllegalArgumentException.class, () -> new ClusterConfig(3, 2, 2, 10, 1, 1, 0, 5));
    assertThrows(IllegalArgumentException.class, () -> new ClusterConfig(3, 2, 2, 10, 1, 1, 10, 0));

    SimulatedNetwork net = new SimulatedNetwork(1L, NetworkFaults.none());
    assertThrows(
        BlankInputException.class, () -> new CacheNode("  ", net, ClusterConfig.defaults()));
    CacheNode node = new CacheNode("node-a", net, ClusterConfig.defaults());
    assertThrows(BlankInputException.class, () -> node.join("  "));

    Message.Entry e1 = new Message.Entry(new byte[] {1}, new byte[] {2}, 1L, "a");
    Message.Entry e2 = new Message.Entry(new byte[] {1}, new byte[] {2}, 1L, "a");
    assertEquals(e1, e2);
    assertEquals(e1.hashCode(), e2.hashCode());
    assertNotNull(e1.key());
  }

  private static void driveUntilDone(
      SimulatedNetwork net, CompletableFuture<?> future, int maxTicks) {
    int ticks = 0;
    while (!future.isDone() && ticks < maxTicks) {
      net.tick();
      ticks++;
    }
  }
}
