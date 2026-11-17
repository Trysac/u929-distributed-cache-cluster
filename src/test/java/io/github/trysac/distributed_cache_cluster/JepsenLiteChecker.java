package io.github.trysac.distributed_cache_cluster;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Jepsen-lite history recorder and property verifier.
 *
 * <p>Records the invocation tick and completion tick of every {@code put} and {@code get}
 * operation, then checks:
 *
 * <ol>
 *   <li>No acknowledged write is lost at the end of the run when final reads are performed.
 *   <li>No read returns a value older than the last write that was acknowledged strictly before the
 *       read began.
 * </ol>
 */
final class JepsenLiteChecker {

  record WriteOp(
      ByteBuffer key,
      int versionSeq,
      ByteBuffer value,
      long invokeTick,
      long completeTick,
      boolean acknowledged) {}

  record ReadOp(
      ByteBuffer key,
      Optional<byte[]> value,
      long invokeTick,
      long completeTick,
      boolean succeeded) {}

  record Report(int lostAcknowledgedWrites, int staleReads) {}

  private final List<WriteOp> writes = new ArrayList<>();
  private final List<ReadOp> reads = new ArrayList<>();
  private final Map<ByteBuffer, List<WriteOp>> ackedWritesByKey = new LinkedHashMap<>();
  private int nextSeq = 1;

  int nextVersionSeq() {
    return nextSeq++;
  }

  void recordWrite(
      byte[] key, int versionSeq, byte[] value, long invokeTick, long completeTick, boolean acked) {
    ByteBuffer kb = ByteBuffer.wrap(key.clone());
    WriteOp op =
        new WriteOp(
            kb,
            versionSeq,
            ByteBuffer.wrap(value.clone()).asReadOnlyBuffer(),
            invokeTick,
            completeTick,
            acked);
    writes.add(op);
    if (acked) {
      ackedWritesByKey.computeIfAbsent(kb, k -> new ArrayList<>()).add(op);
    }
  }

  void recordRead(
      byte[] key, Optional<byte[]> value, long invokeTick, long completeTick, boolean succeeded) {
    ByteBuffer kb = ByteBuffer.wrap(key.clone());
    Optional<byte[]> cloned = value.map(byte[]::clone);
    reads.add(new ReadOp(kb, cloned, invokeTick, completeTick, succeeded));
  }

  Report verify(Map<ByteBuffer, Optional<byte[]>> finalReads) {
    int staleReads = 0;
    for (ReadOp read : reads) {
      if (!read.succeeded()) {
        continue;
      }
      List<WriteOp> keyWrites = ackedWritesByKey.getOrDefault(read.key(), List.of());
      WriteOp latestBeforeRead = null;
      for (WriteOp w : keyWrites) {
        if (w.completeTick() < read.invokeTick()) {
          if (latestBeforeRead == null || w.versionSeq() > latestBeforeRead.versionSeq()) {
            latestBeforeRead = w;
          }
        }
      }
      if (latestBeforeRead != null) {
        if (read.value().isEmpty()) {
          staleReads++;
        } else {
          int readSeq = decodeSeq(read.value().get());
          if (readSeq < latestBeforeRead.versionSeq()) {
            staleReads++;
          }
        }
      }
    }

    int lostWrites = 0;
    for (Map.Entry<ByteBuffer, List<WriteOp>> entry : ackedWritesByKey.entrySet()) {
      List<WriteOp> keyWrites = entry.getValue();
      if (keyWrites.isEmpty()) {
        continue;
      }
      WriteOp highestAcked = keyWrites.get(0);
      for (WriteOp w : keyWrites) {
        if (w.versionSeq() > highestAcked.versionSeq()) {
          highestAcked = w;
        }
      }
      Optional<byte[]> finalVal = finalReads.getOrDefault(entry.getKey(), Optional.empty());
      if (finalVal.isEmpty()) {
        lostWrites++;
      } else {
        int finalSeq = decodeSeq(finalVal.get());
        if (finalSeq < highestAcked.versionSeq()) {
          lostWrites++;
        }
      }
    }

    return new Report(lostWrites, staleReads);
  }

  static byte[] encodeSeq(int seq) {
    return ("v-" + seq).getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  static int decodeSeq(byte[] value) {
    String s = new String(value, java.nio.charset.StandardCharsets.UTF_8);
    if (s.startsWith("v-")) {
      return Integer.parseInt(s.substring(2));
    }
    return -1;
  }

  static boolean sameBytes(byte[] a, byte[] b) {
    return Arrays.equals(a, b);
  }
}
