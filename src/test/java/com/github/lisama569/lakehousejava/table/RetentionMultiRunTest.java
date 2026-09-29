package com.github.lisama569.lakehousejava.table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Multi-round expiration: an old pinned version plus a tiny retainLatest window,
 * first GC, then more writes, then GC again (potentially many rounds).
 */
class RetentionMultiRunTest {

    @TempDir
    Path tmp;

    private TableManager t;

    @BeforeEach
    void setUp() {
        LocalLakehouseEngine engine = new LocalLakehouseEngine(tmp);
        Schema schema = Schemas.builder()
                .column("id", DataType.LONG, false, null)
                .column("v", DataType.STRING, true, null)
                .primaryKey("id")
                .build();
        t = engine.createTable("t", schema);
    }

    private void up(long id, String v) {
        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", id, "v", v)))));
    }

    private void del(long id) {
        t.write(new WriteBatch(List.of(RowOp.delete(Row.of("id", id)))));
    }

    private Map<Long, List<Row>> snapshotRows(List<Long> versions) {
        Map<Long, List<Row>> out = new TreeMap<>();
        for (long v : versions) {
            out.put(v, t.readSnapshot(v));
        }
        return out;
    }

    private Map<Object, String> byId(List<Row> rows) {
        Map<Object, String> out = new TreeMap<>();
        rows.forEach(r -> out.put(r.get("id"), (String) r.get("v")));
        return out;
    }

    @Test
    @DisplayName("pin early version + retainLatest=1: GC, writes, GC again must complete and keep both readable")
    void secondExpireAfterWrites() {
        up(1, "a"); // v1 -- the version to pin
        up(2, "b"); // v2
        up(3, "c"); // v3 current

        t.setRetentionPolicy(RetentionPolicy.of(1, List.of(1L)));
        ExpireResult first = t.expireSnapshots(3L);
        assertTrue(first.committed());
        assertEquals(List.of(1L, 3L), first.retainedVersions());
        assertEquals(List.of(0L, 2L), first.expiredVersions());

        up(4, "d"); // v4
        del(2);     // v5 current: id=2 removed
        Map<Long, List<Row>> before = snapshotRows(List.of(1L, 5L));

        // This used to abort with an internal error and every retry stayed stuck.
        ExpireResult second = assertDoesNotThrow(() -> t.expireSnapshots(5L));
        assertTrue(second.committed());
        assertEquals(List.of(1L, 5L), second.retainedVersions());
        // v3 left the retainLatest=1 window (v4 landed, then v5); v4 is also behind it now
        assertEquals(List.of(3L, 4L), second.expiredVersions());
        assertEquals(List.of(0L, 2L), second.previouslyExpired());

        assertEquals(before.get(1L), t.readSnapshot(1L), "pinned v1 unchanged");
        assertEquals(before.get(5L), t.readSnapshot(5L), "latest v5 unchanged");
        assertEquals(before.get(5L), t.readSnapshot(null));
        assertEquals(Map.of(1L, "a", 3L, "c", 4L, "d"), byId(t.readSnapshot(null)),
                "no lost, duplicated or resurrected rows");
    }

    @Test
    @DisplayName("many write->GC rounds with a pinned old version: every run completes and conclusions stay stable")
    void repeatedWriteThenExpireRounds() {
        up(1, "a"); // v1 pinned
        up(2, "b"); // v2
        up(3, "c"); // v3
        t.setRetentionPolicy(RetentionPolicy.of(2, List.of(1L)));

        // Round 0 (base 3): retained 1,2,3 (window keeps 2,3 plus the pin); expired 0.
        ExpireResult r0 = t.expireSnapshots(3L);
        assertEquals(List.of(1L, 2L, 3L), r0.retainedVersions());
        assertEquals(List.of(0L), r0.expiredVersions());
        List<Row> pinnedForever = t.readSnapshot(1L);
        assertTrue(r0.bytesReclaimed() > 0);

        // Each following round writes exactly one commit and GCs on the new current
        // version, so the previous current (inside the old retainLatest=2 window)
        // always ages out while its own delta file is still physically on disk.
        //
        // Round 1 (base 4): retained 1,3,4; v2 leaves the window.
        up(4, "d");
        assertRound(4L, List.of(1L, 3L, 4L), List.of(2L),
                Map.of(1L, "a", 2L, "b", 3L, "c", 4L, "d"));

        // Round 2 (base 5): retained 1,4,5; v3 ages out (its own delta replay path
        // crosses the GC boundary anchored on v1).
        del(2);
        assertRound(5L, List.of(1L, 4L, 5L), List.of(3L),
                Map.of(1L, "a", 3L, "c", 4L, "d"));

        // Round 3 (base 6): retained 1,5,6; v4 ages out.
        up(5, "e");
        assertRound(6L, List.of(1L, 5L, 6L), List.of(4L),
                Map.of(1L, "a", 3L, "c", 4L, "d", 5L, "e"));

        // Round 4 (base 7): retained 1,6,7; v5 ages out.
        up(3, "c2"); // update an old row
        assertRound(7L, List.of(1L, 6L, 7L), List.of(5L),
                Map.of(1L, "a", 3L, "c2", 4L, "d", 5L, "e"));

        // Round 5 (base 8): retained 1,7,8; v6 ages out.
        del(4);
        assertRound(8L, List.of(1L, 7L, 8L), List.of(6L),
                Map.of(1L, "a", 3L, "c2", 5L, "e"));

        // Final cumulative expired set stays unique and sorted.
        assertEquals(List.of(0L, 2L, 3L, 4L, 5L, 6L), t.expiredVersions());

        // The pinned version reads identically after five more GC generations.
        assertEquals(pinnedForever, t.readSnapshot(1L));

        // Expired versions remain distinguishable from versions that never existed.
        assertEquals(RejectReasonCode.VERSION_EXPIRED,
                assertThrows(TableException.class, () -> t.readSnapshot(3L)).code());
        assertEquals(RejectReasonCode.VERSION_NOT_FOUND,
                assertThrows(TableException.class, () -> t.readSnapshot(999L)).code());

        // Expiring again at the same base is an idempotent no-op cleanup.
        List<Row> latest = t.readSnapshot(null);
        ExpireResult again = t.expireSnapshots(8L);
        assertFalse(again.committed());
        assertEquals(0, again.filesDeleted().size());
        assertEquals(latest, t.readSnapshot(null));

        // Reopening the table and running GC again still converges identically.
        TableManager reopened = new LocalLakehouseEngine(tmp).openTable("t");
        ExpireResult reopenedRun = reopened.expireSnapshots(8L);
        assertFalse(reopenedRun.committed());
        assertEquals(0, reopenedRun.filesDeleted().size());
        assertEquals(pinnedForever, reopened.readSnapshot(1L));
        assertEquals(latest, reopened.readSnapshot(null));
        assertEquals(List.of(1L, 7L, 8L), reopened.readableVersions());
    }

    private void assertRound(long base, List<Long> retainedNow, List<Long> newlyExpired,
                             Map<Object, String> expectedLatestById) {
        List<Long> retainedReadable = new java.util.ArrayList<>(retainedNow);
        Map<Long, List<Row>> before = snapshotRows(retainedReadable);
        ExpireResult r = assertDoesNotThrow(() -> t.expireSnapshots(base));
        assertTrue(r.committed(), "round at base " + base + " must publish");
        assertEquals(retainedNow, r.retainedVersions());
        assertEquals(newlyExpired, r.expiredVersions(), "newly expired at base " + base);
        for (long v : retainedNow) {
            assertEquals(before.get(v), t.readSnapshot(v),
                    "retained v" + v + " must read identically after GC round at base " + base);
        }
        assertEquals(expectedLatestById, byId(t.readSnapshot(null)),
                "latest content after GC round at base " + base);
    }

    @Test
    @DisplayName("interruption during a later GC round never sticks: retry completes with the same conclusion")
    void interruptionOnLaterRoundResumes() {
        up(1, "a"); // v1 pinned
        up(2, "b"); // v2
        up(3, "c"); // v3
        t.setRetentionPolicy(RetentionPolicy.of(1, List.of(1L)));
        t.expireSnapshots(3L); // gen1: retained 1,3

        up(4, "d"); // v4
        del(2);     // v5 current

        Map<Long, List<Row>> before = snapshotRows(List.of(1L, 5L));

        // Crash right after the gen-2 state was published, before any deletion.
        assertThrows(ExpireHook.SimulatedInterruption.class,
                () -> ((LocalTableManager) t).expireSnapshots(5L, ExpireHook.failAfterPublish()));

        // Visible conclusion is already the gen-2 one, deterministically.
        assertEquals(List.of(1L, 5L), t.readableVersions());
        assertEquals(before.get(5L), t.readSnapshot(5L));
        assertEquals(before.get(1L), t.readSnapshot(1L));

        // Retry finishes leftover cleanup without re-publishing.
        ExpireResult retry = t.expireSnapshots(5L);
        assertFalse(retry.committed(), "retry is cleanup-only: state already published");
        assertEquals(List.of(1L, 5L), retry.retainedVersions());

        // A further round must still work across the cleanup boundary.
        up(6, "f"); // v6
        Map<Long, List<Row>> before2 = snapshotRows(List.of(1L, 6L));
        ExpireResult gen3 = assertDoesNotThrow(() -> t.expireSnapshots(6L));
        assertTrue(gen3.committed());
        assertEquals(List.of(1L, 6L), gen3.retainedVersions());
        assertEquals(before2.get(1L), t.readSnapshot(1L));
        assertEquals(before2.get(6L), t.readSnapshot(6L));
        assertEquals(Map.of(1L, "a", 3L, "c", 4L, "d", 6L, "f"), byId(t.readSnapshot(null)));
    }

    @Test
    @DisplayName("crash mid-deletion on a later round: retry yields the unique conclusion, no retained version lost")
    void midDeletionCrashOnLaterRoundResumes() {
        up(1, "a");
        up(2, "b");
        up(3, "c");
        t.setRetentionPolicy(RetentionPolicy.of(1, List.of(1L)));
        t.expireSnapshots(3L); // gen1

        up(4, "d");
        up(5, "e"); // v5 current

        Map<Long, List<Row>> before = snapshotRows(List.of(1L, 5L));

        assertThrows(ExpireHook.SimulatedInterruption.class,
                () -> ((LocalTableManager) t).expireSnapshots(5L, ExpireHook.failAfterDeletes(1)));

        // The run must not have left the table stuck: retrying completes it.
        ExpireResult done = assertDoesNotThrow(() -> t.expireSnapshots(5L));
        assertFalse(done.committed(), "retry only finishes leftover deletions");
        assertEquals(List.of(1L, 5L), t.readableVersions());
        assertEquals(before.get(1L), t.readSnapshot(1L));
        assertEquals(before.get(5L), t.readSnapshot(5L));
        assertEquals(before.get(5L), t.readSnapshot(null));

        // And again: fully idempotent.
        ExpireResult again = t.expireSnapshots(5L);
        assertFalse(again.committed());
        assertEquals(0, again.filesDeleted().size());
        assertEquals(before.get(5L), t.readSnapshot(null));
    }

    @Test
    @DisplayName("concurrent commit during a later GC round aborts the run with RETENTION_CONFLICT and deletes nothing")
    void concurrentCommitAbortsLaterRound() {
        up(1, "a");
        up(2, "b");
        up(3, "c");
        t.setRetentionPolicy(RetentionPolicy.of(1, List.of(1L)));
        t.expireSnapshots(3L); // gen1, base 3

        up(4, "d"); // current -> 4
        long staleBase = t.currentVersion();
        up(5, "e"); // a newer commit lands before GC starts: base 4 is stale

        TableException e = assertThrows(TableException.class,
                () -> t.expireSnapshots(staleBase));
        assertEquals(RejectReasonCode.RETENTION_CONFLICT, e.code());

        // Whole run abandoned: nothing expired by the stale attempt, nothing deleted.
        assertEquals(List.of(0L, 2L), t.expiredVersions());
        for (long v : new long[]{1L, 3L, 4L, 5L}) {
            assertNotNull(t.readSnapshot(v), "version " + v + " must stay readable after the conflict");
        }
        assertEquals(5L, t.currentVersion());

        // Retrying with the current base succeeds normally.
        ExpireResult ok = assertDoesNotThrow(() -> t.expireSnapshots(5L));
        assertTrue(ok.committed());
        assertEquals(List.of(1L, 5L), ok.retainedVersions());
        assertEquals(List.of(3L, 4L), ok.expiredVersions());
    }

    @Test
    @DisplayName("a later GC still shrinks on-disk footprint while keeping the pinned old version readable")
    void laterRoundsStillReclaimDisk() throws Exception {
        String fat = "z".repeat(200);
        for (long id = 1; id <= 10; id++) {
            t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", id, "v", fat + "-init-" + id)))));
        }
        // v10 current; pin v1.
        t.setRetentionPolicy(RetentionPolicy.of(1, List.of(1L)));
        t.expireSnapshots(10L);

        List<Row> pinned = t.readSnapshot(1L);

        for (long id = 1; id <= 10; id++) {
            t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", id, "v", fat + "-round2-" + id)))));
        }
        long beforeGen2 = sizeOf(t.tableDir());
        ExpireResult r2 = t.expireSnapshots(20L);
        long afterGen2 = sizeOf(t.tableDir());

        assertTrue(r2.bytesReclaimed() > 0);
        assertTrue(afterGen2 < beforeGen2, "second run must shrink footprint: "
                + beforeGen2 + " -> " + afterGen2);
        assertEquals(pinned, t.readSnapshot(1L), "pinned v1 still byte-identical");
    }

    private static long sizeOf(Path dir) throws Exception {
        try (var s = java.nio.file.Files.walk(dir)) {
            return s.filter(java.nio.file.Files::isRegularFile).mapToLong(p -> {
                try {
                    return java.nio.file.Files.size(p);
                } catch (Exception e) {
                    return 0L;
                }
            }).sum();
        }
    }
}
