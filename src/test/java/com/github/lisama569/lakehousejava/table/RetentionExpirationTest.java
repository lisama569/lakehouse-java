package com.github.lisama569.lakehousejava.table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers snapshot expiration: dependency protection of retained versions,
 * concurrency interleave with writes, interruption/resume safety, repeated
 * cleanup, writes/compaction after GC, and reopen persistence.
 */
class RetentionExpirationTest {

    @TempDir
    Path tmp;

    private LocalLakehouseEngine engine;
    private TableManager t;

    @BeforeEach
    void setUp() {
        engine = new LocalLakehouseEngine(tmp);
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

    private Map<Long, List<Row>> capture(long to) {
        Map<Long, List<Row>> all = new java.util.TreeMap<>();
        for (long v = 0; v <= to; v++) {
            all.put(v, t.readSnapshot(v));
        }
        return all;
    }

    private void assertReadable(Map<Long, List<Row>> expected, List<Long> versions) {
        for (long v : versions) {
            assertEquals(expected.get(v), t.readSnapshot(v),
                    "retained version " + v + " must read identically after GC");
        }
    }

    private void assertExpired(List<Long> versions) {
        for (long v : versions) {
            TableException e = assertThrows(TableException.class, () -> t.readSnapshot(v),
                    "version " + v + " should be expired");
            assertEquals(RejectReasonCode.VERSION_EXPIRED, e.code());
        }
    }

    // ----------------------------------------- 1. dependency protection

    @Test
    @DisplayName("expiration materializes retained versions; their conclusions stay identical and expired ones are gone")
    void dependencyProtection() {
        up(1, "a");   // v1
        up(1, "b");   // v2 (update)
        up(2, "c");   // v3
        del(1);       // v4
        up(3, "d");   // v5
        up(3, "e");   // v6 (current)

        Map<Long, List<Row>> before = capture(6);

        t.setRetentionPolicy(RetentionPolicy.of(2, List.of(2L)));
        ExpireResult r = t.expireSnapshots(6L);

        assertTrue(r.committed());
        assertEquals(List.of(2L, 5L, 6L), r.retainedVersions());
        assertEquals(List.of(0L, 1L, 3L, 4L), r.expiredVersions());
        assertTrue(r.bytesReclaimed() > 0);

        // retained versions stay byte-conclusion identical even though their
        // original delta files are physically gone
        assertReadable(before, List.of(2L, 5L, 6L));
        assertEquals(before.get(6L), t.readSnapshot(null));
        // no lost/duplicated rows on the latest view (id=1 deleted at v4; ids 2 and 3 survive)
        assertEquals(2, t.readSnapshot(null).size());
        Map<Object, String> latestById = new java.util.TreeMap<>();
        t.readSnapshot(null).forEach(row -> latestById.put(row.get("id"), (String) row.get("v")));
        assertEquals(Map.of(2L, "c", 3L, "e"), latestById);

        assertExpired(List.of(0L, 1L, 3L, 4L));

        // expired vs never-existed stay distinguishable
        assertEquals(RejectReasonCode.VERSION_NOT_FOUND,
                assertThrows(TableException.class, () -> t.readSnapshot(77L)).code());
        // schemaAt honours expiration too
        assertEquals(RejectReasonCode.VERSION_EXPIRED,
                assertThrows(TableException.class, () -> t.schemaAt(1L)).code());

        // history still lists every version but marks expired ones
        List<CommitInfo> h = t.history();
        assertEquals(7, h.size());
        assertFalse(h.get(2).expired(), "v2 pinned -> not expired");
        assertFalse(h.get(5).expired(), "v5 retained");
        assertTrue(h.get(1).expired(), "v1 must be marked expired");
        assertTrue(h.get(3).expired(), "v3 must be marked expired");
    }

    // ----------------------------------------- 2. concurrent interleave

    @Test
    @DisplayName("GC based on an outdated base is rejected with RETENTION_CONFLICT and deletes nothing")
    void expireConflictsWithConcurrentWrite() {
        for (long i = 1; i <= 4; i++) {
            up(i, "v" + i);
        }
        t.setRetentionPolicy(RetentionPolicy.retainLatest(1));
        long staleBase = t.currentVersion(); // 4

        // a commit lands after the GC base was observed
        up(99, "late"); // current -> 5

        TableException e = assertThrows(TableException.class, () -> t.expireSnapshots(staleBase));
        assertEquals(RejectReasonCode.RETENTION_CONFLICT, e.code());
        assertEquals(5L, t.currentVersion());
        // nothing expired / deleted: old versions still readable
        assertTrue(t.expiredVersions().isEmpty());
        for (long v = 0; v <= 5; v++) {
            assertNotNull(t.readSnapshot(v));
        }

        // retry with the fresh base succeeds and keeps the still-in-range committed version
        List<Row> v5rows = t.readSnapshot(5L);
        ExpireResult r = t.expireSnapshots(5L);
        assertTrue(r.committed());
        assertEquals(List.of(5L), r.retainedVersions());
        assertEquals(v5rows, t.readSnapshot(5L));
        assertEquals(5, v5rows.size());
    }

    @Test
    @DisplayName("GC running concurrently with a writer never wins against a newer commit (lock-serialized)")
    void concurrentGcAndWrite() throws Exception {
        for (long i = 1; i <= 3; i++) {
            up(i, "v" + i);
        }
        t.setRetentionPolicy(RetentionPolicy.retainLatest(1));
        final long base = t.currentVersion();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<TableException> gcFut = pool.submit(() -> {
                start.await();
                try {
                    t.expireSnapshots(base);
                    return null;
                } catch (TableException ex) {
                    return ex;
                }
            });
            Future<?> writeFut = pool.submit(() -> {
                start.await();
                up(42L, "concurrent");
                return null;
            });
            start.countDown();
            TableException gcErr = gcFut.get(30, TimeUnit.SECONDS);
            writeFut.get(30, TimeUnit.SECONDS);

            if (gcErr != null) {
                // writer won first: GC must conflict and delete nothing
                assertEquals(RejectReasonCode.RETENTION_CONFLICT, gcErr.code());
            }
            // regardless of order, the successfully committed current version is readable
            long current = t.currentVersion();
            assertNotNull(t.readSnapshot(current));
            // and no retained version became unreadable
            assertNotNull(t.readSnapshot(base));
        } finally {
            pool.shutdownNow();
        }
    }

    // ----------------------------------------- 3. interruption / resume

    @Test
    @DisplayName("crash after state publish but before deletion: retry converges, no loss")
    void resumeAfterPublishCrash() {
        for (long i = 1; i <= 6; i++) {
            up(i, "v" + i);
        }
        t.setRetentionPolicy(RetentionPolicy.retainLatest(2));
        long base = t.currentVersion();

        assertThrows(ExpireHook.SimulatedInterruption.class,
                () -> ((LocalTableManager) t).expireSnapshots(base, ExpireHook.failAfterPublish()));

        // state is published; nothing deleted yet -> retained readable, expired still physically present too
        assertNotNull(t.readSnapshot(base));
        assertEquals(List.of(5L, 6L), t.readableVersions());

        // retry: cleanup completes, conclusion identical, unique
        ExpireResult retry = t.expireSnapshots(base);
        assertFalse(retry.committed(), "retry completes leftover cleanup without re-publishing");
        assertEquals(List.of(5L, 6L), retry.retainedVersions());
        assertExpired(List.of(0L, 1L, 2L, 3L, 4L));
        assertNotNull(t.readSnapshot(5L));
        assertNotNull(t.readSnapshot(6L));
    }

    @Test
    @DisplayName("crash mid-deletion: re-running yields a unique result and finishes cleanup")
    void resumeMidDeletion() throws Exception {
        for (long i = 1; i <= 6; i++) {
            up(i, "v" + i);
        }
        t.setRetentionPolicy(RetentionPolicy.retainLatest(2));
        long base = t.currentVersion();

        assertThrows(ExpireHook.SimulatedInterruption.class,
                () -> ((LocalTableManager) t).expireSnapshots(base, ExpireHook.failAfterDeletes(2)));
        assertThrows(ExpireHook.SimulatedInterruption.class,
                () -> ((LocalTableManager) t).expireSnapshots(base, ExpireHook.failAfterDeletes(3)));

        ExpireResult done = t.expireSnapshots(base);
        assertFalse(done.committed());
        // final cleanup deletes whatever remained; total physical deletion set is unique
        assertEquals(List.of(5L, 6L), t.readableVersions());
        assertExpired(List.of(0L, 1L, 2L, 3L, 4L));

        // no orphaned snapshot/temp files remain after convergence
        List<String> dataFiles = Files.list(t.tableDir().resolve("data"))
                .map(p -> p.getFileName().toString()).sorted().toList();
        assertTrue(dataFiles.stream().noneMatch(f -> f.endsWith(".tmp")), "no temp files left: " + dataFiles);
        long retainGenerations = dataFiles.stream().filter(f -> f.contains(".retain.v")).count();
        assertEquals(2, retainGenerations,
                "one materialized snapshot per retained version, prior generation reclaimed: " + dataFiles);
        assertTrue(dataFiles.stream().noneMatch(f -> f.endsWith(".data.json") || f.endsWith(".deletes.json")),
                "all original delta/delete files below base must be gone: " + dataFiles);

        // no double counting / drift on retained rows
        List<Row> latest = t.readSnapshot(null);
        assertEquals(6, latest.size());
        long distinct = latest.stream().map(r -> r.get("id")).distinct().count();
        assertEquals(6, distinct, "no duplicated rows");
    }

    @Test
    @DisplayName("crash before state publish leaves everything intact (all versions still readable)")
    void crashBeforePublishIsHarmless() {
        for (long i = 1; i <= 4; i++) {
            up(i, "v" + i);
        }
        // no hook point exists before publish other than materialization IO errors;
        // a failing call due to a stale base is the pre-publish abort path
        long base = t.currentVersion();
        up(5, "v5");
        assertThrows(TableException.class, () -> t.expireSnapshots(base));
        assertTrue(t.expiredVersions().isEmpty());
        for (long v = 0; v <= 5; v++) {
            assertNotNull(t.readSnapshot(v));
        }
    }

    // ----------------------------------------- 4. repeated expiration

    @Test
    @DisplayName("repeated expire with the same base/policy is idempotent and keeps a unique conclusion")
    void repeatedExpireStable() {
        for (long i = 1; i <= 6; i++) {
            up(i, "v" + i);
        }
        t.setRetentionPolicy(RetentionPolicy.retainLatest(2));
        long base = t.currentVersion();

        ExpireResult first = t.expireSnapshots(base);
        List<Row> latest = t.readSnapshot(null);
        ExpireResult second = t.expireSnapshots(base);
        ExpireResult third = t.expireSnapshots(base);

        assertEquals(0, second.filesDeleted().size(), "second run finds no garbage");
        assertEquals(0, third.filesDeleted().size());
        assertFalse(second.committed());
        assertEquals(latest, t.readSnapshot(null));
        assertEquals(6, t.readSnapshot(null).size());
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L), t.expiredVersions());
        assertTrue(first.bytesReclaimed() > 0);
    }

    // ----------------------------------------- 5. writes / evolution after GC

    @Test
    @DisplayName("new writes after GC replay correctly on the materialized latest snapshot")
    void writesAfterGc() {
        up(1, "a");
        up(2, "b");
        up(3, "c"); // base 3
        t.setRetentionPolicy(RetentionPolicy.retainLatest(1));
        t.expireSnapshots(3L);

        up(4, "d");  // v4 on top of materialized v3
        del(2);      // v5
        up(3, "c2"); // v6 update
        List<Row> rows = t.readSnapshot(null);
        Map<Object, String> byId = new java.util.TreeMap<>();
        rows.forEach(r -> byId.put(r.get("id"), (String) r.get("v")));
        assertEquals(Map.of(1L, "a", 3L, "c2", 4L, "d"), byId, "id=2 deleted, 3 updated, 1 and 4 present");

        // latest versions after the new commits are delta-replayed over the retained snapshot
        assertNotNull(t.readSnapshot(4L));
        assertNotNull(t.readSnapshot(5L));
        assertNotNull(t.readSnapshot(6L));
        // old versions below the new chain are expired
        assertExpired(List.of(0L, 1L, 2L));

        // a second GC now retains v6 and re-materializes; conclusion stays the same
        List<Row> beforeGc2 = t.readSnapshot(null);
        ExpireResult r2 = t.expireSnapshots(null);
        assertEquals(beforeGc2, t.readSnapshot(null));
        assertEquals(List.of(6L), r2.retainedVersions());
    }

    @Test
    @DisplayName("compaction after GC works and keeps the current content")
    void compactionAfterGc() {
        for (long i = 1; i <= 5; i++) {
            up(i, "v" + i);
        }
        t.setRetentionPolicy(RetentionPolicy.retainLatest(1));
        t.expireSnapshots(5L);

        List<Row> before = t.readSnapshot(null);
        long compactV = t.compact(null);
        assertEquals(6L, compactV);
        assertEquals(before, t.readSnapshot(null));
        assertEquals(before, t.readSnapshot(6L));
        // v5 retained snapshot still readable beneath the compaction delta (full snapshot at v6)
        assertEquals(5, t.readSnapshot(5L).size());
    }

    @Test
    @DisplayName("a pinned version under an older schema still reads with its own schema after GC + evolution")
    void pinnedVersionKeepsOldSchema() {
        up(1, "a"); // v1
        up(2, "b"); // v2
        t.setRetentionPolicy(RetentionPolicy.of(1, List.of(1L)));
        t.expireSnapshots(2L); // retain v1 (old schema) and v2

        long v3 = t.addColumn("tag", DataType.STRING, true, "legacy");
        assertEquals(3L, v3);
        up(3, "c"); // v4 writes with new column available

        // pinned v1: the column added later must not appear
        Row pinned = t.readSnapshot(1L).get(0);
        assertFalse(pinned.values().containsKey("tag"), "pinned old version keeps its own schema");
        // current fills the default
        assertEquals("legacy", t.readSnapshot(null).stream()
                .filter(r -> r.get("id").equals(1L)).findFirst().orElseThrow().get("tag"));
        // expired pre-GC versions stay expired
        assertExpired(List.of(0L));
    }

    // ----------------------------------------- 5b. multi-round expiration (regression for second+ GC runs)

    @Test
    @DisplayName("multi-round GC with an early pin and retainLatest=1: every later GC completes; pinned and latest rows never drift")
    void multiRoundGcWithPinnedVersion() {
        up(1, "a");   // v1 (pinned)
        up(2, "b");   // v2
        up(3, "c");   // v3
        up(4, "d");   // v4

        t.setRetentionPolicy(RetentionPolicy.of(1, List.of(1L)));
        List<Row> pinnedAtStart = t.readSnapshot(1L);

        // first GC: pins v1, keeps latest v4, expires 0/2/3
        ExpireResult gc1 = t.expireSnapshots(4L);
        assertTrue(gc1.committed());
        assertEquals(List.of(1L, 4L), gc1.retainedVersions());
        assertEquals(List.of(0L, 2L, 3L), gc1.expiredVersions());
        assertTrue(gc1.bytesReclaimed() > 0);
        assertEquals(pinnedAtStart, t.readSnapshot(1L));

        // round 1: two more commits (one insert, one delete), then GC at the new current
        up(5, "e");   // v5
        del(3);       // v6
        List<Row> live6 = t.readSnapshot(6L);
        ExpireResult gc2 = t.expireSnapshots(6L);
        assertTrue(gc2.committed(), "second GC after more writes must publish instead of crashing");
        assertEquals(List.of(1L, 6L), gc2.retainedVersions());
        assertEquals(List.of(4L, 5L), gc2.expiredVersions());
        assertTrue(gc2.bytesReclaimed() > 0);
        assertEquals(pinnedAtStart, t.readSnapshot(1L), "pinned v1 must not drift across GC rounds");
        assertEquals(live6, t.readSnapshot(6L), "retained latest must read identically");
        assertLive(Map.of(1L, "a", 2L, "b", 4L, "d", 5L, "e"));
        assertExpired(List.of(0L, 2L, 3L, 4L, 5L));

        // round 2: an update and an insert
        up(2, "b2");  // v7
        up(6, "f");   // v8
        List<Row> live8 = t.readSnapshot(8L);
        ExpireResult gc3 = t.expireSnapshots(8L);
        assertTrue(gc3.committed());
        assertEquals(List.of(1L, 8L), gc3.retainedVersions());
        assertEquals(List.of(6L, 7L), gc3.expiredVersions());
        assertEquals(pinnedAtStart, t.readSnapshot(1L));
        assertEquals(live8, t.readSnapshot(8L));
        assertLive(Map.of(1L, "a", 2L, "b2", 4L, "d", 5L, "e", 6L, "f"));

        // round 3: a delete and an update
        del(5);       // v9
        up(1, "a2");  // v10
        List<Row> live10 = t.readSnapshot(10L);
        ExpireResult gc4 = t.expireSnapshots(10L);
        assertTrue(gc4.committed());
        assertEquals(List.of(1L, 10L), gc4.retainedVersions());
        assertEquals(List.of(8L, 9L), gc4.expiredVersions());
        assertEquals(pinnedAtStart, t.readSnapshot(1L));
        assertEquals(live10, t.readSnapshot(10L));
        assertLive(Map.of(1L, "a2", 2L, "b2", 4L, "d", 6L, "f"));

        // round 4: pure inserts
        up(7, "g");   // v11
        up(8, "h");   // v12
        List<Row> live12 = t.readSnapshot(12L);
        ExpireResult gc5 = t.expireSnapshots(12L);
        assertTrue(gc5.committed());
        assertEquals(List.of(1L, 12L), gc5.retainedVersions());
        assertEquals(List.of(10L, 11L), gc5.expiredVersions());
        assertEquals(pinnedAtStart, t.readSnapshot(1L));
        assertEquals(live12, t.readSnapshot(12L));
        assertLive(Map.of(1L, "a2", 2L, "b2", 4L, "d", 6L, "f", 7L, "g", 8L, "h"));

        // expired versions are reported as VERSION_EXPIRED (existed) forever, not VERSION_NOT_FOUND
        assertExpired(List.of(0L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L));
        assertEquals(RejectReasonCode.VERSION_NOT_FOUND,
                assertThrows(TableException.class, () -> t.readSnapshot(123L)).code());

        // history still enumerates every commit; retained two are not marked expired
        List<CommitInfo> h = t.history();
        assertEquals(13, h.size());
        assertFalse(h.get(1).expired());
        assertFalse(h.get(12).expired());
        assertTrue(h.get(4).expired());

        // repeating the last GC is an idempotent no-op with a unique, stable conclusion
        ExpireResult rep1 = t.expireSnapshots(12L);
        ExpireResult rep2 = t.expireSnapshots(12L);
        assertFalse(rep1.committed());
        assertEquals(0, rep1.filesDeleted().size());
        assertEquals(0, rep2.filesDeleted().size());
        assertEquals(pinnedAtStart, t.readSnapshot(1L));
        assertEquals(live12, t.readSnapshot(null));
        assertEquals(List.of(1L, 12L), t.readableVersions());
    }

    @Test
    @DisplayName("retainLatest=2 with an early pin across multiple GC rounds keeps both windows and the pin exact")
    void multiRoundGcWithRetainLatestTwoAndPin() {
        up(1, "a"); // v1
        up(2, "b"); // v2
        up(3, "c"); // v3
        t.setRetentionPolicy(RetentionPolicy.of(2, List.of(1L)));
        List<Row> pinned = t.readSnapshot(1L);

        ExpireResult gc1 = t.expireSnapshots(3L);
        assertEquals(List.of(1L, 2L, 3L), gc1.retainedVersions());
        assertEquals(List.of(0L), gc1.expiredVersions());

        up(4, "d"); // v4
        up(5, "e"); // v5
        List<Row> v4 = t.readSnapshot(4L);
        List<Row> v5 = t.readSnapshot(5L);
        ExpireResult gc2 = t.expireSnapshots(5L);
        assertEquals(List.of(1L, 4L, 5L), gc2.retainedVersions());
        assertEquals(List.of(2L, 3L), gc2.expiredVersions());
        assertEquals(pinned, t.readSnapshot(1L));
        assertEquals(v4, t.readSnapshot(4L));
        assertEquals(v5, t.readSnapshot(5L));

        up(6, "f"); // v6
        List<Row> v6 = t.readSnapshot(6L);
        ExpireResult gc3 = t.expireSnapshots(6L);
        assertEquals(List.of(1L, 5L, 6L), gc3.retainedVersions());
        assertEquals(List.of(4L), gc3.expiredVersions());
        assertEquals(pinned, t.readSnapshot(1L));
        assertEquals(v5, t.readSnapshot(5L));
        assertEquals(v6, t.readSnapshot(6L));
        assertExpired(List.of(0L, 2L, 3L, 4L));
    }

    @Test
    @DisplayName("stale premise on a later GC round aborts the whole run with zero deletions; retrying on the fresh base succeeds")
    void staleBaseAbortsLaterGcWholeRun() throws Exception {
        for (long i = 1; i <= 6; i++) {
            up(i, "v" + i);
        }
        t.setRetentionPolicy(RetentionPolicy.of(1, List.of(1L)));
        List<Row> pinned = t.readSnapshot(1L);
        t.expireSnapshots(6L); // retained [1,6]

        // a commit lands before the second round runs
        up(7, "v7"); // current -> 7
        long filesBefore = countDataFiles();

        TableException e = assertThrows(TableException.class, () -> t.expireSnapshots(6L));
        assertEquals(RejectReasonCode.RETENTION_CONFLICT, e.code());
        assertEquals(filesBefore, countDataFiles(), "conflicted later GC must delete zero files");
        // neither the previously committed latest nor the new commit became unreadable
        assertNotNull(t.readSnapshot(6L));
        assertNotNull(t.readSnapshot(7L));
        assertEquals(pinned, t.readSnapshot(1L));

        // retrying on the fresh base completes normally
        ExpireResult r = t.expireSnapshots(7L);
        assertTrue(r.committed());
        assertEquals(List.of(1L, 7L), r.retainedVersions());
        assertEquals(List.of(6L), r.expiredVersions());
        assertEquals(pinned, t.readSnapshot(1L));
        assertNotNull(t.readSnapshot(7L));
        assertExpired(List.of(0L, 2L, 3L, 4L, 5L, 6L));

        // and again on the next round: conflict then success, the table never gets stuck
        up(8, "v8");
        assertThrows(TableException.class, () -> t.expireSnapshots(7L));
        ExpireResult r3 = t.expireSnapshots(8L);
        assertEquals(List.of(1L, 8L), r3.retainedVersions());
        assertEquals(pinned, t.readSnapshot(1L));
    }

    @Test
    @DisplayName("true concurrent commit vs a later-round GC: the GC conflicts and deletes nothing; convergence always succeeds")
    void concurrentCommitDuringLaterGc() throws Exception {
        for (long i = 1; i <= 4; i++) {
            up(i, "v" + i);
        }
        t.setRetentionPolicy(RetentionPolicy.of(1, List.of(1L)));
        List<Row> pinned = t.readSnapshot(1L);
        t.expireSnapshots(4L);

        final long base = t.currentVersion(); // 4
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<TableException> gcFut = pool.submit(() -> {
                start.await();
                try {
                    t.expireSnapshots(base);
                    return null;
                } catch (TableException ex) {
                    return ex;
                }
            });
            Future<?> writeFut = pool.submit(() -> {
                start.await();
                up(42L, "concurrent");
                return null;
            });
            start.countDown();
            TableException gcErr = gcFut.get(30, TimeUnit.SECONDS);
            writeFut.get(30, TimeUnit.SECONDS);

            if (gcErr != null) {
                assertEquals(RejectReasonCode.RETENTION_CONFLICT, gcErr.code());
            }
            long current = t.currentVersion();
            assertNotNull(t.readSnapshot(current), "the committed current version is always readable");
            assertEquals(pinned, t.readSnapshot(1L));
            assertNotNull(t.readSnapshot(4L));

            // converging on whatever actually won must always complete
            ExpireResult r = t.expireSnapshots(t.currentVersion());
            assertEquals(pinned, t.readSnapshot(1L));
            assertTrue(r.retainedVersions().contains(1L));
            assertTrue(r.retainedVersions().contains(current));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("crash during a later GC round (after publish / mid-delete): retries converge, then GC keeps working")
    void interruptionDuringLaterGcRound() throws Exception {
        for (long i = 1; i <= 8; i++) {
            up(i, "v" + i);
        }
        t.setRetentionPolicy(RetentionPolicy.of(1, List.of(1L)));
        List<Row> pinned = t.readSnapshot(1L);
        t.expireSnapshots(8L);

        up(9, "v9");  // v9
        up(10, "v10"); // v10
        List<Row> live10 = t.readSnapshot(10L);

        // crash right after the second-round state was published
        assertThrows(ExpireHook.SimulatedInterruption.class,
                () -> ((LocalTableManager) t).expireSnapshots(10L, ExpireHook.failAfterPublish()));
        assertEquals(List.of(1L, 10L), t.readableVersions());
        assertEquals(pinned, t.readSnapshot(1L));
        assertEquals(live10, t.readSnapshot(10L));

        // crash again in the middle of the leftover deletions
        assertThrows(ExpireHook.SimulatedInterruption.class,
                () -> ((LocalTableManager) t).expireSnapshots(10L, ExpireHook.failAfterDeletes(1)));
        assertEquals(pinned, t.readSnapshot(1L));
        assertEquals(live10, t.readSnapshot(10L));

        // plain retry converges without re-publishing; conclusion is unique
        ExpireResult done = t.expireSnapshots(10L);
        assertFalse(done.committed());
        assertEquals(List.of(1L, 10L), done.retainedVersions());
        assertEquals(pinned, t.readSnapshot(1L));
        assertEquals(live10, t.readSnapshot(10L));
        assertExpired(List.of(0L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L));
        ExpireResult again = t.expireSnapshots(10L);
        assertEquals(0, again.filesDeleted().size(), "no repeated deletions / double counting");

        // only the two current-generation materialized snapshots remain; no orphans or temp files
        List<String> dataFiles = Files.list(t.tableDir().resolve("data"))
                .map(p -> p.getFileName().toString()).sorted().toList();
        assertEquals(List.of("v1.retain.v2.json", "v10.retain.v2.json"), dataFiles,
                "exactly the retained snapshots of the newest generation remain: " + dataFiles);

        // the interruption must not wedge future rounds
        up(11, "v11"); // v11
        ExpireResult next = t.expireSnapshots(11L);
        assertTrue(next.committed());
        assertEquals(List.of(1L, 11L), next.retainedVersions());
        assertEquals(pinned, t.readSnapshot(1L));
        assertNotNull(t.readSnapshot(11L));
    }

    @Test
    @DisplayName("loosening the policy never revives already-expired versions and never breaks later GC runs")
    void loosenedPolicyDoesNotReviveExpiredVersions() {
        up(1, "a");
        up(2, "b");
        up(3, "c");
        up(4, "d");
        t.setRetentionPolicy(RetentionPolicy.retainLatest(1));
        t.expireSnapshots(4L); // retained [4], expired 0..3
        assertExpired(List.of(0L, 1L, 2L, 3L));

        // loosen without any new commit: same base cannot re-materialize physically removed versions
        t.setRetentionPolicy(RetentionPolicy.retainLatest(5));
        ExpireResult same = t.expireSnapshots(4L);
        assertFalse(same.committed());
        assertEquals(0, same.filesDeleted().size());
        assertEquals(List.of(4L), same.retainedVersions());
        assertExpired(List.of(0L, 1L, 2L, 3L));

        // pinning a now-expired version remains invalid and must not mutate the stored policy
        TableException pe = assertThrows(TableException.class,
                () -> t.setRetentionPolicy(RetentionPolicy.of(5, List.of(2L))));
        assertEquals(RejectReasonCode.RETENTION_POLICY_INVALID, pe.code());
        assertEquals(5, t.retentionPolicy().retainLatestVersions());

        // new commits + GC under the loosened policy still cannot revive old versions;
        // v4 was the retained latest and simply stays readable alongside v5
        up(5, "e");
        ExpireResult r5 = t.expireSnapshots(5L);
        assertEquals(List.of(4L, 5L), r5.retainedVersions(),
                "window 1..5 minus already-expired 0..3 leaves the still-materialized v4 plus v5");
        assertEquals(List.of(), r5.expiredVersions());
        assertExpired(List.of(0L, 1L, 2L, 3L));
        assertNotNull(t.readSnapshot(4L));
        assertNotNull(t.readSnapshot(5L));
    }

    @Test
    @DisplayName("bulky history over multiple GC rounds: managed data footprint keeps dropping and rows stay correct")
    void multiRoundGcFootprintKeepsDropping() throws Exception {
        String fat = "z".repeat(220);
        for (long v = 1; v <= 10; v++) {
            List<RowOp> ops = new ArrayList<>();
            for (long id = 1; id <= 15; id++) {
                ops.add(RowOp.upsert(Row.of("id", id, "v", fat + "-r" + v + "-id" + id)));
            }
            t.write(new WriteBatch(ops));
        }
        // keep only the latest version: each round can reclaim the previous bulky deltas
        t.setRetentionPolicy(RetentionPolicy.retainLatest(1));

        // commit manifests are immutable and retained forever by design, so the
        // reclaim-observable footprint is the data directory GC actually manages
        long beforeGc1 = sizeOf(t.tableDir().resolve("data"));
        ExpireResult gc1 = t.expireSnapshots(10L);
        long afterGc1 = sizeOf(t.tableDir().resolve("data"));
        assertTrue(gc1.bytesReclaimed() > 0);
        assertTrue(afterGc1 < beforeGc1,
                "first GC must shrink the data footprint: before=" + beforeGc1 + " after=" + afterGc1);

        long previous = afterGc1;
        for (long round = 1; round <= 3; round++) {
            for (int k = 0; k < 3; k++) {
                List<RowOp> ops = new ArrayList<>();
                for (long id = 1; id <= 15; id++) {
                    ops.add(RowOp.upsert(Row.of("id", id, "v", fat + "-round" + round + "-" + k)));
                }
                t.write(new WriteBatch(ops));
            }
            long base = t.currentVersion();
            ExpireResult r = t.expireSnapshots(base);
            assertTrue(r.bytesReclaimed() > 0, "round " + round + " must reclaim bytes");
            // after each GC the data directory holds exactly one managed file:
            // the current generation's materialized snapshot (prior generation + deltas gone)
            List<String> files = Files.list(t.tableDir().resolve("data"))
                    .map(p -> p.getFileName().toString()).sorted().toList();
            assertEquals(1, files.size(), "only the newest retained snapshot may remain: " + files);
            assertTrue(files.get(0).equals("v" + base + ".retain.v" + (round + 1) + ".json"),
                    "unexpected surviving file: " + files);
            long now = sizeOf(t.tableDir().resolve("data"));
            assertTrue(now <= previous * 2,
                    "round " + round + " data footprint stays a single snapshot, not an accumulating chain: "
                            + now + " vs " + previous);
            previous = now;
            assertEquals(15, t.readSnapshot(null).size(), "no lost or duplicated rows after round " + round);
        }
        assertEquals(15, t.readSnapshot(null).stream().map(r -> r.get("id")).distinct().count(),
                "no duplicated rows in the final snapshot");
    }

    private void assertLive(java.util.Map<Long, String> expectedIdToV) {
        java.util.Map<Object, String> byId = new java.util.TreeMap<>();
        t.readSnapshot(null).forEach(row -> byId.put(row.get("id"), (String) row.get("v")));
        assertEquals(expectedIdToV, byId);
    }

    private long countDataFiles() throws Exception {
        try (var s = Files.list(t.tableDir().resolve("data"))) {
            return s.filter(Files::isRegularFile).count();
        }
    }

    // ----------------------------------------- 6. reopen persistence

    @Test
    @DisplayName("retention state survives reopen: expired stay expired, retained stay readable")
    void reopenAfterGc() {
        for (long i = 1; i <= 5; i++) {
            up(i, "v" + i);
        }
        t.setRetentionPolicy(RetentionPolicy.of(2, List.of(1L)));
        List<Row> pinBefore = t.readSnapshot(1L);
        t.expireSnapshots(5L);

        TableManager t2 = new LocalLakehouseEngine(tmp).openTable("t");
        assertEquals(List.of(1L, 4L, 5L), t2.readableVersions());
        assertTrue(t2.expiredVersions().contains(2L));
        assertEquals(pinBefore, t2.readSnapshot(1L));
        assertEquals(RejectReasonCode.VERSION_EXPIRED,
                assertThrows(TableException.class, () -> t2.readSnapshot(2L)).code());
        assertEquals(RejectReasonCode.VERSION_NOT_FOUND,
                assertThrows(TableException.class, () -> t2.readSnapshot(100L)).code());

        // GC again on the reopened manager is a stable no-op
        ExpireResult again = t2.expireSnapshots(5L);
        assertEquals(0, again.filesDeleted().size());
    }

    // ----------------------------------------- 7b. GC after an earlier compaction

    @Test
    @DisplayName("GC after a prior compaction: materialization composes with the natural full snapshot")
    void gcAfterCompaction() {
        up(1, "a"); // v1
        up(2, "b"); // v2
        long compactV = t.compact(null); // v3 full snapshot
        up(3, "c");                      // v4 delta on top of the v3 full snapshot
        up(3, "c2");                     // v5
        Map<Long, List<Row>> before = capture(5);

        t.setRetentionPolicy(RetentionPolicy.of(2, List.of(1L)));
        ExpireResult r = t.expireSnapshots(5L);
        // pinned v1 sits below the v3 compaction full snapshot; it must be independently materialized
        assertEquals(List.of(1L, 4L, 5L), r.retainedVersions());
        assertReadable(before, List.of(1L, 4L, 5L));
        assertEquals(before.get(5L), t.readSnapshot(null));
        assertExpired(List.of(0L, 2L, 3L));

        // a following compaction over the GC boundary still works
        List<Row> latest = t.readSnapshot(null);
        long v6 = t.compact(null);
        assertEquals(6L, v6);
        assertEquals(latest, t.readSnapshot(6L));
    }

    // ----------------------------------------- 8. disk reclaim with bulky data

    @Test
    @DisplayName("bulky history: total on-disk footprint drops after expiration")
    void diskFootprintDrops() throws Exception {
        String fat = "z".repeat(300);
        for (long v = 1; v <= 8; v++) {
            List<RowOp> ops = new ArrayList<>();
            for (long id = 1; id <= 20; id++) {
                ops.add(RowOp.upsert(Row.of("id", id, "v", fat + "-v" + v + "-" + id)));
            }
            t.write(new WriteBatch(ops));
        }
        long before = sizeOf(t.tableDir());
        t.setRetentionPolicy(RetentionPolicy.retainLatest(1));
        ExpireResult r = t.expireSnapshots(null);
        long after = sizeOf(t.tableDir());
        assertTrue(r.bytesReclaimed() > 0);
        assertTrue(after < before, "footprint must shrink: before=" + before + " after=" + after);
    }

    private static long sizeOf(Path dir) throws Exception {
        try (var s = Files.walk(dir)) {
            return s.filter(Files::isRegularFile).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (Exception e) {
                    return 0L;
                }
            }).sum();
        }
    }
}
