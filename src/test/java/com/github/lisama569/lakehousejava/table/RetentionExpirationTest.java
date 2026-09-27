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
