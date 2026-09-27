package com.github.lisama569.lakehousejava.table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers historical-version retention and explicit vacuum:
 * policy persistence, dependency (replay-chain) protection, concurrency premise,
 * interruption retry, repeated/idempotent cleanup, expired-vs-never-existed reads,
 * and observable disk-space reclamation.
 */
class RetentionVacuumTest {

    @TempDir
    Path tmp;

    private LakehouseEngine engine;
    private TableManager t;

    @BeforeEach
    void setUp() {
        engine = new LocalLakehouseEngine(tmp);
        Schema schema = Schemas.builder()
                .column("id", DataType.LONG, false, null)
                .column("amount", DataType.LONG, true, null)
                .primaryKey("id")
                .build();
        t = engine.createTable("ledger", schema);
    }

    /** Build versions 1..5, compact at 6; compacted v6 is the full-snapshot anchor. */
    private void buildHistoryWithCompaction() {
        t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", 1L, "amount", 10L)),
                RowOp.upsert(Row.of("id", 2L, "amount", 20L)))));            // v1
        t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", 1L, "amount", 11L)),
                RowOp.delete(Row.of("id", 2L)))));                            // v2
        t.addColumn("tag", DataType.STRING, true, "legacy");                  // v3
        t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", 3L, "amount", 30L, "tag", "x"))))); // v4
        t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", 4L, "amount", 40L)))));             // v5
        t.compact(null);                                                      // v6 full snapshot
    }

    private long dataBytes() {
        Path data = t.tableDir().resolve("data");
        if (!Files.isDirectory(data)) {
            return 0L;
        }
        try (var stream = Files.walk(data)) {
            return stream.filter(Files::isRegularFile).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).sum();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private long dataFileCount() {
        Path data = t.tableDir().resolve("data");
        if (!Files.isDirectory(data)) {
            return 0L;
        }
        try (var stream = Files.list(data)) {
            return stream.filter(Files::isRegularFile).count();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ------------------------------------------------------------- policy

    @Test
    @DisplayName("retention policy persists, survives reopen, and does not change versions")
    void policyPersistence() {
        buildHistoryWithCompaction();
        long v = t.currentVersion();
        t.setRetentionPolicy(RetentionPolicy.of(2, 1L, 4L));

        RetentionPolicy p = t.retentionPolicy();
        assertEquals(2, p.minVersionsToKeep());
        assertEquals(List.of(1L, 4L), p.pinnedVersions());
        assertEquals(v, t.currentVersion(), "setting a policy must not add a version");

        TableManager reopened = new LocalLakehouseEngine(tmp).openTable("ledger");
        RetentionPolicy p2 = reopened.retentionPolicy();
        assertEquals(2, p2.minVersionsToKeep());
        assertEquals(List.of(1L, 4L), p2.pinnedVersions());
    }

    @Test
    @DisplayName("default behavior is unchanged: without a policy nothing is ever deleted")
    void defaultKeepsEverything() {
        buildHistoryWithCompaction();
        long filesBefore = dataFileCount();
        assertNull(t.retentionPolicy());
        // normal operations keep working and files accumulate but are never removed
        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", 5L, "amount", 50L)))));
        t.compact(null);
        assertTrue(dataFileCount() > filesBefore);
        // every historical version v0..v6 stays readable with no policy set
        for (long v = 0L; v <= 6L; v++) {
            final long version = v;
            assertDoesNotThrow(() -> t.readSnapshot(version), "version " + version + " must remain readable by default");
        }
        assertEquals(2, t.readSnapshot(1L).size());
    }

    @Test
    @DisplayName("vacuum without a policy is rejected with RETENTION_POLICY_MISSING")
    void vacuumWithoutPolicyRejected() {
        buildHistoryWithCompaction();
        TableException e = assertThrows(TableException.class, () -> t.vacuum(null));
        assertEquals(RejectReasonCode.RETENTION_POLICY_MISSING, e.code());
    }

    @Test
    @DisplayName("pinning a non-existent version or negative window is rejected")
    void invalidPolicyRejected() {
        buildHistoryWithCompaction();
        TableException missing = assertThrows(TableException.class,
                () -> t.setRetentionPolicy(RetentionPolicy.of(1, 99L)));
        assertEquals(RejectReasonCode.RETENTION_POLICY_INVALID, missing.code());

        assertThrows(IllegalArgumentException.class, () -> RetentionPolicy.keepRecent(-1));
    }

    // ----------------------------------------------- dependency protection / reads

    @Test
    @DisplayName("vacuum keeps pinned and latest versions byte-conclusion identical; old versions expire")
    void pinnedAndLatestProtected() {
        buildHistoryWithCompaction();

        // capture every version's conclusion before cleanup
        List<List<Row>> before = new java.util.ArrayList<>();
        for (long v = 0; v <= 6L; v++) {
            before.add(t.readSnapshot(v));
        }

        t.setRetentionPolicy(RetentionPolicy.of(1, 1L)); // pin v1, keep only latest otherwise
        long bytesBefore = dataBytes();
        VacuumResult r = t.vacuum(6L);

        assertFalse(r.noOp());
        assertTrue(r.bytesReclaimed() > 0L, "disk usage must drop, was " + bytesBefore);
        assertTrue(dataBytes() < bytesBefore, "data directory must shrink after vacuum");

        // latest (v6) and pinned (v1) remain exactly as before
        assertEquals(before.get(6), t.readSnapshot(6L));
        assertEquals(before.get(1), t.readSnapshot(1L), "pinned version must stay reproducible");
        assertEquals(before.get(6), t.readSnapshot(null));

        // retainedVersions reports what survived; expired versions are explicitly reported
        assertTrue(r.retainedVersions().contains(6L));
        assertTrue(r.retainedVersions().contains(1L));
        for (Long expired : r.expiredVersions()) {
            assertFalse(expired.equals(1L) || expired.equals(6L), "pinned/latest never expire");
        }

        // a reclaimed version is unreadable, but the reason differs from "never existed"
        TableException gone = assertThrows(TableException.class, () -> t.readSnapshot(2L));
        assertEquals(RejectReasonCode.VERSION_EXPIRED, gone.code());
        TableException never = assertThrows(TableException.class, () -> t.readSnapshot(42L));
        assertEquals(RejectReasonCode.VERSION_NOT_FOUND, never.code());

        // the expired version still appears in history and keeps its schema (manifest retained)
        CommitInfo ci = t.history().get(2);
        assertEquals(2L, ci.version());
        assertNotNull(t.schemaAt(2L));
    }

    @Test
    @DisplayName("pinned version's whole replay chain is protected even when the anchor is old")
    void pinnedChainProtection() {
        // no compaction: v0 is the full-snapshot anchor for every version
        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", 1L, "amount", 1L))))); // v1
        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", 2L, "amount", 2L))))); // v2
        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", 3L, "amount", 3L))))); // v3
        List<Row> pinnedBefore = t.readSnapshot(2L);

        t.setRetentionPolicy(RetentionPolicy.of(1, 2L));
        VacuumResult r = t.vacuum(3L);

        // v2 chain is v0(anchor)+v1+v2: none of those may be deleted; so nothing is reclaimable
        assertTrue(r.noOp(), "pinned chain shares the only anchor/deltas => no file can be dropped");
        assertEquals(pinnedBefore, t.readSnapshot(2L));
        assertEquals(2, pinnedBefore.size());
    }

    // ------------------------------------------------- concurrency / premise

    @Test
    @DisplayName("vacuum aborts wholesale when a commit lands during the operation")
    void vacuumAbortsOnConcurrentCommit() {
        buildHistoryWithCompaction();
        long base = t.currentVersion();
        t.setRetentionPolicy(RetentionPolicy.keepRecent(1));
        List<Row> latestBefore = t.readSnapshot(null);

        // A second manager on the same directory commits during the vacuum's plan->delete window.
        TableManager other = new LocalLakehouseEngine(tmp).openTable("ledger");
        // capture the garbage set the vacuum planned, via a tiny introspection on disk after planning:
        // files present before the concurrent commit but not referenced by v6 are exactly the candidates.
        List<String> plannedGarbage = collectGarbageCandidates(t, base, RetentionPolicy.keepRecent(1));
        assertFalse(plannedGarbage.isEmpty(), "fixture must produce reclaimable candidates");

        LocalTableManager concurrent = new LocalTableManager(t.tableDir(), "ledger") {
            @Override
            void beforeVacuumDelete(long plannedBase) {
                // runs while the first vacuum is not holding the lock
                other.write(new WriteBatch(List.of(
                        RowOp.upsert(Row.of("id", 99L, "amount", 99L)))));
            }
        };

        TableException e = assertThrows(TableException.class, () -> concurrent.vacuum(base));
        assertEquals(RejectReasonCode.VACUUM_CONFLICT, e.code());

        // no half-cleanup: every file the vacuum had planned to delete is still on disk
        for (String rel : plannedGarbage) {
            assertTrue(Files.isRegularFile(t.tableDir().resolve(rel)),
                    "aborted vacuum must not have deleted " + rel);
        }
        assertEquals(base + 1, t.currentVersion());
        List<Row> after = t.readSnapshot(null);
        assertTrue(after.stream().anyMatch(r -> r.get("id").equals(99L)), "concurrent commit must survive");
        assertEquals(latestBefore.size() + 1, after.size());
        // the aborted vacuum left no ledger entry
        assertEquals(0, t.vacuumHistory().size());
        // every old version is still readable because nothing was deleted
        assertEquals(latestBefore, t.readSnapshot(base));
    }

    /** Mirror of the manager's candidate-garbage rule, computed read-only for test assertions. */
    private static List<String> collectGarbageCandidates(TableManager table, long base, RetentionPolicy policy) {
        java.util.TreeSet<Long> retained = new java.util.TreeSet<>();
        retained.add(base);
        for (long v = Math.max(0L, base - (long) policy.minVersionsToKeep() + 1); v <= base; v++) {
            retained.add(v);
        }
        policy.pinnedVersions().forEach(retained::add);

        // files in each retained version's replay chain (anchor snapshot -> version) are protected
        java.util.Set<String> retainedSet = new java.util.HashSet<>();
        for (long rv : retained) {
            long cursor = rv;
            while (true) {
                CommitRecord c = readCommit(table, cursor);
                retainedSet.addAll(c.dataFiles);
                retainedSet.addAll(c.deleteFiles);
                if (c.fullSnapshot) {
                    break;
                }
                cursor--;
            }
        }
        java.util.TreeSet<String> garbage = new java.util.TreeSet<>();
        for (long v = 0; v <= base; v++) {
            CommitRecord cr = readCommit(table, v);
            for (String rel : cr.dataFiles) {
                if (!retainedSet.contains(rel) && Files.isRegularFile(table.tableDir().resolve(rel))) {
                    garbage.add(rel);
                }
            }
            for (String rel : cr.deleteFiles) {
                if (!retainedSet.contains(rel) && Files.isRegularFile(table.tableDir().resolve(rel))) {
                    garbage.add(rel);
                }
            }
        }
        return new java.util.ArrayList<>(garbage);
    }

    private static CommitRecord readCommit(TableManager table, long v) {
        Path p = table.tableDir().resolve("commits").resolve(v + ".commit.json");
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().readValue(Files.readAllBytes(p), CommitRecord.class);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Test
    @DisplayName("vacuum based on an already-stale premise is rejected before any work")
    void vacuumStalePremiseRejected() {
        buildHistoryWithCompaction();
        t.setRetentionPolicy(RetentionPolicy.keepRecent(1));

        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", 7L, "amount", 70L))))); // now v7
        List<String> candidates = collectGarbageCandidates(t, 6L, RetentionPolicy.keepRecent(1));

        TableException e = assertThrows(TableException.class, () -> t.vacuum(6L));
        assertEquals(RejectReasonCode.VACUUM_CONFLICT, e.code());
        // rejected up front: not even the v6-era garbage may be touched
        for (String rel : candidates) {
            assertTrue(Files.isRegularFile(t.tableDir().resolve(rel)));
        }
        assertEquals(0, t.vacuumHistory().size());
    }

    // ---------------------------------------------- interruption / idempotency

    @Test
    @DisplayName("an interrupted vacuum is safely retried: unique deterministic end state")
    void interruptedVacuumIsRetriable() throws Exception {
        buildHistoryWithCompaction();
        t.setRetentionPolicy(RetentionPolicy.of(1, 1L));
        long base = t.currentVersion();
        List<Row> latestBefore = t.readSnapshot(null);
        List<Row> pinnedBefore = t.readSnapshot(1L);

        // First attempt "crashes" after deleting a couple of files.
        LocalTableManager crashing = new LocalTableManager(t.tableDir(), "ledger") {
            @Override
            void beforeVacuumFileDelete(long b, int index, int total) {
                if (index == 2) {
                    throw new IllegalStateException("simulated crash mid-vacuum");
                }
            }
        };
        assertThrows(IllegalStateException.class, () -> crashing.vacuum(base));

        // partial deletion happened, but the crash left no completed ledger entry
        assertEquals(0, t.vacuumHistory().size());
        // retained versions must still be readable even after the partial run
        assertEquals(latestBefore, t.readSnapshot(base), "latest version intact after partial vacuum");
        assertEquals(pinnedBefore, t.readSnapshot(1L), "pinned version intact after partial vacuum");

        // retry on a fresh manager instance, with the same base: converges to a clean result
        LocalTableManager retrying = new LocalTableManager(t.tableDir(), "ledger");
        VacuumResult retry = retrying.vacuum(base);
        assertFalse(retry.noOp());
        assertEquals(latestBefore, t.readSnapshot(null));
        assertEquals(pinnedBefore, t.readSnapshot(1L));
        // only files actually still present got deleted in the retry; nothing is double-counted
        long deletedCount = retry.deletedFiles().stream().distinct().count();
        assertEquals(deletedCount, retry.deletedFiles().size(), "no duplicate deletions");

        // final conclusion is stable and unique
        long filesAfter = dataFileCount();
        VacuumResult again = t.vacuum(base);
        assertTrue(again.noOp(), "a subsequent vacuum over the same base has nothing to do");
        assertEquals(filesAfter, dataFileCount());
        assertEquals(1, t.vacuumHistory().size(), "exactly one completed vacuum recorded");

        // expired versions now report VERSION_EXPIRED, retained ones read identically
        TableException expired = assertThrows(TableException.class, () -> t.readSnapshot(2L));
        assertEquals(RejectReasonCode.VERSION_EXPIRED, expired.code());
        assertEquals(pinnedBefore, t.readSnapshot(1L));
    }

    @Test
    @DisplayName("repeated vacuums are idempotent: no row loss, duplicate count, or conclusion drift")
    void repeatedVacuumStable() {
        buildHistoryWithCompaction();
        t.setRetentionPolicy(RetentionPolicy.of(1, 1L));
        List<Row> latestBefore = t.readSnapshot(null);
        List<Row> pinnedBefore = t.readSnapshot(1L);

        VacuumResult first = t.vacuum(null);
        long bytesAfterFirst = dataBytes();
        VacuumResult second = t.vacuum(null);
        VacuumResult third = t.vacuum(null);

        assertTrue(second.noOp() && third.noOp());
        assertEquals(bytesAfterFirst, dataBytes());
        assertEquals(latestBefore, t.readSnapshot(null));
        assertEquals(pinnedBefore, t.readSnapshot(1L));
        // repeated cleanup must not duplicate rows
        long idOne = t.readSnapshot(null).stream().filter(r -> r.get("id").equals(1L)).count();
        assertEquals(1, idOne);
        // only the first run recorded a completed ledger entry
        assertEquals(1, t.vacuumHistory().size());
        assertEquals(first.bytesReclaimed(), t.vacuumHistory().get(0).bytesReclaimed);
    }

    @Test
    @DisplayName("new writes after a vacuum are protected by the next vacuum's fresh premise")
    void writesAfterVacuumThenVacuumAgain() {
        buildHistoryWithCompaction();
        t.setRetentionPolicy(RetentionPolicy.keepRecent(1));
        VacuumResult first = t.vacuum(null);
        assertFalse(first.noOp());

        // new delta commit after cleanup; the compact anchor v6 file still exists so this is readable
        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", 8L, "amount", 80L, "tag", "z")))));
        List<Row> rows = t.readSnapshot(null);
        assertTrue(rows.stream().anyMatch(r -> r.get("id").equals(8L)));

        // compact again so the post-cleanup deltas become a new anchor, then vacuum
        long cv = t.compact(null);
        VacuumResult second = t.vacuum(cv);
        assertEquals(cv, second.baseVersion());
        assertTrue(second.bytesReclaimed() >= 0L);
        assertTrue(t.readSnapshot(null).stream().anyMatch(r -> r.get("id").equals(8L)));
    }

    @Test
    @DisplayName("retention, expiry and ledger survive reopening the table")
    void vacuumStateSurvivesReopen() {
        buildHistoryWithCompaction();
        t.setRetentionPolicy(RetentionPolicy.of(1, 1L));
        VacuumResult r = t.vacuum(null);
        long bytes = dataBytes();

        TableManager reopened = new LocalLakehouseEngine(tmp).openTable("ledger");
        assertEquals(1, reopened.retentionPolicy().minVersionsToKeep());
        assertEquals(List.of(1L), reopened.retentionPolicy().pinnedVersions());
        assertEquals(r.retainedVersions(), reopened.vacuumHistory().get(0).retainedVersions);
        assertEquals(r.expiredVersions(), reopened.vacuumHistory().get(0).expiredVersions);
        assertEquals(bytes, dataBytes());

        // pinned and latest still read; expired still explained after reopen
        assertEquals(2, reopened.readSnapshot(1L).size());
        assertDoesNotThrow(() -> reopened.readSnapshot(null));
        TableException e = assertThrows(TableException.class, () -> reopened.readSnapshot(2L));
        assertEquals(RejectReasonCode.VERSION_EXPIRED, e.code());
    }
}

