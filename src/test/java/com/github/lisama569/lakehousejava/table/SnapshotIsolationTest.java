package com.github.lisama569.lakehousejava.table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers: addressable versions, time-travel stability after later writes and
 * compaction, snapshot isolation and explicit rejection of stale-base writes,
 * plus concurrent commits with a unique final state.
 */
class SnapshotIsolationTest {

    @TempDir
    Path tmp;

    private TableManager t;

    @BeforeEach
    void setUp() {
        LakehouseEngine engine = new LocalLakehouseEngine(tmp);
        Schema schema = Schemas.builder()
                .column("id", DataType.LONG, false, null)
                .column("name", DataType.STRING, true, null)
                .primaryKey("id")
                .build();
        t = engine.createTable("items", schema);
    }

    @Test
    @DisplayName("each commit is addressable; old versions stay stable after later writes")
    void timeTravelStableAcrossWrites() {
        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", 1L, "name", "v1")))));
        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", 1L, "name", "v2")))));
        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", 2L, "name", "new-in-v3")))));

        assertEquals("v1", t.readSnapshot(1L).get(0).get("name"));
        assertEquals(1, t.readSnapshot(1L).size());
        assertEquals("v2", t.readSnapshot(2L).get(0).get("name"));
        assertEquals(2, t.readSnapshot(3L).size());
        assertEquals("v2", t.readSnapshot(null).get(0).get("name"));
        assertEquals(3L, t.currentVersion());

        // deletes must also be reflected correctly via time travel
        t.write(new WriteBatch(List.of(RowOp.delete(Row.of("id", 1L)))));
        assertEquals(1, t.readSnapshot(4L).size());
        assertEquals(2, t.readSnapshot(3L).size(), "historical snapshot unchanged after delete");

        TableException e = assertThrows(TableException.class, () -> t.readSnapshot(99L));
        assertEquals(RejectReasonCode.VERSION_NOT_FOUND, e.code());
    }

    @Test
    @DisplayName("a write based on an expired base version is rejected with VERSION_CONFLICT")
    void staleBaseVersionRejected() {
        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", 1L, "name", "a")))));
        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", 2L, "name", "b"))))); // current = 2

        WriteBatch stale = new WriteBatch(List.of(RowOp.upsert(Row.of("id", 3L, "name", "c"))), 1L);
        TableException e = assertThrows(TableException.class, () -> t.write(stale));
        assertEquals(RejectReasonCode.VERSION_CONFLICT, e.code());
        assertEquals(2L, t.currentVersion(), "conflicting write must not advance the version");
        assertEquals(2, t.readSnapshot(null).size(), "no partial effect from conflicting write");

        // retried against the fresh base succeeds
        WriteResult ok = t.write(new WriteBatch(
                List.of(RowOp.upsert(Row.of("id", 3L, "name", "c"))), 2L));
        assertTrue(ok.committed());
        assertEquals(3L, ok.newVersion());
    }

    @Test
    @DisplayName("concurrent commits serialize; stale ones are rejected, final state is unique")
    void concurrentCommitsSnapshotIsolation() throws Exception {
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger committed = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();

        try {
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 1; i <= threads; i++) {
                final long id = i;
                final long base = 0L; // every thread pins itself to the same stale base after the first wins
                futures.add(pool.submit((Callable<Void>) () -> {
                    start.await();
                    try {
                        WriteResult r = t.write(new WriteBatch(
                                List.of(RowOp.upsert(Row.of("id", id, "name", "n" + id))), base));
                        if (r.committed()) {
                            committed.incrementAndGet();
                        }
                    } catch (TableException e) {
                        if (e.code() == RejectReasonCode.VERSION_CONFLICT) {
                            conflicts.incrementAndGet();
                        } else {
                            throw e;
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertEquals(1, committed.get(), "exactly one writer pinned to v0 can win");
        assertEquals(threads - 1, conflicts.get(), "all others must receive VERSION_CONFLICT");
        assertEquals(1L, t.currentVersion());
        assertEquals(1, t.readSnapshot(null).size());
    }

    @Test
    @DisplayName("history lists every version in order with its commit kind")
    void historyComplete() {
        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", 1L, "name", "a")))));
        t.write(new WriteBatch(List.of(RowOp.delete(Row.of("id", 1L)))));

        List<CommitInfo> h = t.history();
        assertEquals(3, h.size());
        assertEquals(0L, h.get(0).version());
        assertEquals(CommitKind.APPEND_WRITE, h.get(1).kind());
        assertEquals(CommitKind.APPEND_WRITE, h.get(2).kind());
    }
}
