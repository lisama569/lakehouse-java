package com.github.lisama569.lakehousejava.table;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * One full offline lifecycle: writes with duplicate convergence and rejections,
 * time travel, schema evolution, concurrent stale-base commits, compaction and
 * re-verification. Batches, versions and decisions are logged.
 */
class EndToEndIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(EndToEndIntegrationTest.class);

    @TempDir
    Path tmp;

    @Test
    void fullLifecycle() throws Exception {
        LakehouseEngine engine = new LocalLakehouseEngine(tmp);
        Schema v0Schema = Schemas.builder()
                .column("order_id", DataType.STRING, false, null)
                .column("region", DataType.STRING, false, "EU")
                .column("amount", DataType.LONG, true, null)
                .primaryKey("order_id")
                .build();
        TableManager orders = engine.createTable("orders", v0Schema);
        log.info("[step] created table orders at version={} schema={}", orders.currentVersion(),
                v0Schema.columnNames());

        WriteResult batch1 = orders.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("order_id", "o-1", "region", "US", "amount", 100L)),
                RowOp.upsert(Row.of("order_id", "o-1", "region", "US", "amount", 150L)), // duplicate key
                RowOp.upsert(Row.of("order_id", "o-2", "amount", 200L)))));             // default region
        log.info("[step] batch1 result={}", resultDigest(batch1));
        assertTrue(batch1.committed());
        assertEquals(1, batch1.duplicateKeysCollapsed());
        assertEquals(2, batch1.upserted());

        WriteResult bad = orders.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("region", "US")),                                     // missing PK
                RowOp.upsert(Row.of("order_id", "o-3", "amount", "oops")))));            // type mismatch
        log.info("[step] bad batch committed={} rejections={}", bad.committed(), bad.rejections());
        assertFalse(bad.committed());
        assertEquals(List.of(RejectReasonCode.MISSING_PRIMARY_KEY, RejectReasonCode.TYPE_MISMATCH),
                bad.rejections().stream().map(RejectedRow::reason).toList());

        Object snapshotV1 = orders.readSnapshot(1L);
        orders.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("order_id", "o-3", "region", "AP", "amount", 300L)),
                RowOp.delete(Row.of("order_id", "o-2")))));
        log.info("[step] v2 rows={}, v1 rows still={}", orders.readSnapshot(null).size(),
                ((List<?>) snapshotV1).size());
        assertEquals(2, orders.readSnapshot(null).size());
        assertEquals(2, orders.readSnapshot(1L).size(), "time travel stable");

        orders.addColumn("currency", DataType.STRING, false, "USD");
        orders.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("order_id", "o-4", "region", "EU", "amount", 400L, "currency", "EUR")))));
        long v5 = orders.changeColumnType("amount", DataType.DOUBLE);
        log.info("[step] schema evolved to v{} columns={}", v5, orders.currentSchema().columnNames());
        assertEquals(150.0d, orders.readSnapshot(null).stream()
                .filter(r -> r.get("order_id").equals("o-1")).findFirst().orElseThrow().get("amount"));

        // two writers pinned to the same old version: one wins, one conflicts
        final long base = orders.currentVersion();
        Runnable staleWriter = () -> {
            TableException e = assertThrows(TableException.class, () -> orders.write(new WriteBatch(
                    List.of(RowOp.upsert(Row.of("order_id", "late", "region", "US", "amount", 1L))),
                    base)));
            assertEquals(RejectReasonCode.VERSION_CONFLICT, e.code());
        };
        Thread w1 = new Thread(() -> {
            try {
                orders.write(new WriteBatch(
                        List.of(RowOp.upsert(Row.of("order_id", "win", "region", "US", "amount", 1L))),
                        base));
            } catch (TableException e) {
                assertEquals(RejectReasonCode.VERSION_CONFLICT, e.code());
            }
        });
        Thread w2 = new Thread(staleWriter);
        w1.start();
        w2.start();
        w1.join();
        w2.join();

        List<Row> before = orders.readSnapshot(null);
        long compactV = orders.compact(null);
        log.info("[step] compacted to v{}, pre-compact rows={}, post-compact rows={}",
                compactV, before.size(), orders.readSnapshot(null).size());
        assertEquals(before, orders.readSnapshot(null));
        assertEquals(2, orders.readSnapshot(1L).size(), "old versions still identical after compaction");
    }

    private static String resultDigest(WriteResult r) {
        return "committed=" + r.committed() + " newVersion=" + r.newVersion()
                + " upserted=" + r.upserted() + " deleted=" + r.deleted()
                + " duplicatesCollapsed=" + r.duplicateKeysCollapsed()
                + " rejections=" + r.rejections();
    }
}
