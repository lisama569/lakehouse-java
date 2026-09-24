package com.github.lisama569.lakehousejava.table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers: upsert/update/delete by primary key, deterministic duplicate-key
 * convergence inside a batch, and distinguishable rejection reasons.
 */
class WriteValidationTest {

    @TempDir
    Path tmp;

    private TableManager t;

    @BeforeEach
    void setUp() {
        LakehouseEngine engine = new LocalLakehouseEngine(tmp);
        Schema schema = Schemas.builder()
                .column("id", DataType.LONG, false, null)
                .column("name", DataType.STRING, false, null)
                .column("score", DataType.DOUBLE, true, 0.0d)
                .primaryKey("id")
                .build();
        t = engine.createTable("users", schema);
    }

    private Map<Object, Row> byId(List<Row> rows) {
        return rows.stream().collect(Collectors.toMap(r -> r.get("id"), Function.identity()));
    }

    @Test
    @DisplayName("duplicate keys in one batch converge to the last op deterministically")
    void duplicateKeysLastWins() {
        WriteResult r = t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", 1L, "name", "first")),
                RowOp.upsert(Row.of("id", 1L, "name", "second")),
                RowOp.upsert(Row.of("id", 2L, "name", "keep")),
                RowOp.upsert(Row.of("id", 1L, "name", "third")))));

        assertTrue(r.committed());
        assertEquals(1L, r.newVersion());
        assertEquals(2, r.upserted());
        assertEquals(2, r.duplicateKeysCollapsed(), "two of the four ops share id=1 and must collapse");

        Map<Object, Row> rows = byId(t.readSnapshot(null));
        assertEquals(2, rows.size());
        assertEquals("third", rows.get(1L).get("name"), "last op for a key wins");
        assertEquals("keep", rows.get(2L).get("name"));
    }

    @Test
    @DisplayName("upsert updates existing rows across batches; delete removes by key")
    void upsertAcrossBatchesAndDelete() {
        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", 1L, "name", "alice")))));
        WriteResult upd = t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", 1L, "name", "alice2")),
                RowOp.delete(Row.of("id", 2L)))));
        assertTrue(upd.committed());
        assertEquals(2L, upd.newVersion());
        assertEquals(1, upd.upserted());
        assertEquals(1, upd.deleted());
        assertEquals("alice2", byId(t.readSnapshot(null)).get(1L).get("name"));

        WriteResult del = t.write(new WriteBatch(List.of(RowOp.delete(Row.of("id", 1L)))));
        assertTrue(del.committed());
        assertTrue(t.readSnapshot(null).isEmpty(), "delete of an existing key removes the row");
    }

    @Test
    @DisplayName("a delete later in the same batch overrides an upsert for the key")
    void deleteWinsOverUpsertWithinBatch() {
        WriteResult r = t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", 1L, "name", "ghost")),
                RowOp.delete(Row.of("id", 1L)))));
        assertTrue(r.committed());
        assertEquals(0, r.upserted());
        assertEquals(1, r.deleted());
        assertTrue(t.readSnapshot(null).isEmpty());
    }

    @Test
    @DisplayName("missing primary key is rejected with MISSING_PRIMARY_KEY")
    void missingPrimaryKey() {
        WriteResult r = t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("name", "nokey")))));
        assertFalse(r.committed());
        assertNull(r.newVersion(), "rejected batches must not create a version");
        assertEquals(1, r.rejections().size());
        assertEquals(RejectReasonCode.MISSING_PRIMARY_KEY, r.rejections().get(0).reason());
        assertEquals(0L, t.currentVersion());
    }

    @Test
    @DisplayName("type mismatch is rejected with TYPE_MISMATCH and distinguishes from missing key")
    void typeMismatch() {
        WriteResult r = t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", "not-a-number", "name", "x")))));
        assertFalse(r.committed());
        assertEquals(RejectReasonCode.TYPE_MISMATCH, r.rejections().get(0).reason());

        WriteResult r2 = t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", 1L, "name", "x", "score", true)))));
        assertFalse(r2.committed());
        assertEquals(RejectReasonCode.TYPE_MISMATCH, r2.rejections().get(0).reason());
    }

    @Test
    @DisplayName("unknown column (whole extra column) is rejected with UNKNOWN_COLUMN")
    void unknownColumn() {
        WriteResult r = t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", 1L, "name", "x", "surprise", "v")))));
        assertFalse(r.committed());
        assertEquals(RejectReasonCode.UNKNOWN_COLUMN, r.rejections().get(0).reason());
    }

    @Test
    @DisplayName("missing value for a NOT NULL column without default is rejected with MISSING_REQUIRED_VALUE")
    void missingRequiredValue() {
        WriteResult r = t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", 1L)))));
        assertFalse(r.committed());
        assertEquals(RejectReasonCode.MISSING_REQUIRED_VALUE, r.rejections().get(0).reason());
    }

    @Test
    @DisplayName("a batch with one bad op is fully rejected; valid ops do not partially take effect")
    void noPartialEffect() {
        WriteResult r = t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", "bad1", "name", "broken1")),
                RowOp.upsert(Row.of("id", "bad2", "name", "broken2")))));
        assertFalse(r.committed());
        assertEquals(2, r.rejections().size(), "both ops are validated and reported independently");
        assertEquals(List.of(0, 1),
                r.rejections().stream().map(RejectedRow::opIndex).toList());
        assertTrue(t.readSnapshot(null).isEmpty(), "nothing from the batch may land");
        assertEquals(0L, t.currentVersion());
    }

    @Test
    @DisplayName("repeating the same batch twice yields the same final state (idempotent upsert)")
    void repeatedBatchDeterministic() {
        List<RowOp> batch = List.of(
                RowOp.upsert(Row.of("id", 1L, "name", "a")),
                RowOp.upsert(Row.of("id", 2L, "name", "b")));
        t.write(new WriteBatch(batch));
        t.write(new WriteBatch(batch));
        t.write(new WriteBatch(batch));

        List<Row> rows = t.readSnapshot(null);
        assertEquals(2, rows.size());
        assertEquals(3L, t.currentVersion(), "each commit still gets its own version");
        // content is identical regardless of repetition
        assertEquals(List.of("a", "b"), rows.stream().map(r -> (String) r.get("name")).toList());
    }
}
