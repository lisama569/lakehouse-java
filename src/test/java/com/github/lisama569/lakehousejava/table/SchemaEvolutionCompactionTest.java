package com.github.lisama569.lakehousejava.table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers: additive schema evolution with deterministic defaults interpretable by
 * new and old versions, forbidden incompatible changes, and compaction that
 * preserves every version's query conclusion.
 */
class SchemaEvolutionCompactionTest {

    @TempDir
    Path tmp;

    private TableManager t;

    @BeforeEach
    void setUp() {
        LakehouseEngine engine = new LocalLakehouseEngine(tmp);
        Schema schema = Schemas.builder()
                .column("id", DataType.LONG, false, null)
                .column("amount", DataType.LONG, true, null)
                .primaryKey("id")
                .build();
        t = engine.createTable("ledger", schema);
    }

    private void seed() {
        t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", 1L, "amount", 10L)),
                RowOp.upsert(Row.of("id", 2L, "amount", 20L)))));
    }

    @Test
    @DisplayName("new nullable column gets its deterministic default on old rows and new rows")
    void addColumnDefaultBackfill() {
        seed();
        long v2 = t.addColumn("tag", DataType.STRING, true, "legacy");
        assertEquals(2L, v2);

        // old snapshot keeps exactly the old columns (historical interpretation preserved)
        Row oldRow = t.readSnapshot(1L).get(0);
        assertFalse(oldRow.values().containsKey("tag"), "v1 rows must not fabricate columns");

        // reading old data through the new schema fills the declared default
        List<Row> current = t.readSnapshot(null);
        assertEquals("legacy", current.get(0).get("tag"));
        assertEquals("legacy", current.get(1).get("tag"));
        assertEquals(DataType.STRING, t.schemaAt(null).column("tag").type());
        // historical schema (v1) genuinely did not know the column
        assertNull(t.schemaAt(1L).column("tag"));
    }

    @Test
    @DisplayName("rows written after addColumn can specify the new column; absent still defaults")
    void newRowsAfterEvolution() {
        seed();
        t.addColumn("tag", DataType.STRING, true, "legacy");
        t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", 3L, "amount", 30L, "tag", "fresh")),
                RowOp.upsert(Row.of("id", 1L, "amount", 11L))))); // update old key, tag omitted -> default

        var byId = t.readSnapshot(null).stream().collect(java.util.stream.Collectors.toMap(r -> r.get("id"), r -> r));
        assertEquals("fresh", byId.get(3L).get("tag"));
        assertEquals("legacy", byId.get(1L).get("tag"), "upsert without the column gets the default");
        assertEquals(20L, byId.get(2L).get("amount"));
    }

    @Test
    @DisplayName("NOT NULL column without default cannot be added")
    void addNotNullWithoutDefaultRejected() {
        TableException e = assertThrows(TableException.class,
                () -> t.addColumn("required", DataType.STRING, false, null));
        assertEquals(RejectReasonCode.SCHEMA_EVOLUTION_INCOMPATIBLE, e.code());
    }

    @Test
    @DisplayName("duplicate column name and bad default are rejected distinctly")
    void addColumnValidation() {
        TableException dup = assertThrows(TableException.class,
                () -> t.addColumn("id", DataType.STRING, true, null));
        assertEquals(RejectReasonCode.SCHEMA_COLUMN_DUPLICATE, dup.code());

        TableException badDefault = assertThrows(TableException.class,
                () -> t.addColumn("d", DataType.LONG, true, "not-a-long"));
        assertEquals(RejectReasonCode.SCHEMA_EVOLUTION_INCOMPATIBLE, badDefault.code());
    }

    @Test
    @DisplayName("LONG -> DOUBLE widening is allowed; STRING -> LONG is rejected")
    void typeEvolutionRules() {
        seed();
        long v = t.changeColumnType("amount", DataType.DOUBLE);
        assertEquals(2L, v);
        // stored longs read as doubles after widening (current schema)
        assertEquals(10.0d, t.readSnapshot(null).get(0).get("amount"));
        // historical schema still says LONG and old versions read their native values
        assertEquals(DataType.LONG, t.schemaAt(1L).column("amount").type());
        assertEquals(10L, t.readSnapshot(1L).get(0).get("amount"),
                "v1 is interpreted with the v1 schema: amount remains LONG");

        TableException e = assertThrows(TableException.class,
                () -> t.changeColumnType("amount", DataType.BOOLEAN));
        assertEquals(RejectReasonCode.SCHEMA_EVOLUTION_INCOMPATIBLE, e.code());

        TableException unknown = assertThrows(TableException.class,
                () -> t.changeColumnType("ghost", DataType.STRING));
        assertEquals(RejectReasonCode.SCHEMA_EVOLUTION_INCOMPATIBLE, unknown.code());
    }

    @Test
    @DisplayName("dropping an existing column is always rejected with SCHEMA_COLUMN_DROP_FORBIDDEN")
    void dropColumnForbidden() {
        TableException e = assertThrows(TableException.class, () -> t.dropColumn("amount"));
        assertEquals(RejectReasonCode.SCHEMA_COLUMN_DROP_FORBIDDEN, e.code());
        TableException pk = assertThrows(TableException.class, () -> t.dropColumn("id"));
        assertEquals(RejectReasonCode.SCHEMA_COLUMN_DROP_FORBIDDEN, pk.code());
    }

    @Test
    @DisplayName("compaction rewrites a full snapshot; every old version stays reproducible")
    void compactionPreservesAllVersions() {
        // build a chain with inserts, update, delete, schema evolution
        seed();                                                  // v1: 2 rows
        t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", 1L, "amount", 11L)),
                RowOp.delete(Row.of("id", 2L)))));               // v2: 1 row (id=1 -> 11)
        t.addColumn("tag", DataType.STRING, true, "legacy");     // v3
        t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("id", 3L, "amount", 30L, "tag", "x"))))); // v4: 2 rows

        // snapshot expectations captured before compaction
        List<Row> v1Before = t.readSnapshot(1L);
        List<Row> v2Before = t.readSnapshot(2L);
        List<Row> v3Before = t.readSnapshot(3L);
        List<Row> v4Before = t.readSnapshot(4L);

        long v5 = t.compact(4L);
        assertEquals(5L, v5);
        CommitInfo compactCommit = t.history().get(5);
        assertEquals(CommitKind.COMPACTION, compactCommit.kind());

        // latest content identical, no loss / duplication
        List<Row> v5Rows = t.readSnapshot(5L);
        assertEquals(v4Before, v5Rows, "compaction must not change the current table content");
        assertEquals(2, v5Rows.size());

        // every historical version yields the exact same conclusion after compaction
        assertEquals(v1Before, t.readSnapshot(1L));
        assertEquals(v2Before, t.readSnapshot(2L));
        assertEquals(v3Before, t.readSnapshot(3L));
        assertEquals(v4Before, t.readSnapshot(4L));

        // compaction based on an expired version is rejected
        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", 9L, "amount", 90L))))); // v6
        TableException e = assertThrows(TableException.class, () -> t.compact(5L));
        assertEquals(RejectReasonCode.COMPACTION_CONFLICT, e.code());
        assertEquals(6L, t.currentVersion());
    }

    @Test
    @DisplayName("repeated compaction keeps the result stable and counts unchanged")
    void repeatedCompactionStable() {
        seed();
        t.compact(null);
        List<Row> first = t.readSnapshot(null);
        t.compact(null);
        t.compact(null);
        List<Row> later = t.readSnapshot(null);
        assertEquals(first, later);
        assertEquals(2, later.size());
        assertEquals(1, later.stream().map(r -> r.get("id")).filter(id -> id.equals(1L)).count(),
                "no duplicated counts after repeated compaction");
    }
}
