package com.github.lisama569.lakehousejava.table;

import java.nio.file.Path;
import java.util.List;

/**
 * Fully offline end-to-end demonstration. Run via:
 * <pre>mvn spring-boot:run -Dspring-boot.run.arguments=demo</pre>
 * Data lands under {@code ./lakehouse-demo-data} and can be inspected as plain JSON.
 */
public final class LocalLakehouseDemo {

    private LocalLakehouseDemo() {
    }

    public static void run(Path warehouseRoot) {
        println("== local lakehouse demo, warehouse = " + warehouseRoot.toAbsolutePath());
        LocalLakehouseEngine engine = new LocalLakehouseEngine(warehouseRoot);

        String tableName = "demo_orders";
        TableManager t;
        if (engine.tableExists(tableName)) {
            t = engine.openTable(tableName);
            println("opened existing table at version " + t.currentVersion());
        } else {
            Schema schema = Schemas.builder()
                    .column("order_id", DataType.STRING, false, null)
                    .column("region", DataType.STRING, false, "EU")
                    .column("amount", DataType.LONG, true, null)
                    .primaryKey("order_id")
                    .build();
            t = engine.createTable(tableName, schema);
            println("[v" + t.currentVersion() + "] table created with columns " + schema.columnNames());
        }

        println("-- batch A: duplicate key o-1 must converge (last op wins) --");
        WriteResult a = t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("order_id", "o-1", "region", "US", "amount", 100L)),
                RowOp.upsert(Row.of("order_id", "o-1", "region", "US", "amount", 150L)),
                RowOp.upsert(Row.of("order_id", "o-2", "amount", 200L)))));
        println("decision committed=" + a.committed() + " newVersion=" + a.newVersion()
                + " upserted=" + a.upserted() + " duplicatesCollapsed=" + a.duplicateKeysCollapsed());

        println("-- batch B: invalid rows, must be rejected with distinct reasons, no partial effect --");
        WriteResult b = t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("region", "US")),
                RowOp.upsert(Row.of("order_id", "o-3", "amount", "not-a-number")),
                RowOp.upsert(Row.of("order_id", "o-4", "amount", 40L, "mystery", 1)))));
        println("decision committed=" + b.committed() + " (version stays " + t.currentVersion() + ")");
        b.rejections().forEach(r -> println("  reject opIndex=" + r.opIndex()
                + " reason=" + r.reason() + " detail=" + r.detail()));

        println("-- batch C: update + delete --");
        WriteResult c = t.write(new WriteBatch(List.of(
                RowOp.upsert(Row.of("order_id", "o-1", "region", "US", "amount", 160L)),
                RowOp.delete(Row.of("order_id", "o-2")))));
        println("decision committed=" + c.committed() + " newVersion=" + c.newVersion()
                + " upserted=" + c.upserted() + " deleted=" + c.deleted());

        long snapshotBeforeEvolve = t.currentVersion();

        println("-- schema evolution: add currency (default USD), widen amount LONG -> DOUBLE --");
        long vAdd = t.addColumn("currency", DataType.STRING, false, "USD");
        println("addColumn committed newVersion=" + vAdd);
        long vWiden = t.changeColumnType("amount", DataType.DOUBLE);
        println("widenColumn committed newVersion=" + vWiden);

        println("-- forbidden evolutions --");
        tryReject(() -> t.changeColumnType("region", DataType.LONG), "STRING -> LONG");
        tryReject(() -> t.dropColumn("amount"), "drop column");
        tryReject(() -> t.addColumn("currency", DataType.STRING, true, null), "duplicate column");

        println("-- stale base version write must conflict --");
        tryReject(() -> t.write(new WriteBatch(
                List.of(RowOp.upsert(Row.of("order_id", "late", "region", "US", "amount", 1L))),
                snapshotBeforeEvolve)), "write on expired base v" + snapshotBeforeEvolve);

        println("-- time travel: version 1 still returns the two original rows --");
        List<Row> v1 = t.readSnapshot(1L);
        println("v1 rows=" + v1.size() + " first=" + v1.get(0).values());
        println("current rows=" + t.readSnapshot(null).size());

        println("-- compaction: full snapshot rewrite, old files retained --");
        long compactV = t.compact(null);
        println("compaction committed newVersion=" + compactV
                + " rows=" + t.readSnapshot(null).size());
        println("v1 after compaction rows=" + t.readSnapshot(1L).size()
                + " (identical conclusion: " + t.readSnapshot(1L).equals(v1) + ")");

        println("-- history --");
        for (CommitInfo ci : t.history()) {
            println("  v" + ci.version() + " " + ci.kind() + " : " + ci.description());
        }
        println("== demo finished. Inspect JSON under " + warehouseRoot.resolve(tableName) + " ==");
    }

    private static void tryReject(Runnable action, String what) {
        try {
            action.run();
            println("  UNEXPECTED ACCEPT: " + what);
        } catch (TableException e) {
            println("  rejected: " + what + " -> " + e.code() + " (" + e.getMessage() + ")");
        }
    }

    private static void println(String s) {
        System.out.println("[demo] " + s);
    }
}
