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

        println("-- schema evolution: add currency (default USD), widen amount LONG -> DOUBLE --");
        if (t.currentSchema().column("currency") == null) {
            long vAdd = t.addColumn("currency", DataType.STRING, false, "USD");
            println("addColumn committed newVersion=" + vAdd);
        } else {
            println("column currency already present (skipping add on reopen)");
        }
        if (t.currentSchema().column("amount").type() != DataType.DOUBLE) {
            long vWiden = t.changeColumnType("amount", DataType.DOUBLE);
            println("widenColumn committed newVersion=" + vWiden);
        } else {
            println("column amount already DOUBLE (skipping widen on reopen)");
        }

        println("-- forbidden evolutions --");
        tryReject(() -> t.changeColumnType("region", DataType.LONG), "STRING -> LONG");
        tryReject(() -> t.dropColumn("amount"), "drop column");
        tryReject(() -> t.addColumn("currency", DataType.STRING, true, null), "duplicate column");

        println("-- stale base version write must conflict --");
        long staleBase = t.currentVersion() - 1; // definitely behind after at least one commit
        tryReject(() -> t.write(new WriteBatch(
                List.of(RowOp.upsert(Row.of("order_id", "late", "region", "US", "amount", 1L))),
                staleBase)), "write on expired base v" + staleBase);

        long travelV = t.readableVersions().stream().findFirst().orElse(t.currentVersion());
        println("-- time travel: oldest still-readable version v" + travelV + " stays stable --");
        List<Row> oldRows = t.readSnapshot(travelV);
        println("v" + travelV + " rows=" + oldRows.size()
                + (oldRows.isEmpty() ? "" : " first=" + oldRows.get(0).values()));
        if (t.expiredVersions().contains(1L)) {
            tryReject(() -> t.readSnapshot(1L), "read expired v1 (distinct from never-existed)");
        }
        println("current rows=" + t.readSnapshot(null).size());

        println("-- compaction: full snapshot rewrite, retained old files untouched --");
        long compactV = t.compact(null);
        println("compaction committed newVersion=" + compactV
                + " rows=" + t.readSnapshot(null).size());
        println("v" + travelV + " after compaction rows=" + t.readSnapshot(travelV).size()
                + " (identical conclusion: " + t.readSnapshot(travelV).equals(oldRows) + ")");

        println("-- retention & expiration (explicit trigger only; default keeps everything) --");
        demoRetention(t);

        println("-- history --");
        for (CommitInfo ci : t.history()) {
            println("  v" + ci.version() + " " + ci.kind() + (ci.expired() ? " [EXPIRED]" : "")
                    + " : " + ci.description());
        }
        println("== demo finished. Inspect JSON under " + warehouseRoot.resolve(tableName) + " ==");
    }

    private static void demoRetention(TableManager t) {
        long current = t.currentVersion();
        long pinCandidate = Math.max(1L, current - 1);
        // On a re-run against the same directory the candidate may already be expired;
        // an expired version cannot be pinned, so fall back to the current version then.
        long pin = t.expiredVersions().contains(pinCandidate) ? current : pinCandidate;
        println("setting policy: retainLatest=1, pin v" + pin + " (policy alone deletes nothing)");
        t.setRetentionPolicy(RetentionPolicy.of(1, List.of(pin)));
        println("  readable before expire: " + t.readableVersions());

        List<Row> pinnedBefore = t.readSnapshot(pin);
        List<Row> latestBefore = t.readSnapshot(null);

        ExpireResult r = t.expireSnapshots(null);
        println("expire committed=" + r.committed()
                + " base=v" + r.baseVersion()
                + " retained=" + r.retainedVersions()
                + " expiredNow=" + r.expiredVersions()
                + " filesDeleted=" + r.filesDeleted().size()
                + " bytesReclaimed=" + r.bytesReclaimed());

        // retained versions keep an identical row-by-row conclusion
        println("  pinned v" + pin + " identical=" + pinnedBefore.equals(t.readSnapshot(pin))
                + ", latest identical=" + latestBefore.equals(t.readSnapshot(null)));
        println("  readable after expire: " + t.readableVersions());

        // expired version vs a version that never existed are distinguishable
        if (!r.expiredVersions().isEmpty()) {
            long gone = r.expiredVersions().get(0);
            tryReject(() -> t.readSnapshot(gone), "read expired v" + gone);
        }
        tryReject(() -> t.readSnapshot(999L), "read never-existed v999");

        // running again is a stable no-op
        ExpireResult again = t.expireSnapshots(null);
        println("repeated expire committed=" + again.committed()
                + " filesDeleted=" + again.filesDeleted().size() + " (idempotent)");
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
