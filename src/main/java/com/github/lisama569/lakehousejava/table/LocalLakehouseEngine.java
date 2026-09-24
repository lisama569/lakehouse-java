package com.github.lisama569.lakehousejava.table;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Filesystem-backed {@link LakehouseEngine}.
 *
 * <pre>
 *   &lt;warehouseRoot&gt;/
 *     &lt;table-name&gt;/
 *       _lakehouse.json
 *       commits/0.commit.json   empty full snapshot at creation
 *       commits/1.commit.json   first write / schema change ...
 *       data/...
 * </pre>
 */
public final class LocalLakehouseEngine implements LakehouseEngine {

    private final Path warehouseRoot;
    private final JsonStore json = new JsonStore();

    public LocalLakehouseEngine(Path warehouseRoot) {
        this.warehouseRoot = warehouseRoot;
        try {
            Files.createDirectories(warehouseRoot);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create warehouse root " + warehouseRoot, e);
        }
    }

    @Override
    public TableManager createTable(String name, Schema schema) {
        validateName(name);
        Path dir = warehouseRoot.resolve(name);
        Path metaFile = dir.resolve(LocalTableManager.META_FILE);
        if (Files.exists(metaFile)) {
            throw new TableException(RejectReasonCode.SCHEMA_COLUMN_DUPLICATE,
                    "table already exists: " + name);
        }
        try {
            Files.createDirectories(dir.resolve("commits"));
            Files.createDirectories(dir.resolve("data"));
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create table directory " + dir, e);
        }

        // Version 0 = empty full snapshot; the anchor for all delta replay chains.
        CommitRecord v0 = new CommitRecord(0, -1L, CommitKind.SCHEMA_CHANGE,
                System.currentTimeMillis(), "create table with initial schema", true, schema);
        json.writeAtomic(dir.resolve("commits").resolve("0.commit.json"), v0);

        TableMetadata meta = new TableMetadata(name, schema, 0);
        json.writeAtomic(metaFile, meta);
        return new LocalTableManager(dir, name, json);
    }

    @Override
    public TableManager openTable(String name) {
        validateName(name);
        Path dir = warehouseRoot.resolve(name);
        if (!Files.isRegularFile(dir.resolve(LocalTableManager.META_FILE))) {
            throw new TableException(RejectReasonCode.VERSION_NOT_FOUND,
                    "no such table: " + name);
        }
        return new LocalTableManager(dir, name, json);
    }

    @Override
    public Path warehouseRoot() {
        return warehouseRoot;
    }

    public boolean tableExists(String name) {
        validateName(name);
        return Files.isRegularFile(warehouseRoot.resolve(name).resolve(LocalTableManager.META_FILE));
    }

    private static void validateName(String name) {
        if (name == null || name.isBlank()) {
            throw new TableException(RejectReasonCode.EMPTY_TABLE_NAME, "table name must not be blank");
        }
        if (!name.matches("[A-Za-z0-9_\\-]+")) {
            throw new TableException(RejectReasonCode.EMPTY_TABLE_NAME,
                    "table name must match [A-Za-z0-9_-]+ but was: " + name);
        }
    }
}
