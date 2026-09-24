package com.github.lisama569.lakehousejava.table;

import java.nio.file.Path;
import java.util.List;

/**
 * Operations available on a single local lakehouse table.
 */
public interface TableManager {

    String name();

    Path tableDir();

    long currentVersion();

    Schema currentSchema();

    WriteResult write(WriteBatch batch);

    List<Row> readSnapshot(Long versionOrNull);

    Schema schemaAt(Long versionOrNull);

    List<CommitInfo> history();

    long addColumn(String columnName, DataType type, boolean nullable, Object defaultValue);

    /** Widen an existing column type; incompatible changes are rejected. */
    long changeColumnType(String columnName, DataType newType);

    /** Dropping existing columns is always rejected (recorded for explicitness/tests). */
    long dropColumn(String columnName);

    long compact(Long expectedBaseVersion);
}
