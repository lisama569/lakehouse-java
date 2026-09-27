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

    // ------------------------------------------------------------ retention / GC

    /**
     * Set the history retention policy (null = keep everything, the default).
     * This only records intent: no files are removed until {@link #expireSnapshots(Long)}
     * is explicitly invoked.
     */
    void setRetentionPolicy(RetentionPolicy policy);

    /** Currently configured retention policy; defaults to "keep all". */
    RetentionPolicy retentionPolicy();

    /** Versions still readable right now (all versions before the first GC run). */
    List<Long> readableVersions();

    /** Versions that once existed but were expired by a retention run. */
    List<Long> expiredVersions();

    /**
     * Explicitly expire snapshots according to the current policy.
     *
     * @param expectedBaseVersion if non-null, the run is rejected with
     *                            {@link RejectReasonCode#RETENTION_CONFLICT} when the
     *                            table advanced since this base was observed
     * @return a deterministic summary of what was retained/expired/reclaimed
     */
    ExpireResult expireSnapshots(Long expectedBaseVersion);
}
