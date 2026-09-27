package com.github.lisama569.lakehousejava.table;

/**
 * Distinguishable rejection reasons for writes and schema changes.
 */
public enum RejectReasonCode {
    MISSING_PRIMARY_KEY,
    TYPE_MISMATCH,
    UNKNOWN_COLUMN,
    MISSING_REQUIRED_VALUE,
    SCHEMA_EVOLUTION_INCOMPATIBLE,
    SCHEMA_COLUMN_DROP_FORBIDDEN,
    SCHEMA_COLUMN_DUPLICATE,
    VERSION_CONFLICT,
    VERSION_NOT_FOUND,
    /** Reading a version that once existed but was reclaimed by an explicit vacuum. */
    VERSION_EXPIRED,
    COMPACTION_CONFLICT,
    /** A vacuum based on an expired current-version premise (a concurrent commit landed). */
    VACUUM_CONFLICT,
    /** Vacuum invoked while the table has no retention policy set. */
    RETENTION_POLICY_MISSING,
    /** Retention policy references an invalid version or parameter. */
    RETENTION_POLICY_INVALID,
    EMPTY_TABLE_NAME
}
