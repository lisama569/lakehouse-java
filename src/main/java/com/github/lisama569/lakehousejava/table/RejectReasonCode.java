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
    COMPACTION_CONFLICT,
    EMPTY_TABLE_NAME
}
