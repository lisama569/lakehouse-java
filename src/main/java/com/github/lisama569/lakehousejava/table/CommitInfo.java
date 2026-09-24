package com.github.lisama569.lakehousejava.table;

/**
 * Metadata for one committed version.
 */
public record CommitInfo(long version, CommitKind kind, long timestampMs, String description) {
}
