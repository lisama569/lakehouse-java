package com.github.lisama569.lakehousejava.table;

/**
 * Metadata for one committed version.
 *
 * @param expired true when the version was expired by a retention run;
 *                its data is gone and reading it throws VERSION_EXPIRED
 */
public record CommitInfo(long version, CommitKind kind, long timestampMs, String description, boolean expired) {

    public CommitInfo(long version, CommitKind kind, long timestampMs, String description) {
        this(version, kind, timestampMs, description, false);
    }
}
