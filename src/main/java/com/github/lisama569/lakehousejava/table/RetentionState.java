package com.github.lisama569.lakehousejava.table;

import java.util.ArrayList;
import java.util.List;

/**
 * On-disk retention state ({@code _retention.json}): which versions are still
 * readable after the latest explicit expiration run, how each retained version
 * is materialized on disk, and which versions were expired (and when).
 *
 * <p>This file is the single source of truth for readers: it is atomically
 * replaced <em>before</em> any obsolete file is deleted, so a crash at any
 * point leaves either the old state (old files still needed) or the new state
 * (only redundant garbage left for the next run).</p>
 */
public final class RetentionState {

    /** Latest version the last expiration run was based on; -1 when GC never ran. */
    public long gcBaseVersion = -1L;

    /** Audit counter of successful expiration runs. */
    public long gcGeneration = 0L;

    /** Timestamp of the last published expiration state. */
    public long updatedAtMs = 0L;

    /** Versions that remain readable, sorted ascending. */
    public List<Entry> retained = new ArrayList<>();

    /** All versions ever expired (cumulative, sorted ascending). */
    public List<ExpiredEntry> expired = new ArrayList<>();

    public RetentionState() {
    }

    /** One retained version and how to read it. */
    public static final class Entry {
        public long version;
        /** Full-snapshot data file holding this version's rows (GC-owned, e.g. {@code data/v3.retain.v2.json}). */
        public String snapshotFile = "";
        /** Original commit manifest of the version (kept for schema/history). */
        public String commitFile = "";

        public Entry() {
        }

        public Entry(long version, String snapshotFile, String commitFile) {
            this.version = version;
            this.snapshotFile = snapshotFile;
            this.commitFile = commitFile;
        }
    }

    /** A version that was explicitly expired: distinguishes "never existed" from "existed, now garbage". */
    public static final class ExpiredEntry {
        public long version;
        public long expiredAtMs;
        public long gcGeneration;

        public ExpiredEntry() {
        }

        public ExpiredEntry(long version, long expiredAtMs, long gcGeneration) {
            this.version = version;
            this.expiredAtMs = expiredAtMs;
            this.gcGeneration = gcGeneration;
        }
    }
}
