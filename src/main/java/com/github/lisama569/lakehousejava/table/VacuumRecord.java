package com.github.lisama569.lakehousejava.table;

import java.util.ArrayList;
import java.util.List;

/**
 * One durable vacuum attempt, appended to {@code vacuums/<seq>.vacuum.json}.
 *
 * <p>The ledger only records history; it never decides what is safe to delete.
 * It lets later runs (and readers) explain why a version stopped being readable.</p>
 */
public final class VacuumRecord {

    public long sequence;
    public long baseVersion;
    public long timestampMs;
    public boolean completed;
    public List<String> deletedFiles = new ArrayList<>();
    public long bytesReclaimed;
    public List<Long> retainedVersions = new ArrayList<>();
    public List<Long> expiredVersions = new ArrayList<>();

    public VacuumRecord() {
    }

    public VacuumRecord(long sequence, long baseVersion, long timestampMs) {
        this.sequence = sequence;
        this.baseVersion = baseVersion;
        this.timestampMs = timestampMs;
    }
}
