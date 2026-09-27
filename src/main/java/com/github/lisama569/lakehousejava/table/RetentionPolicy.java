package com.github.lisama569.lakehousejava.table;

import java.util.List;

/**
 * Historical-version retention policy of a table.
 *
 * <p>A version is <b>retained</b> (kept readable) by a vacuum when it is any of:</p>
 * <ul>
 *   <li>the current (latest) version &mdash; always retained implicitly;</li>
 *   <li>one of the explicitly {@link #pinnedVersions()} (long-term pinning);</li>
 *   <li>among the {@link #minVersionsToKeep()} most recent versions.</li>
 * </ul>
 *
 * <p>The policy only takes effect when a vacuum is triggered explicitly; ordinary
 * writes, schema evolution and compaction never delete anything on their own.</p>
 *
 * @param minVersionsToKeep keep at least this many of the most recent versions (&ge; 0)
 * @param pinnedVersions    version numbers pinned for long-term retention (never expire)
 */
public record RetentionPolicy(int minVersionsToKeep, List<Long> pinnedVersions) {

    public RetentionPolicy {
        if (minVersionsToKeep < 0) {
            throw new IllegalArgumentException("minVersionsToKeep must be >= 0 but was " + minVersionsToKeep);
        }
        pinnedVersions = pinnedVersions == null ? List.of() : List.copyOf(pinnedVersions);
    }

    /** Keep only the {@code n} most recent versions (plus the implicit latest). */
    public static RetentionPolicy keepRecent(int n) {
        return new RetentionPolicy(n, List.of());
    }

    /** Pin specific versions long-term, keeping at least {@code n} recent versions too. */
    public static RetentionPolicy of(int n, long... pinned) {
        Long[] boxed = new Long[pinned.length];
        for (int i = 0; i < pinned.length; i++) {
            boxed[i] = pinned[i];
        }
        return new RetentionPolicy(n, List.of(boxed));
    }
}
