package com.github.lisama569.lakehousejava.table;

import java.util.List;

/**
 * History retention policy of a table.
 *
 * <p>A version is retained when it is among the {@code retainLatestVersions}
 * most recent versions <em>or</em> it is explicitly pinned in
 * {@code pinnedVersions}. The current/latest version is always retained.
 * A {@code null} {@code retainLatestVersions} means "no count-based cap":
 * without an explicit {@link LocalTableManager#expireSnapshots(Long)} run the
 * default behaviour stays "keep everything".</p>
 *
 * @param retainLatestVersions keep at least this many most recent versions; {@code null} = unlimited
 * @param pinnedVersions versions explicitly pinned for long-term retention
 */
public record RetentionPolicy(Integer retainLatestVersions, List<Long> pinnedVersions) {

    public RetentionPolicy {
        if (retainLatestVersions != null && retainLatestVersions < 1) {
            throw new TableException(RejectReasonCode.RETENTION_POLICY_INVALID,
                    "retainLatestVersions must be >= 1 but was " + retainLatestVersions);
        }
        if (pinnedVersions == null) {
            pinnedVersions = List.of();
        }
        for (Long v : pinnedVersions) {
            if (v == null) {
                throw new TableException(RejectReasonCode.RETENTION_POLICY_INVALID,
                        "pinned versions must not contain null");
            }
        }
        pinnedVersions = pinnedVersions.stream().distinct().sorted().toList();
    }

    /** Keep only the {@code n} most recent versions. */
    public static RetentionPolicy retainLatest(int n) {
        return new RetentionPolicy(n, List.of());
    }

    /** Keep the {@code n} most recent versions and additionally pin the given versions. */
    public static RetentionPolicy of(int n, List<Long> pinned) {
        return new RetentionPolicy(n, pinned);
    }

    /** No retention cap: every version stays readable until a policy is set and GC is run. */
    public static RetentionPolicy keepAll() {
        return new RetentionPolicy(null, List.of());
    }

    /** Pin specific versions for long-term retention without a count-based cap. */
    public static RetentionPolicy pin(List<Long> pinned) {
        return new RetentionPolicy(null, pinned);
    }
}
