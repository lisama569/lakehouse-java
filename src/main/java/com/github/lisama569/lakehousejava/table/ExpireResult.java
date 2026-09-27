package com.github.lisama569.lakehousejava.table;

import java.util.List;

/**
 * Result of one explicit snapshot-expiration (garbage collection) run.
 *
 * <p>A run is based on a fixed current version ({@code baseVersion}). If the
 * table advanced while the run was prepared, the whole run is rejected with
 * {@link RejectReasonCode#RETENTION_CONFLICT} and nothing is deleted.</p>
 *
 * @param committed         whether a new retention state was published
 * @param baseVersion       current version the run was based on
 * @param retainedVersions  versions still readable after the run
 * @param expiredVersions   versions that became unreadable by this run
 * @param previouslyExpired versions already unreadable before this run
 * @param filesDeleted      data/manifest files actually deleted
 * @param bytesReclaimed    bytes freed by the deleted files (best-effort sum of file sizes)
 */
public record ExpireResult(boolean committed,
                           long baseVersion,
                           List<Long> retainedVersions,
                           List<Long> expiredVersions,
                           List<Long> previouslyExpired,
                           List<String> filesDeleted,
                           long bytesReclaimed) {

    static ExpireResult noop(long baseVersion, List<Long> retained, List<Long> expired) {
        return new ExpireResult(false, baseVersion, retained, List.of(), expired, List.of(), 0L);
    }
}
