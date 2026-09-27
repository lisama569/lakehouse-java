package com.github.lisama569.lakehousejava.table;

import java.util.List;

/**
 * Outcome of an explicit vacuum (historical-version garbage collection).
 *
 * <p>A vacuum is a no-op when no file is eligible for deletion; retried vacuums
 * after an interruption converge to the same deterministic conclusion.</p>
 *
 * @param baseVersion        current-version premise the vacuum was computed against
 * @param deletedFiles       data files physically deleted in this run (empty on a no-op)
 * @param bytesReclaimed     total bytes freed by {@link #deletedFiles()}
 * @param retainedVersions   versions still readable after this run (latest + pinned + recent window)
 * @param expiredVersions    versions unreadable after this run because their files were reclaimed
 * @param noOp               {@code true} when nothing was eligible for deletion
 */
public record VacuumResult(long baseVersion,
                           List<String> deletedFiles,
                           long bytesReclaimed,
                           List<Long> retainedVersions,
                           List<Long> expiredVersions,
                           boolean noOp) {
}
