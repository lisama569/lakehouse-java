package com.github.lisama569.lakehousejava.table;

import java.util.List;

/**
 * Result of attempting a write batch.
 */
public record WriteResult(boolean committed, Long newVersion, int upserted, int deleted,
                          int duplicateKeysCollapsed, List<RejectedRow> rejections) {

    public static WriteResult rejected(List<RejectedRow> rejections) {
        return new WriteResult(false, null, 0, 0, 0, List.copyOf(rejections));
    }
}
