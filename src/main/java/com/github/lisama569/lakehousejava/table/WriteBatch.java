package com.github.lisama569.lakehousejava.table;

import java.util.List;

/**
 * A batch of mutations based on a (possibly null = latest) base version.
 */
public record WriteBatch(List<RowOp> ops, Long expectedBaseVersion) {

    public WriteBatch(List<RowOp> ops) {
        this(ops, null);
    }
}
