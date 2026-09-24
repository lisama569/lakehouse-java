package com.github.lisama569.lakehousejava.reject;

import java.util.List;

/**
 * 写入批次校验失败：整个批次被拒绝，不会产生任何部分生效。
 */
public class BatchRejectedException extends RuntimeException {

    private final transient List<RejectReason> reasons;

    public BatchRejectedException(List<RejectReason> reasons) {
        super("batch rejected with " + reasons.size() + " reason(s): " + reasons);
        this.reasons = List.copyOf(reasons);
    }

    public List<RejectReason> reasons() {
        return reasons;
    }
}
