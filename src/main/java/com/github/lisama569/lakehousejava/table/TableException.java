package com.github.lisama569.lakehousejava.table;

/**
 * Error with a distinguishable {@link RejectReasonCode}.
 */
public class TableException extends RuntimeException {

    private final RejectReasonCode code;

    public TableException(RejectReasonCode code, String message) {
        super(message);
        this.code = code;
    }

    public RejectReasonCode code() {
        return code;
    }
}
