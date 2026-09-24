package com.github.lisama569.lakehousejava.table;

/**
 * One rejected mutation: position within the batch, code and human-readable detail.
 */
public record RejectedRow(int opIndex, RejectReasonCode reason, String detail) {
}
