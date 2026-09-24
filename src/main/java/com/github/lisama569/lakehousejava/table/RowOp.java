package com.github.lisama569.lakehousejava.table;

/**
 * A single mutation: an UPSERT row or a DELETE identified by the row's key values.
 */
public record RowOp(RowOpType type, Row row) {

    public static RowOp upsert(Row row) {
        return new RowOp(RowOpType.UPSERT, row);
    }

    public static RowOp delete(Row keyRow) {
        return new RowOp(RowOpType.DELETE, keyRow);
    }
}
