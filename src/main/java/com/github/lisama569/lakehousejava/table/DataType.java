package com.github.lisama569.lakehousejava.table;

/**
 * Supported column data types.
 */
public enum DataType {
    STRING,
    LONG,
    DOUBLE,
    BOOLEAN;

    /**
     * Whether this type is a widening of the old type (schema evolution).
     * LONG -&gt; DOUBLE is the only supported widening.
     */
    public boolean isWideningOf(DataType old) {
        return this == old || (this == DOUBLE && old == LONG);
    }
}
