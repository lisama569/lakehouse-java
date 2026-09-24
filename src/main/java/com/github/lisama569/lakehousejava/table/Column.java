package com.github.lisama569.lakehousejava.table;

/**
 * A table column definition.
 */
public record Column(String name, DataType type, boolean nullable, Object defaultValue) {
}
