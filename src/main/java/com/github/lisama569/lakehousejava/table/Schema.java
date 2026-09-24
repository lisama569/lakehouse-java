package com.github.lisama569.lakehousejava.table;

import java.util.List;
import java.util.Map;

/**
 * Immutable table schema: ordered columns plus the primary key column names.
 */
public record Schema(List<String> columnNames, Map<String, Column> columns, List<String> primaryKey) {

    public Column column(String name) {
        return columns.get(name);
    }
}
