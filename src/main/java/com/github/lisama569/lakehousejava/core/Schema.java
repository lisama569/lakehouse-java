package com.github.lisama569.lakehousejava.core;

import java.util.List;

/**
 * 表结构：主键列名 + 有序列定义列表。
 *
 * <p>结构本身随快照不可变；结构演进会产生带新 schemaId 的新结构，
 * 旧版本继续引用旧结构，保证时间旅行读取可被对应版本解释。
 */
public final class Schema {

    private final int schemaId;
    private final String primaryKey;
    private final List<ColumnDef> columns;

    public Schema(int schemaId, String primaryKey, List<ColumnDef> columns) {
        this.schemaId = schemaId;
        this.primaryKey = primaryKey;
        this.columns = List.copyOf(columns);
    }

    public int schemaId() {
        return schemaId;
    }

    public String primaryKey() {
        return primaryKey;
    }

    public List<ColumnDef> columns() {
        return columns;
    }

    public ColumnDef column(String name) {
        for (ColumnDef c : columns) {
            if (c.name().equals(name)) {
                return c;
            }
        }
        return null;
    }

    public boolean hasColumn(String name) {
        return column(name) != null;
    }

    /**
     * 基于本结构追加一列，产生新结构（新 schemaId）。调用方负责兼容性校验。
     */
    public Schema withAppendedColumn(int newSchemaId, ColumnDef column) {
        List<ColumnDef> merged = java.util.stream.Stream
                .concat(columns.stream(), java.util.stream.Stream.of(column))
                .toList();
        return new Schema(newSchemaId, primaryKey, merged);
    }
}
