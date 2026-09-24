package com.github.lisama569.lakehousejava.core;

/**
 * 列（字段）定义。
 *
 * @param name         列名
 * @param type         逻辑类型
 * @param defaultValue 结构演进新增该列时，对历史行生效的缺省值（String/Long/Double/Boolean 或 null）
 */
public record ColumnDef(String name, DataType type, Object defaultValue) {
}
