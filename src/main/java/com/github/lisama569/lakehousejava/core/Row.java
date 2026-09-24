package com.github.lisama569.lakehousejava.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 一行数据：列名 -> 取值。使用 LinkedHashMap 保证列顺序稳定、可解释。
 */
public final class Row {

    private final Map<String, Object> values = new LinkedHashMap<>();

    public Row() {
    }

    @JsonCreator
    public Row(Map<String, Object> values) {
        if (values != null) {
            this.values.putAll(values);
        }
    }

    /** 便捷构造：Row.of("id", 1L, "name", "alice") */
    public static Row of(Object... kv) {
        if (kv.length % 2 != 0) {
            throw new IllegalArgumentException("Row.of expects key/value pairs");
        }
        Row row = new Row();
        for (int i = 0; i < kv.length; i += 2) {
            row.put((String) kv[i], kv[i + 1]);
        }
        return row;
    }

    public Object get(String column) {
        return values.get(column);
    }

    public void put(String column, Object value) {
        values.put(column, value);
    }

    /** 直接以 JSON 对象形式序列化 Row */
    @JsonValue
    public Map<String, Object> values() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Row other && values.equals(other.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
