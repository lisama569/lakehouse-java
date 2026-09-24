package com.github.lisama569.lakehousejava.table;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One row: ordered column name to value (String/Long/Double/Boolean/null).
 */
public record Row(Map<String, Object> values) {

    public Row() {
        this(new LinkedHashMap<>());
    }

    public Object get(String column) {
        return values.get(column);
    }

    /** Build a row from alternating name/value arguments, e.g. {@code Row.of("id", 1L, "name", "a")}. */
    public static Row of(Object... nameValuePairs) {
        if (nameValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException("name/value pairs must be even in count");
        }
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < nameValuePairs.length; i += 2) {
            values.put((String) nameValuePairs[i], nameValuePairs[i + 1]);
        }
        return new Row(values);
    }
}
