package com.github.lisama569.lakehousejava.table;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema building, validation and value coercion helpers.
 */
public final class Schemas {

    private Schemas() {
    }

    public static final class Builder {
        private final List<Column> columns = new ArrayList<>();
        private final List<String> primaryKey = new ArrayList<>();

        public Builder column(String name, DataType type, boolean nullable, Object defaultValue) {
            columns.add(new Column(name, type, nullable, defaultValue));
            return this;
        }

        public Builder primaryKey(String name) {
            primaryKey.add(name);
            return this;
        }

        public Schema build() {
            Map<String, Column> map = new LinkedHashMap<>();
            for (Column c : columns) {
                if (c.name() == null || c.name().isBlank()) {
                    throw new TableException(RejectReasonCode.SCHEMA_COLUMN_DUPLICATE,
                            "column name must not be blank");
                }
                if (map.putIfAbsent(c.name(), c) != null) {
                    throw new TableException(RejectReasonCode.SCHEMA_COLUMN_DUPLICATE,
                            "duplicate column: " + c.name());
                }
                checkDefault(c);
            }
            if (primaryKey.isEmpty()) {
                throw new TableException(RejectReasonCode.MISSING_PRIMARY_KEY,
                        "table must declare at least one primary-key column");
            }
            for (String pk : primaryKey) {
                if (!map.containsKey(pk)) {
                    throw new TableException(RejectReasonCode.MISSING_PRIMARY_KEY,
                            "primary key column not defined: " + pk);
                }
            }
            return new Schema(List.copyOf(new ArrayList<>(map.keySet())), Map.copyOf(map), List.copyOf(primaryKey));
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Validate a column's declared default value against its type and nullability. */
    public static void checkDefault(Column column) {
        Object dv = column.defaultValue();
        if (dv == null) {
            return;
        }
        if (!isAssignable(dv, column.type())) {
            throw new TableException(RejectReasonCode.SCHEMA_EVOLUTION_INCOMPATIBLE,
                    "default value " + describe(dv) + " is not assignable to " + column.type()
                            + " column '" + column.name() + "'");
        }
    }

    private static String describe(Object v) {
        return "'" + v + "' (" + v.getClass().getSimpleName() + ")";
    }

    /**
     * Coerce a raw incoming value to the column type.
     *
     * @return coerced value, or null when the raw value is null/absent
     * @throws TableException with {@link RejectReasonCode#TYPE_MISMATCH} on incompatible input
     */
    public static Object coerce(Object raw, DataType type) {
        if (raw == null) {
            return null;
        }
        switch (type) {
            case STRING -> {
                return raw.toString();
            }
            case LONG -> {
                if (raw instanceof Long l) {
                    return l;
                }
                if (raw instanceof Integer i) {
                    return i.longValue();
                }
                if (raw instanceof Number n && n.doubleValue() == Math.rint(n.doubleValue())
                        && n.doubleValue() >= Long.MIN_VALUE && n.doubleValue() <= Long.MAX_VALUE) {
                    return n.longValue();
                }
                if (raw instanceof String s) {
                    try {
                        return Long.parseLong(s.trim());
                    } catch (NumberFormatException e) {
                        throw typeMismatch(raw, type);
                    }
                }
                throw typeMismatch(raw, type);
            }
            case DOUBLE -> {
                if (raw instanceof Double d) {
                    return d;
                }
                if (raw instanceof Number n) {
                    return n.doubleValue();
                }
                if (raw instanceof String s) {
                    try {
                        return Double.parseDouble(s.trim());
                    } catch (NumberFormatException e) {
                        throw typeMismatch(raw, type);
                    }
                }
                throw typeMismatch(raw, type);
            }
            case BOOLEAN -> {
                if (raw instanceof Boolean b) {
                    return b;
                }
                if (raw instanceof String s && (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false"))) {
                    return Boolean.parseBoolean(s.trim());
                }
                throw typeMismatch(raw, type);
            }
            default -> throw new IllegalStateException("unknown type " + type);
        }
    }

    private static boolean isAssignable(Object value, DataType type) {
        if (value == null) {
            return true;
        }
        return switch (type) {
            case STRING -> value instanceof String;
            case LONG -> value instanceof Long || value instanceof Integer;
            case DOUBLE -> value instanceof Number;
            case BOOLEAN -> value instanceof Boolean;
        };
    }

    private static TableException typeMismatch(Object raw, DataType type) {
        return new TableException(RejectReasonCode.TYPE_MISMATCH,
                "value " + describe(raw) + " is not assignable to " + type);
    }
}
