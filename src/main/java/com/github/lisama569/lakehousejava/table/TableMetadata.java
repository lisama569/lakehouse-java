package com.github.lisama569.lakehousejava.table;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * On-disk table metadata ({@code _lakehouse.json}), updated atomically after every commit.
 */
public final class TableMetadata {

    public String name;
    public long currentVersion;
    public List<StoredColumn> schema = new ArrayList<>();
    public List<String> primaryKey = new ArrayList<>();
    /** Null = no count-based retention cap. */
    public Integer retainLatestVersions;
    /** Explicitly pinned versions; null/absent is treated as an empty list. */
    public List<Long> pinnedVersions = new ArrayList<>();

    public static final class StoredColumn {
        public String name;
        public String type;
        public boolean nullable;
        public Object defaultValue;

        public StoredColumn() {
        }

        public StoredColumn(Column column) {
            this.name = column.name();
            this.type = column.type().name();
            this.nullable = column.nullable();
            this.defaultValue = column.defaultValue();
        }

        Column toColumn() {
            return new Column(name, DataType.valueOf(type), nullable, defaultValue);
        }
    }

    public TableMetadata() {
    }

    public TableMetadata(String name, Schema schema, long currentVersion) {
        this.name = name;
        this.currentVersion = currentVersion;
        for (String columnName : schema.columnNames()) {
            this.schema.add(new StoredColumn(schema.column(columnName)));
        }
        this.primaryKey = new ArrayList<>(schema.primaryKey());
    }

    Schema toSchema() {
        List<String> names = new ArrayList<>();
        var map = new LinkedHashMap<String, Column>();
        for (StoredColumn sc : schema) {
            Column c = sc.toColumn();
            names.add(c.name());
            map.put(c.name(), c);
        }
        return new Schema(List.copyOf(names), Map.copyOf(map), List.copyOf(primaryKey));
    }
}
