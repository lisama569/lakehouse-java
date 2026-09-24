package com.github.lisama569.lakehousejava.table;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * On-disk per-version manifest: {@code commits/<version>.commit.json}.
 *
 * <p>A full snapshot ({@code fullSnapshot == true}) lists every live row file and
 * delete marker at that version. Otherwise it carries only the delta data/delete
 * files produced by this commit; readers resolve a version by walking backwards
 * to the nearest full snapshot and replaying deltas in order.</p>
 */
public final class CommitRecord {

    public long version;
    public long parentVersion;
    public String kind;
    public long timestampMs;
    public String description = "";
    public boolean fullSnapshot;
    public List<String> dataFiles = new ArrayList<>();
    public List<String> deleteFiles = new ArrayList<>();
    public List<StoredSchemaColumn> schemaSnapshot = new ArrayList<>();
    public List<String> primaryKey = new ArrayList<>();

    public CommitRecord() {
    }

    public static final class StoredSchemaColumn {
        public String name;
        public String type;
        public boolean nullable;
        public Object defaultValue;

        public StoredSchemaColumn() {
        }

        public StoredSchemaColumn(Column column) {
            this.name = column.name();
            this.type = column.type().name();
            this.nullable = column.nullable();
            this.defaultValue = column.defaultValue();
        }

        Column toColumn() {
            return new Column(name, DataType.valueOf(type), nullable, defaultValue);
        }
    }

    public CommitRecord(long version, long parentVersion, CommitKind kind, long timestampMs,
                        String description, boolean fullSnapshot, Schema schema) {
        this.version = version;
        this.parentVersion = parentVersion;
        this.kind = kind.name();
        this.timestampMs = timestampMs;
        this.description = description == null ? "" : description;
        this.fullSnapshot = fullSnapshot;
        for (String columnName : schema.columnNames()) {
            this.schemaSnapshot.add(new StoredSchemaColumn(schema.column(columnName)));
        }
        this.primaryKey = new ArrayList<>(schema.primaryKey());
    }

    public CommitKind commitKind() {
        return CommitKind.valueOf(kind);
    }

    Schema toSchema() {
        List<String> names = new ArrayList<>();
        var map = new LinkedHashMap<String, Column>();
        for (StoredSchemaColumn sc : schemaSnapshot) {
            Column c = sc.toColumn();
            names.add(c.name());
            map.put(c.name(), c);
        }
        return new Schema(List.copyOf(names), Map.copyOf(map), List.copyOf(primaryKey));
    }
}
