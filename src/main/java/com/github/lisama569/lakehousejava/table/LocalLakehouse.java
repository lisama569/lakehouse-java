package com.github.lisama569.lakehousejava.table;

import com.github.lisama569.lakehousejava.core.ColumnDef;
import com.github.lisama569.lakehousejava.core.DataType;
import com.github.lisama569.lakehousejava.exception.TableException;
import com.github.lisama569.lakehousejava.storage.LocalFileIO;
import com.github.lisama569.lakehousejava.storage.TableMetadata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * 本地"湖"：以一个本地目录作为 warehouse，管理其中的表。全部能力离线可用，
 * 无外部服务、无真实凭据。
 *
 * <p>目录布局：
 * <pre>
 * warehouse/
 *   &lt;table&gt;/
 *     metadata.json                 表名、主键、currentVersion、全部历史 schema
 *     snapshots/000..001.json       每个成功提交一个全量快照文件（不可变、可寻址）
 *     compaction/*.json             整理记录（整理前后每版本校验和）
 * </pre>
 */
public class LocalLakehouse {

    private final Path warehouseDir;
    private final LocalFileIO fileIO;

    public LocalLakehouse(Path warehouseDir) {
        try {
            this.warehouseDir = warehouseDir.toAbsolutePath().normalize();
            Files.createDirectories(this.warehouseDir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot init warehouse at " + warehouseDir, e);
        }
        this.fileIO = new LocalFileIO();
    }

    public Path warehouseDir() {
        return warehouseDir;
    }

    public LocalFileIO fileIO() {
        return fileIO;
    }

    private Path tableDir(String tableName) {
        if (tableName == null || !tableName.matches("[A-Za-z0-9_-]+")) {
            throw new TableException("invalid table name: " + tableName);
        }
        return warehouseDir.resolve(tableName);
    }

    /**
     * 建表（幂等：已存在则返回已有表）。
     * 初始 schemaId=1、currentVersion=0，尚无快照文件（空表由"无版本"表示）。
     */
    public LocalTable createTableIfAbsent(String tableName,
                                          String primaryKey,
                                          List<ColumnDef> columns) {
        Path dir = tableDir(tableName);
        Path metaFile = dir.resolve("metadata.json");
        if (Files.exists(metaFile)) {
            return loadTable(tableName);
        }
        if (primaryKey == null || primaryKey.isBlank()) {
            throw new TableException("primary key must not be blank");
        }
        boolean pkFound = columns.stream().anyMatch(c -> c.name().equals(primaryKey));
        if (!pkFound) {
            throw new TableException("primary key column '" + primaryKey
                    + "' must be declared in columns");
        }
        for (ColumnDef c : columns) {
            if (c.defaultValue() != null && !c.type().accepts(c.defaultValue())) {
                throw new TableException("column '" + c.name()
                        + "' default value incompatible with type " + c.type());
            }
        }
        try {
            Files.createDirectories(dir.resolve("snapshots"));
            Files.createDirectories(dir.resolve("compaction"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        var canonical = columns.stream()
                .map(c -> c.defaultValue() == null ? c
                        : new ColumnDef(c.name(), c.type(), c.type().normalize(c.defaultValue())))
                .toList();
        TableMetadata meta = new TableMetadata(tableName, primaryKey, 0L, 2,
                List.of(new TableMetadata.SchemaEntry(1, canonical)));
        fileIO.writeJsonAtomically(metaFile, meta);
        return new LocalTableImpl(dir, fileIO, meta);
    }

    /** 打开已存在的表 */
    public LocalTable loadTable(String tableName) {
        Path metaFile = tableDir(tableName).resolve("metadata.json");
        if (!Files.exists(metaFile)) {
            throw new TableException("table not found: " + tableName);
        }
        TableMetadata meta = fileIO.readJson(metaFile, TableMetadata.class);
        return new LocalTableImpl(tableDir(tableName), fileIO, meta);
    }

    /** warehouse 下已有的表名 */
    public List<String> listTables() {
        try (var stream = Files.list(warehouseDir)) {
            return stream.filter(Files::isDirectory)
                    .filter(d -> Files.exists(d.resolve("metadata.json")))
                    .map(d -> d.getFileName().toString())
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 便捷构造列定义；新增列时请显式给出非 null 的 defaultValue */
    public static ColumnDef column(String name, DataType type, Object defaultValue) {
        return new ColumnDef(name, type, defaultValue);
    }
}
