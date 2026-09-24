package com.github.lisama569.lakehousejava.storage;

import com.github.lisama569.lakehousejava.core.ColumnDef;

import java.util.List;

/**
 * 表元数据（metadata.json）的持久化结构。
 *
 * @param tableName          表名
 * @param primaryKey         主键列名
 * @param currentVersion     当前最新版本号
 * @param nextSchemaId       下一个可用 schema id
 * @param schemas           全部历史结构（id -> 定义），按 id 升序
 */
public record TableMetadata(String tableName,
                            String primaryKey,
                            long currentVersion,
                            int nextSchemaId,
                            List<SchemaEntry> schemas) {

    /**
     * 一个历史结构版本。
     */
    public record SchemaEntry(int schemaId, List<ColumnDef> columns) {
    }
}
