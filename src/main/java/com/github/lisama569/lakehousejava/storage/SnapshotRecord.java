package com.github.lisama569.lakehousejava.storage;

import com.github.lisama569.lakehousejava.core.Row;

import java.time.Instant;
import java.util.List;

/**
 * 单个版本快照文件（snapshots/NNNN.json）的持久化结构：
 * 该版本提交时刻的全量存活行（按主键有序），保证时间旅行读取结果稳定。
 *
 * @param version     版本号
 * @param parentVersion 父版本号（首个数据版本为 0）
 * @param schemaId    生效结构 id
 * @param operation   产生该版本的操作（WRITE/ADD_COLUMN 等）
 * @param createdAt   提交时刻
 * @param description 说明
 * @param rows        全量存活行
 */
public record SnapshotRecord(long version,
                             long parentVersion,
                             int schemaId,
                             String operation,
                             Instant createdAt,
                             String description,
                             List<Row> rows) {
}
