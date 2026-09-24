package com.github.lisama569.lakehousejava.core;

import java.time.Instant;

/**
 * 一次成功提交产生的快照（可寻址版本）元信息。
 *
 * @param version      单调递增版本号，从 1 开始
 * @param schemaId     该版本生效的结构 id
 * @param rowCount     该版本快照中的存活行数
 * @param createdAt    提交时间戳
 * @param operation    产生该版本的操作类型
 * @param description  可读说明（批次行数、判定依据等）
 */
public record SnapshotInfo(long version,
                           int schemaId,
                           long rowCount,
                           Instant createdAt,
                           String operation,
                           String description) {
}
