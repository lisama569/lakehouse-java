package com.github.lisama569.lakehousejava.storage;

import java.time.Instant;
import java.util.Map;

/**
 * 整理记录（compaction/*.json）。整理不产生新版本，仅记录整理前后每个版本的校验和。
 *
 * @param id              整理记录 id
 * @param ranAt           整理时间
 * @param versionsRewritten 重写的版本文件数
 * @param beforeChecksums 整理前 版本号 -> 校验和
 * @param afterChecksums  整理后 版本号 -> 校验和
 */
public record CompactionLog(String id,
                            Instant ranAt,
                            int versionsRewritten,
                            Map<Long, String> beforeChecksums,
                            Map<Long, String> afterChecksums) {
}
