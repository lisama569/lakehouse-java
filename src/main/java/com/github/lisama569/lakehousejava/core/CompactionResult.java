package com.github.lisama569.lakehousejava.core;

import java.util.Map;

/**
 * 存储整理结果。整理不产生新版本、不改写任何历史结论。
 *
 * @param compacted       是否执行了整理
 * @param versionsRewritten 被规范化重写的版本文件数
 * @param beforeChecksums 整理前 版本号 -> 内容校验和
 * @param afterChecksums  整理后 版本号 -> 内容校验和
 * @param reason          判定依据
 */
public record CompactionResult(boolean compacted,
                               int versionsRewritten,
                               Map<Long, String> beforeChecksums,
                               Map<Long, String> afterChecksums,
                               String reason) {
}
