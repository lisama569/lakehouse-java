package com.github.lisama569.lakehousejava.core;

/**
 * 写入或结构演进提交的结果。
 *
 * @param accepted         是否成功提交
 * @param version          提交产生的新版本号（被拒绝时为期望基准版本）
 * @param inputRowCount    输入批次的原始行数
 * @param effectiveRowCount 同批次主键去重后实际生效的行数
 * @param reason           判定依据 / 拒绝说明
 */
public record CommitResult(boolean accepted,
                           long version,
                           int inputRowCount,
                           int effectiveRowCount,
                           String reason) {
}
