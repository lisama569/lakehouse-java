package com.github.lisama569.lakehousejava.reject;

/**
 * 写入被拒绝的可区分原因码。
 */
public enum RejectCode {
    /** 某行缺失主键列或主键值为 null/空 */
    PRIMARY_KEY_MISSING,
    /** 某字段取值与列声明类型不匹配 */
    TYPE_MISMATCH,
    /** 某行出现了表结构中不存在的列 */
    UNKNOWN_COLUMN,
    /** 整列缺失：某非主键列在整个批次所有 UPSERT 行中均未出现 */
    MISSING_COLUMN,
    /** 批次为空 */
    EMPTY_BATCH
}
