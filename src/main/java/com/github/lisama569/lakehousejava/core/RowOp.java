package com.github.lisama569.lakehousejava.core;

/**
 * 单条写入操作的种类。
 */
public enum RowOp {
    /** 插入或更新（按主键整行覆盖） */
    UPSERT,
    /** 按主键删除 */
    DELETE
}
