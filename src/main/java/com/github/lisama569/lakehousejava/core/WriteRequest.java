package com.github.lisama569.lakehousejava.core;

import java.util.List;

/**
 * 一次写入请求：基准版本 + 有序操作列表。
 *
 * <p>同批次内重复主键按确定性规则收敛：列表中靠后的同主键操作生效；
 * 若同一主键同时存在 UPSERT 与 DELETE，则 DELETE 最终生效。
 *
 * @param expectedBaseVersion 客户端读取所基于的版本号；与当前版本不一致即拒绝
 * @param operations          有序操作，元素为 {@link RowOperation}
 */
public record WriteRequest(long expectedBaseVersion, List<RowOperation> operations) {

    /**
     * 单条操作：种类 + 行数据。
     */
    public record RowOperation(RowOp op, Row row) {
    }
}
