package com.github.lisama569.lakehousejava.reject;

/**
 * 单条可区分的拒绝原因。
 *
 * @param code   原因码
 * @param rowIndex 出错行在输入批次中的下标（整列缺失时为 -1）
 * @param column 涉及列名（可空）
 * @param detail 人类可读细节
 */
public record RejectReason(RejectCode code, int rowIndex, String column, String detail) {

    public static RejectReason of(RejectCode code, int rowIndex, String column, String detail) {
        return new RejectReason(code, rowIndex, column, detail);
    }
}
