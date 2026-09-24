package com.github.lisama569.lakehousejava.table;

import com.github.lisama569.lakehousejava.core.ColumnDef;
import com.github.lisama569.lakehousejava.core.Row;
import com.github.lisama569.lakehousejava.core.RowOp;
import com.github.lisama569.lakehousejava.core.Schema;
import com.github.lisama569.lakehousejava.core.WriteRequest;
import com.github.lisama569.lakehousejava.reject.RejectCode;
import com.github.lisama569.lakehousejava.reject.RejectReason;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 写入批次校验：全部规则在提交前一次性检查，任一行失败则整批拒绝（无部分生效）。
 *
 * <p>拒绝原因可区分：
 * <ul>
 *   <li>{@link RejectCode#EMPTY_BATCH} 批次为空；</li>
 *   <li>{@link RejectCode#PRIMARY_KEY_MISSING} 行缺主键列或值为空；</li>
 *   <li>{@link RejectCode#TYPE_MISMATCH} 字段取值与列类型不匹配；</li>
 *   <li>{@link RejectCode#UNKNOWN_COLUMN} 行出现表结构中不存在的列；</li>
 *   <li>{@link RejectCode#MISSING_COLUMN} 某非主键列在整批所有 UPSERT 行中均缺失。</li>
 * </ul>
 */
final class WriteValidator {

    private WriteValidator() {
    }

    /**
     * 校验整个请求，返回所有可解释的拒绝原因；空列表表示通过。
     */
    static List<RejectReason> validate(WriteRequest request, Schema schema) {
        List<RejectReason> reasons = new ArrayList<>();
        List<WriteRequest.RowOperation> ops = request.operations();
        if (ops == null || ops.isEmpty()) {
            reasons.add(RejectReason.of(RejectCode.EMPTY_BATCH, -1, null,
                    "write batch is empty"));
            return reasons;
        }

        String pk = schema.primaryKey();
        int upsertCount = 0;
        Set<String> columnsPresentInAnyUpsert = new HashSet<>();

        for (int i = 0; i < ops.size(); i++) {
            WriteRequest.RowOperation operation = ops.get(i);
            Row row = operation.row();
            if (operation.op() == RowOp.UPSERT) {
                upsertCount++;
            }

            Object pkValue = row == null ? null : row.get(pk);
            if (pkValue == null || (pkValue instanceof String s && s.isEmpty())) {
                reasons.add(RejectReason.of(RejectCode.PRIMARY_KEY_MISSING, i, pk,
                        "primary key column '" + pk + "' is missing or null"));
                // 主键缺失时后续逐列检查仍继续，以便一次性报告全部问题
            }

            if (row != null) {
                for (var entry : row.values().entrySet()) {
                    String name = entry.getKey();
                    Object value = entry.getValue();
                    ColumnDef def = schema.column(name);
                    if (def == null) {
                        reasons.add(RejectReason.of(RejectCode.UNKNOWN_COLUMN, i, name,
                                "column '" + name + "' does not exist in schema v"
                                        + schema.schemaId()));
                        continue;
                    }
                    if (operation.op() == RowOp.UPSERT) {
                        columnsPresentInAnyUpsert.add(name);
                    }
                    if (value != null && !def.type().accepts(value)) {
                        reasons.add(RejectReason.of(RejectCode.TYPE_MISMATCH, i, name,
                                "value " + describe(value) + " is not assignable to "
                                        + def.type() + " for column '" + name + "'"));
                    }
                }
            }
        }

        // 整列缺失：仅当批次中确实存在 UPSERT 行时才检查
        // （纯删除批次只需要主键）。主键列已在上面逐行检查。
        if (upsertCount > 0) {
            for (ColumnDef def : schema.columns()) {
                if (def.name().equals(pk)) {
                    continue;
                }
                if (!columnsPresentInAnyUpsert.contains(def.name())) {
                    reasons.add(RejectReason.of(RejectCode.MISSING_COLUMN, -1, def.name(),
                            "column '" + def.name() + "' is absent in every UPSERT row "
                                    + "of the batch (schema v" + schema.schemaId() + ")"));
                }
            }
        }

        return reasons;
    }

    private static String describe(Object value) {
        return "<" + value.getClass().getSimpleName() + "> " + value;
    }
}
