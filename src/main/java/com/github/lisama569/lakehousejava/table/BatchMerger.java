package com.github.lisama569.lakehousejava.table;

import com.github.lisama569.lakehousejava.core.ColumnDef;
import com.github.lisama569.lakehousejava.core.DataType;
import com.github.lisama569.lakehousejava.core.Row;
import com.github.lisama569.lakehousejava.core.RowOp;
import com.github.lisama569.lakehousejava.core.Schema;
import com.github.lisama569.lakehousejava.core.WriteRequest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 把一个批次确定性地收敛并应用到基线上。
 *
 * <p>确定性去重规则（与输入顺序相关、可复现）：
 * <ol>
 *   <li>按输入顺序遍历，同一主键只保留最后一条操作（last-op-wins）；</li>
 *   <li>因此同一主键同时存在 UPSERT 与 DELETE 时，谁在列表最后谁生效，
 *       即 DELETE 排在最后则最终删除，UPSERT 排在最后则最终写入；</li>
 *   <li>收敛后再统一应用到基线，得到全量存活行。</li>
 * </ol>
 */
final class BatchMerger {

    private BatchMerger() {
    }

    /**
     * 收敛结果。
     *
     * @param effective 主键 -> 最终操作
     * @param duplicates 因主键重复被收敛掉的输入行数
     */
    record Collapsed(Map<String, WriteRequest.RowOperation> effective, int duplicates) {
    }

    /** 按确定性规则把批次收敛为 主键 -> 最后一条操作。 */
    static Collapsed collapse(WriteRequest request, Schema schema) {
        // LinkedHashMap 让"首次出现顺序"稳定，应用阶段再统一按主键排序
        Map<String, WriteRequest.RowOperation> byKey = new LinkedHashMap<>();
        int duplicates = 0;
        String pk = schema.primaryKey();
        for (WriteRequest.RowOperation op : request.operations()) {
            String key = String.valueOf(op.row().get(pk));
            if (byKey.containsKey(key)) {
                duplicates++;
            }
            byKey.put(key, op);
        }
        return new Collapsed(byKey, duplicates);
    }

    /**
     * 将收敛后的操作应用到基线（主键有序的全量行），返回新的全量存活行。
     * 调用前批次必须已通过校验。
     */
    static List<Row> apply(List<Row> baseline,
                           Map<String, WriteRequest.RowOperation> collapsed,
                           Schema schema) {
        Map<String, Row> table = new LinkedHashMap<>();
        String pk = schema.primaryKey();
        for (Row row : baseline) {
            table.put(String.valueOf(row.get(pk)), copyRow(row));
        }

        // 按主键排序应用，保证同输入、同基线下结果字节级可复现
        List<String> keys = new ArrayList<>(collapsed.keySet());
        keys.sort(String::compareTo);
        for (String key : keys) {
            WriteRequest.RowOperation operation = collapsed.get(key);
            if (operation.op() == RowOp.DELETE) {
                table.remove(key);
            } else {
                table.put(key, toCanonicalRow(operation.row(), schema));
            }
        }

        List<Row> out = new ArrayList<>(table.values());
        out.sort((a, b) -> String.valueOf(a.get(pk)).compareTo(String.valueOf(b.get(pk))));
        return out;
    }

    /** 将输入行按结构列顺序规范化，类型归一化，并补齐缺省值（整列缺失已在校验阶段拒绝）。 */
    private static Row toCanonicalRow(Row input, Schema schema) {
        Row out = new Row();
        for (ColumnDef def : schema.columns()) {
            Object value = input.get(def.name());
            if (value == null) {
                value = def.defaultValue();
            }
            if (value != null) {
                DataType type = def.type();
                if (type.accepts(value)) {
                    value = type.normalize(value);
                }
            }
            out.put(def.name(), value);
        }
        return out;
    }

    private static Row copyRow(Row row) {
        Row copy = new Row();
        copy.values().putAll(row.values());
        return copy;
    }
}
