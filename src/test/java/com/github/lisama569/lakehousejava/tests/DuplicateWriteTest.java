package com.github.lisama569.lakehousejava.tests;

import com.github.lisama569.lakehousejava.core.ColumnDef;
import com.github.lisama569.lakehousejava.core.CommitResult;
import com.github.lisama569.lakehousejava.core.DataType;
import com.github.lisama569.lakehousejava.core.Row;
import com.github.lisama569.lakehousejava.core.RowOp;
import com.github.lisama569.lakehousejava.core.WriteRequest;
import com.github.lisama569.lakehousejava.table.LocalLakehouse;
import com.github.lisama569.lakehousejava.table.LocalTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景一：同批次重复主键按确定性规则收敛（last-op-wins；DELETE 居末则删除生效）。
 * 同输入重复执行任意次，最终状态唯一确定。
 */
class DuplicateWriteTest {

    private static final Logger log = LoggerFactory.getLogger(DuplicateWriteTest.class);

    private LocalTable newTable(Path dir) {
        return new LocalLakehouse(dir).createTableIfAbsent("orders", "id", List.of(
                new ColumnDef("id", DataType.LONG, null),
                new ColumnDef("status", DataType.STRING, null)));
    }

    private WriteRequest.RowOperation upsert(long id, String status) {
        return new WriteRequest.RowOperation(RowOp.UPSERT, Row.of("id", id, "status", status));
    }

    private WriteRequest.RowOperation delete(long id) {
        return new WriteRequest.RowOperation(RowOp.DELETE, Row.of("id", id));
    }

    @Test
    void duplicateUpsertConvergesToLastRowAndIsDeterministic(@TempDir Path dir) {
        LocalTable table = newTable(dir);

        // 同一主键 id=1 出现三次，最后一条 status=closed 必须生效
        List<WriteRequest.RowOperation> batch = List.of(
                upsert(1L, "open"),
                upsert(2L, "open"),
                upsert(1L, "paid"),
                upsert(1L, "closed"));

        CommitResult result = table.write(new WriteRequest(0L, batch));
        log.info("批次={}, 判定依据={}",
                "[1:open, 2:open, 1:paid, 1:closed]", result.reason());
        assertEquals(1L, result.version());
        assertEquals(4, result.inputRowCount());
        assertEquals(2, result.effectiveRowCount());

        var rows = table.readAt(1);
        assertEquals(2, rows.size());
        assertEquals("closed", rows.get(0).get("status")); // 主键有序，id=1 在首位

        // 在同表基线 v1 上用同样内容重放（基于最新版本），最终状态不变 => 唯一确定
        CommitResult replay = table.write(new WriteRequest(1L, batch));
        assertEquals(2L, replay.version());
        assertEquals(2, table.readLatest().size());
        assertEquals("closed", table.readLatest().get(0).get("status"));
        log.info("重复执行后版本={}, 存活行数=2, id=1 状态仍为 closed（唯一确定）", replay.version());
    }

    @Test
    void deleteTrailingWinsOverUpsert(@TempDir Path dir) {
        LocalTable table = newTable(dir);
        table.write(new WriteRequest(0L, List.of(upsert(1L, "open"))));

        // 同主键：UPSERT 在前、DELETE 在最后 => 最终删除
        var batch = List.of(upsert(1L, "paid"), delete(1L), upsert(1L, "reopened"));
        var r1 = table.write(new WriteRequest(1L, batch));
        log.info("批次={}, 版本=v{}, 判定依据={}", "[1:paid, DELETE 1, 1:reopened]",
                r1.version(), r1.reason());
        assertEquals("reopened", table.readLatest().get(0).get("status"));

        // 反过来：DELETE 在最后 => 最终不存在
        var batch2 = List.of(upsert(1L, "paid"), upsert(1L, "x"), delete(1L));
        var r2 = table.write(new WriteRequest(2L, batch2));
        log.info("批次={}, 版本=v{}, 判定依据={}", "[1:paid, 1:x, DELETE 1]",
                r2.version(), r2.reason());
        // v3 已删除 => 最新为空；但历史 v2 时间旅行仍可读到 reopened
        assertTrue(table.readLatest().isEmpty());
        assertEquals("reopened", table.readAt(2).get(0).get("status"));
    }
}
