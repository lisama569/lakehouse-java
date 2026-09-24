package com.github.lisama569.lakehousejava.tests;

import com.github.lisama569.lakehousejava.core.ColumnDef;
import com.github.lisama569.lakehousejava.core.DataType;
import com.github.lisama569.lakehousejava.core.Row;
import com.github.lisama569.lakehousejava.core.RowOp;
import com.github.lisama569.lakehousejava.core.WriteRequest;
import com.github.lisama569.lakehousejava.reject.BatchRejectedException;
import com.github.lisama569.lakehousejava.reject.RejectCode;
import com.github.lisama569.lakehousejava.table.LocalLakehouse;
import com.github.lisama569.lakehousejava.table.LocalTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景二：非法写入不被静默丢弃，拒绝原因可区分，且整批拒绝、无部分生效。
 */
class RejectionReasonTest {

    private static final Logger log = LoggerFactory.getLogger(RejectionReasonTest.class);

    private LocalTable table(Path dir) {
        return new LocalLakehouse(dir).createTableIfAbsent("t", "id", List.of(
                new ColumnDef("id", DataType.LONG, null),
                new ColumnDef("name", DataType.STRING, null),
                new ColumnDef("active", DataType.BOOLEAN, null)));
    }

    private BatchRejectedException reject(LocalTable t, long base,
                                          WriteRequest.RowOperation... ops) {
        try {
            t.write(new WriteRequest(base, List.of(ops)));
            throw new AssertionError("expected rejection");
        } catch (BatchRejectedException e) {
            return e;
        }
    }

    private WriteRequest.RowOperation up(Row r) {
        return new WriteRequest.RowOperation(RowOp.UPSERT, r);
    }

    @Test
    void emptyBatchRejected(@TempDir Path dir) {
        var e = reject(table(dir), 0L);
        log.info("空批次 -> 拒绝码={}", e.reasons().get(0).code());
        assertEquals(RejectCode.EMPTY_BATCH, e.reasons().get(0).code());
        assertEquals(0L, new LocalLakehouse(dir).loadTable("t").currentVersion());
    }

    @Test
    void missingPrimaryKeyRejected(@TempDir Path dir) {
        var e = reject(table(dir), 0L, up(Row.of("name", "x", "active", true)));
        log.info("缺主键行 -> 拒绝码={}, 行号={}, 列={}",
                e.reasons().get(0).code(), e.reasons().get(0).rowIndex(),
                e.reasons().get(0).column());
        assertEquals(RejectCode.PRIMARY_KEY_MISSING, e.reasons().get(0).code());
        assertEquals(0, e.reasons().get(0).rowIndex());
    }

    @Test
    void typeMismatchRejected(@TempDir Path dir) {
        var e = reject(table(dir), 0L,
                up(Row.of("id", 1L, "name", 123, "active", true)),
                up(Row.of("id", 2L, "name", "ok", "active", "yes")));
        log.info("类型不匹配 -> 全部原因={}", e.reasons());
        assertTrue(e.reasons().stream()
                .anyMatch(r -> r.code() == RejectCode.TYPE_MISMATCH && r.column().equals("name")));
        assertTrue(e.reasons().stream()
                .anyMatch(r -> r.code() == RejectCode.TYPE_MISMATCH && r.column().equals("active")));
        // 整批拒绝：无新版本产生
        assertEquals(0, new LocalLakehouse(dir).loadTable("t").currentVersion());
    }

    @Test
    void missingColumnAcrossBatchRejected(@TempDir Path dir) {
        // 所有 UPSERT 行都缺 active 列 => MISSING_COLUMN（不能静默当作 null 丢弃）
        var e = reject(table(dir), 0L,
                up(Row.of("id", 1L, "name", "a")),
                up(Row.of("id", 2L, "name", "b")));
        log.info("整列缺失 -> 全部原因={}", e.reasons());
        assertTrue(e.reasons().stream()
                .anyMatch(r -> r.code() == RejectCode.MISSING_COLUMN && r.column().equals("active")));
    }

    @Test
    void unknownColumnRejected(@TempDir Path dir) {
        var e = reject(table(dir), 0L,
                up(Row.of("id", 1L, "name", "a", "active", true, "ghost", 9)));
        log.info("未知列 -> 全部原因={}", e.reasons());
        assertTrue(e.reasons().stream()
                .anyMatch(r -> r.code() == RejectCode.UNKNOWN_COLUMN && r.column().equals("ghost")));
    }

    @Test
    void multipleProblemsReportedTogether(@TempDir Path dir) {
        // 一行缺主键 + 另一行类型不匹配 => 一次性报告，全部可区分
        var e = reject(table(dir), 0L,
                up(Row.of("name", "a", "active", true)),
                up(Row.of("id", 2L, "name", 9, "active", true)));
        log.info("混合问题 -> 原因数={}, 原因={}", e.reasons().size(), e.reasons());
        assertEquals(2, e.reasons().size());
        assertEquals(RejectCode.PRIMARY_KEY_MISSING, e.reasons().get(0).code());
        assertEquals(RejectCode.TYPE_MISMATCH, e.reasons().get(1).code());
    }
}
