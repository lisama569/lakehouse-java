package com.github.lisama569.lakehousejava.tests;

import com.github.lisama569.lakehousejava.core.ColumnDef;
import com.github.lisama569.lakehousejava.core.CompactionResult;
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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * 场景六：存储整理只规范化文件布局，不产生新版本、不改变任何版本查询结论；
 * 整理可重复执行，重复整理后校验和稳定（幂等）。
 */
class CompactionTest {

    private static final Logger log = LoggerFactory.getLogger(CompactionTest.class);

    @Test
    void compactionPreservesEveryVersionAndIsIdempotent(@TempDir Path dir) {
        var lh = new LocalLakehouse(dir);
        LocalTable t = lh.createTableIfAbsent("k", "id", List.of(
                new ColumnDef("id", DataType.LONG, null),
                new ColumnDef("name", DataType.STRING, null)));
        t.write(new WriteRequest(0L, List.of(
                new WriteRequest.RowOperation(RowOp.UPSERT, Row.of("id", 1L, "name", "a")),
                new WriteRequest.RowOperation(RowOp.UPSERT, Row.of("id", 2L, "name", "b")))));
        t.write(new WriteRequest(1L, List.of(
                new WriteRequest.RowOperation(RowOp.DELETE, Row.of("id", 1L)),
                new WriteRequest.RowOperation(RowOp.UPSERT, Row.of("id", 3L, "name", "c")))));
        t.addColumn(new ColumnDef("tier", DataType.LONG, 0L), 2L);

        // 整理前留存每个版本的查询结论
        List<List<Row>> before = new ArrayList<>();
        for (long v = 1; v <= t.currentVersion(); v++) {
            before.add(t.readAt(v));
        }

        CompactionResult first = t.compact();
        log.info("第一次整理: {}", first.reason());
        assertEquals(3, first.versionsRewritten());

        // currentVersion 不变；逐版本结论一致（无丢失/重复/漂移）
        assertEquals(3L, t.currentVersion());
        for (long v = 1; v <= 3L; v++) {
            assertEquals(before.get((int) v - 1), t.readAt(v), "v" + v + " drifted after compaction");
        }
        assertEquals(2, t.readAt(1).size());
        assertEquals(2, t.readAt(2).size());
        assertEquals(2, t.readAt(3).size());

        // 再整理：规范化结果必须幂等，校验和不再变化
        CompactionResult second = t.compact();
        log.info("第二次整理: before={}, after={}", second.beforeChecksums(), second.afterChecksums());
        assertEquals(second.beforeChecksums(), second.afterChecksums(),
                "repeated compaction must be byte-stable after normalization");
        assertFalse(first.afterChecksums().isEmpty());
    }
}
