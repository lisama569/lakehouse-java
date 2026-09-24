package com.github.lisama569.lakehousejava.tests;

import com.github.lisama569.lakehousejava.core.ColumnDef;
import com.github.lisama569.lakehousejava.core.CommitResult;
import com.github.lisama569.lakehousejava.core.DataType;
import com.github.lisama569.lakehousejava.core.Row;
import com.github.lisama569.lakehousejava.core.RowOp;
import com.github.lisama569.lakehousejava.core.WriteRequest;
import com.github.lisama569.lakehousejava.exception.ConflictException;
import com.github.lisama569.lakehousejava.exception.SchemaEvolutionException;
import com.github.lisama569.lakehousejava.table.LocalLakehouse;
import com.github.lisama569.lakehousejava.table.LocalTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 场景五：结构演进。
 * 加列必须给出确定缺省值并产生新版本；历史版本继续按旧结构解释；
 * 不兼容类型变更、删除既有列、重复加列均被拒绝。
 */
class SchemaEvolutionTest {

    private static final Logger log = LoggerFactory.getLogger(SchemaEvolutionTest.class);

    private LocalTable table(Path dir) {
        return new LocalLakehouse(dir).createTableIfAbsent("s", "id", List.of(
                new ColumnDef("id", DataType.LONG, null),
                new ColumnDef("name", DataType.STRING, null)));
    }

    private WriteRequest write(long base, Object... kv) {
        return new WriteRequest(base, List.of(new WriteRequest.RowOperation(
                RowOp.UPSERT, Row.of(kv))));
    }

    @Test
    void addColumnBackfillsDefaultButOldSnapshotsKeepOldShape(@TempDir Path dir) {
        LocalTable t = table(dir);
        t.write(write(0L, "id", 1L, "name", "a"));
        t.write(write(1L, "id", 2L, "name", "b"));

        CommitResult evolved = t.addColumn(
                new ColumnDef("tier", DataType.LONG, 0L), 2L);
        log.info("加列提交: 版本=v{}, 判定依据={}", evolved.version(), evolved.reason());
        assertEquals(3L, evolved.version());

        // 新版本：历史行回填确定缺省值，新写入必须提供新列
        var latest = t.readLatest();
        assertEquals(2, latest.size());
        assertEquals(0L, latest.get(0).get("tier"));
        assertEquals(0L, latest.get(1).get("tier"));

        // 旧版本仍按旧结构解释：没有 tier 列
        assertNull(t.readAt(1).get(0).get("tier"));
        assertNull(t.readAt(2).get(1).get("tier"));
        log.info("v1 行内容={} (按 schema v1 解释, 无 tier)", t.readAt(1));

        // 演进之后的写入，新列缺省值确定地出现在行内
        t.write(write(3L, "id", 3L, "name", "c", "tier", 7L));
        Row row3 = t.readLatest().stream().filter(r -> r.get("id").equals(3L)).findFirst().orElseThrow();
        assertEquals(7L, row3.get("tier"));
        log.info("加列后写入 id=3 tier=7, v4 该行={}", row3);
    }

    @Test
    void addColumnRequiresDefaultValue(@TempDir Path dir) {
        LocalTable t = table(dir);
        SchemaEvolutionException e = assertThrows(SchemaEvolutionException.class,
                () -> t.addColumn(new ColumnDef("tier", DataType.LONG, null), 0L));
        log.info("缺省值缺失被拒绝: {}", e.getMessage());
    }

    @Test
    void duplicateAndIncompatibleChangesRejected(@TempDir Path dir) {
        LocalTable t = table(dir);
        t.addColumn(new ColumnDef("tier", DataType.LONG, 0L), 0L);

        // 重复加同名列 => 拒绝
        assertThrows(SchemaEvolutionException.class,
                () -> t.addColumn(new ColumnDef("tier", DataType.LONG, 1L), 1L));
        // 删除既有列：本实现不提供删除入口；通过 addColumn 改名/改型同样拒绝
        SchemaEvolutionException typeChange = assertThrows(SchemaEvolutionException.class,
                () -> t.addColumn(new ColumnDef("name", DataType.LONG, 0L), 1L));
        log.info("不兼容变更(对既有列 name 做 LONG 重定义)被拒绝: {}", typeChange.getMessage());
        // 缺省值与类型不匹配
        assertThrows(SchemaEvolutionException.class,
                () -> t.addColumn(new ColumnDef("flag", DataType.BOOLEAN, "yes"), 1L));
        // 结构演进同样受快照隔离保护
        ConflictException conflict = assertThrows(ConflictException.class,
                () -> t.addColumn(new ColumnDef("x", DataType.STRING, "d"), 0L));
        log.info("结构演进版本冲突被拒绝: 期望 v0 当前 v{}", conflict.currentVersion());
        assertEquals(1L, t.currentVersion());
    }

    @Test
    void longToDoubleWideningOnlyViaExplicitRuleIsNotSilentlyApplied() {
        // 规则本身可解释：只有 LONG->DOUBLE 算放宽；其余拒绝
        assertEquals(true, DataType.LONG.isWideningTo(DataType.DOUBLE));
        assertEquals(false, DataType.DOUBLE.isWideningTo(DataType.LONG));
        assertEquals(false, DataType.STRING.isWideningTo(DataType.LONG));
        assertEquals(true, DataType.LONG.isWideningTo(DataType.LONG));
    }
}
