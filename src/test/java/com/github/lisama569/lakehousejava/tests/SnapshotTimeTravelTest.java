package com.github.lisama569.lakehousejava.tests;

import com.github.lisama569.lakehousejava.core.ColumnDef;
import com.github.lisama569.lakehousejava.core.DataType;
import com.github.lisama569.lakehousejava.core.Row;
import com.github.lisama569.lakehousejava.core.RowOp;
import com.github.lisama569.lakehousejava.core.SnapshotInfo;
import com.github.lisama569.lakehousejava.core.WriteRequest;
import com.github.lisama569.lakehousejava.exception.TableException;
import com.github.lisama569.lakehousejava.table.LocalLakehouse;
import com.github.lisama569.lakehousejava.table.LocalTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 场景三：每次成功提交产生可寻址新版本；时间旅行读取在后续写入后保持稳定。
 */
class SnapshotTimeTravelTest {

    private static final Logger log = LoggerFactory.getLogger(SnapshotTimeTravelTest.class);

    private WriteRequest.RowOperation up(long id, String name) {
        return new WriteRequest.RowOperation(RowOp.UPSERT, Row.of("id", id, "name", name));
    }

    @Test
    void versionsAreAddressableAndImmutable(@TempDir Path dir) {
        LocalTable table = new LocalLakehouse(dir).createTableIfAbsent("p", "id", List.of(
                new ColumnDef("id", DataType.LONG, null),
                new ColumnDef("name", DataType.STRING, null)));

        table.write(new WriteRequest(0L, List.of(up(1L, "v1-a"), up(2L, "v1-b"))));
        table.write(new WriteRequest(1L, List.of(
                new WriteRequest.RowOperation(RowOp.DELETE, Row.of("id", 2L)),
                up(3L, "v2-c"))));
        table.write(new WriteRequest(2L, List.of(up(1L, "v3-a-updated"))));

        // 时间旅行：每个历史版本的结论独立、稳定
        List<Row> v1 = table.readAt(1);
        List<Row> v2 = table.readAt(2);
        List<Row> v3 = table.readAt(3);

        log.info("v1 行数={} 内容={}", v1.size(), v1);
        log.info("v2 行数={} 内容={}", v2.size(), v2);
        log.info("v3 行数={} 内容={}", v3.size(), v3);

        assertEquals(2, v1.size());
        assertEquals("v1-a", v1.get(0).get("name"));
        assertEquals(2, v2.size());
        assertEquals("v2-c", v2.stream().filter(r -> r.get("id").equals(3L)).findFirst().orElseThrow().get("name"));
        assertEquals(2, v3.size());
        assertEquals("v3-a-updated", v3.get(0).get("name"));

        // 版本清单单调、可枚举
        List<SnapshotInfo> snaps = table.listSnapshots();
        assertEquals(List.of(1L, 2L, 3L), snaps.stream().map(SnapshotInfo::version).toList());
        log.info("快照清单: {}", snaps.stream().map(s -> "v" + s.version() + "/" + s.operation()).toList());

        // 越界版本明确报错而非返回空
        assertThrows(TableException.class, () -> table.readAt(99));

        // 快照不可变：调用方拿到的是拷贝，篡改不影响存储
        v1.get(0).put("name", "HACKED");
        assertNotEquals("HACKED", table.readAt(1).get(0).get("name"));
    }

    @Test
    void reloadFromDiskKeepsAllSnapshots(@TempDir Path dir) {
        LocalLakehouse lh = new LocalLakehouse(dir);
        LocalTable t = lh.createTableIfAbsent("p", "id", List.of(
                new ColumnDef("id", DataType.LONG, null),
                new ColumnDef("name", DataType.STRING, null)));
        t.write(new WriteRequest(0L, List.of(up(1L, "a"))));
        t.write(new WriteRequest(1L, List.of(up(2L, "b"))));

        // 模拟重新打开进程：仅从本地文件即可复现全部历史
        LocalTable reopened = lh.loadTable("p");
        assertEquals(2L, reopened.currentVersion());
        assertEquals(1, reopened.readAt(1).size());
        assertEquals(2, reopened.readAt(2).size());
        log.info("重新加载后 v1={}, v2={}, 离线可复现", reopened.readAt(1), reopened.readAt(2));
    }
}
