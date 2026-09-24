package com.github.lisama569.lakehousejava.demo;

import com.github.lisama569.lakehousejava.core.ColumnDef;
import com.github.lisama569.lakehousejava.core.DataType;
import com.github.lisama569.lakehousejava.core.Row;
import com.github.lisama569.lakehousejava.core.RowOp;
import com.github.lisama569.lakehousejava.core.WriteRequest;
import com.github.lisama569.lakehousejava.exception.ConflictException;
import com.github.lisama569.lakehousejava.reject.BatchRejectedException;
import com.github.lisama569.lakehousejava.table.LocalLakehouse;
import com.github.lisama569.lakehousejava.table.LocalTable;

import java.nio.file.Path;
import java.util.List;

/**
 * 离线端到端演示：重复主键收敛 -> 时间旅行 -> 冲突拒绝 -> 结构演进 -> 整理。
 *
 * <p>运行：{@code mvn -o -q exec:java} 不可用时可用
 * {@code mvn -o spring-boot:run}（本类被组件扫描排除，需用 main 直接调用），
 * 或直接在 IDE 运行本 main。演示数据写入 ./lakehouse-demo 目录。
 */
public final class LakehouseDemo {

    private LakehouseDemo() {
    }

    private static WriteRequest.RowOperation up(long id, String name, String city) {
        return new WriteRequest.RowOperation(RowOp.UPSERT,
                Row.of("id", id, "name", name, "city", city));
    }

    private static WriteRequest.RowOperation del(long id) {
        return new WriteRequest.RowOperation(RowOp.DELETE, Row.of("id", id));
    }

    public static void main(String[] args) {
        Path dir = Path.of(args.length > 0 ? args[0] : "lakehouse-demo");
        var lh = new LocalLakehouse(dir);
        LocalTable t = lh.createTableIfAbsent("users", "id", List.of(
                new ColumnDef("id", DataType.LONG, null),
                new ColumnDef("name", DataType.STRING, null),
                new ColumnDef("city", DataType.STRING, null)));

        System.out.println("== 1. 重复主键批次（last-op-wins）==");
        var r1 = t.write(new WriteRequest(t.currentVersion(), List.of(
                up(1L, "ada", "BJ"), up(2L, "bob", "SH"),
                up(1L, "ada2", "BJ"), up(1L, "ada3", "SZ"))));
        System.out.println("提交: " + r1);

        System.out.println("== 2. 删除 + 新插入 ==");
        var r2 = t.write(new WriteRequest(t.currentVersion(), List.of(
                del(2L), up(3L, "cy", "GZ"))));
        System.out.println("提交: " + r2);

        System.out.println("== 3. 时间旅行 ==");
        System.out.println("v1 = " + t.readAt(1));
        System.out.println("v2 = " + t.readAt(2));

        System.out.println("== 4. 基于过期 v0 提交（应被拒绝）==");
        try {
            t.write(new WriteRequest(0L, List.of(up(9L, "ghost", "??"))));
        } catch (ConflictException e) {
            System.out.println("已拒绝: " + e.getMessage());
        }

        System.out.println("== 5. 非法批次（缺主键 + 类型不匹配 + 整列缺失 city）==");
        try {
            t.write(new WriteRequest(t.currentVersion(), List.of(
                    new WriteRequest.RowOperation(RowOp.UPSERT, Row.of("name", "x")),
                    new WriteRequest.RowOperation(RowOp.UPSERT, Row.of("id", 5L, "name", 42)))));
        } catch (BatchRejectedException e) {
            e.reasons().forEach(x -> System.out.println("已拒绝: " + x));
        }

        System.out.println("== 6. 结构演进：加列 level 缺省 0 ==");
        var r3 = t.addColumn(new ColumnDef("level", DataType.LONG, 0L), t.currentVersion());
        System.out.println("提交: " + r3);
        System.out.println("最新行 = " + t.readLatest());

        System.out.println("== 7. 整理 ==");
        var c = t.compact();
        System.out.println(c.reason());
        System.out.println("整理后 v1 仍为 = " + t.readAt(1));
        System.out.println("warehouse 目录: " + dir.toAbsolutePath());
    }
}
