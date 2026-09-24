package com.github.lisama569.lakehousejava.tests;

import com.github.lisama569.lakehousejava.core.ColumnDef;
import com.github.lisama569.lakehousejava.core.DataType;
import com.github.lisama569.lakehousejava.core.Row;
import com.github.lisama569.lakehousejava.core.RowOp;
import com.github.lisama569.lakehousejava.core.WriteRequest;
import com.github.lisama569.lakehousejava.table.LocalLakehouse;
import com.github.lisama569.lakehousejava.table.LocalTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SmokeWiringTest {

    @Test
    void writeAndReadBack(@TempDir Path dir) {
        LocalLakehouse lh = new LocalLakehouse(dir);
        LocalTable table = lh.createTableIfAbsent("users", "id", List.of(
                new ColumnDef("id", DataType.LONG, null),
                new ColumnDef("name", DataType.STRING, null),
                new ColumnDef("score", DataType.DOUBLE, null)));

        var ops = List.of(
                new WriteRequest.RowOperation(RowOp.UPSERT, Row.of("id", 1L, "name", "a", "score", 1.0)),
                new WriteRequest.RowOperation(RowOp.UPSERT, Row.of("id", 2L, "name", "b", "score", 2.0)));
        var result = table.write(new WriteRequest(0L, ops));
        assertEquals(1L, result.version());

        List<Row> rows = table.readLatest();
        assertEquals(2, rows.size());
        assertEquals("a", rows.get(0).get("name"));
    }
}
