package com.github.lisama569.lakehousejava.table;

import java.nio.file.Path;

/**
 * Entry point: manages a directory of local tables with snapshot isolation.
 */
public interface LakehouseEngine {

    TableManager createTable(String name, Schema schema);

    TableManager openTable(String name);

    Path warehouseRoot();
}
