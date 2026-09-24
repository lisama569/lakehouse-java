package com.github.lisama569.lakehousejava.table;

import com.github.lisama569.lakehousejava.core.ColumnDef;
import com.github.lisama569.lakehousejava.core.CommitResult;
import com.github.lisama569.lakehousejava.core.CompactionResult;
import com.github.lisama569.lakehousejava.core.Row;
import com.github.lisama569.lakehousejava.core.Schema;
import com.github.lisama569.lakehousejava.core.SnapshotInfo;
import com.github.lisama569.lakehousejava.core.WriteRequest;

import java.util.List;

/**
 * 本地数据湖表：主键写入、快照时间旅行、结构演进与存储整理的统一门面。
 *
 * <p>并发提交满足快照隔离：所有提交方法携带期望基准版本，过期提交被明确拒绝。
 */
public interface LocalTable {

    /** 表名 */
    String name();

    /** 当前最新版本号（刚建表未提交时为 0） */
    long currentVersion();

    /** 当前生效结构 */
    Schema currentSchema();

    /** 全部可寻址版本信息，按版本号升序 */
    List<SnapshotInfo> listSnapshots();

    /**
     * 按主键写入（插入/更新/删除），同批次重复主键按确定规则收敛。
     *
     * @param request 写入请求（含期望基准版本）
     * @return 提交结果
     */
    CommitResult write(WriteRequest request);

    /**
     * 新增列。必须给出确定的 defaultValue，旧版本按各自结构解释，新版本对历史行套用缺省值。
     *
     * @param column            新列定义
     * @param expectedBaseVersion 期望基准版本
     * @return 提交结果（产生新版本）
     */
    CommitResult addColumn(ColumnDef column, long expectedBaseVersion);

    /**
     * 读取指定版本的全量存活行（时间旅行）。版本不存在则抛出异常。
     */
    List<Row> readAt(long version);

    /** 读取最新版本的全量存活行 */
    List<Row> readLatest();

    /**
     * 存储整理：规范化重写各版本快照文件，校验整理前后每个版本结论一致。
     * 不产生新版本。
     */
    CompactionResult compact();
}
