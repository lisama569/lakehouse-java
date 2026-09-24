package com.github.lisama569.lakehousejava.table;

import com.github.lisama569.lakehousejava.core.ColumnDef;
import com.github.lisama569.lakehousejava.core.CommitResult;
import com.github.lisama569.lakehousejava.core.CompactionResult;
import com.github.lisama569.lakehousejava.core.Row;
import com.github.lisama569.lakehousejava.core.Schema;
import com.github.lisama569.lakehousejava.core.SnapshotInfo;
import com.github.lisama569.lakehousejava.core.WriteRequest;
import com.github.lisama569.lakehousejava.exception.ConflictException;
import com.github.lisama569.lakehousejava.exception.SchemaEvolutionException;
import com.github.lisama569.lakehousejava.exception.TableException;
import com.github.lisama569.lakehousejava.reject.BatchRejectedException;
import com.github.lisama569.lakehousejava.storage.CompactionLog;
import com.github.lisama569.lakehousejava.storage.LocalFileIO;
import com.github.lisama569.lakehousejava.storage.SnapshotRecord;
import com.github.lisama569.lakehousejava.storage.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link LocalTable} 的本地文件实现。
 *
 * <p>并发模型（快照隔离 + 乐观并发控制）：
 * <ul>
 *   <li>同 JVM 内对同一张表的提交由一把可重入锁串行化；</li>
 *   <li>每次提交必须携带 expectedBaseVersion，与 metadata 中 currentVersion 不一致即抛
 *       {@link ConflictException}，不会静默覆盖，也不会部分生效；</li>
 *   <li>快照文件先落盘（含确定版本号），再原子更新 metadata 指针。</li>
 * </ul>
 */
final class LocalTableImpl implements LocalTable {

    private static final Logger log = LoggerFactory.getLogger(LocalTableImpl.class);

    private final Path tableDir;
    private final LocalFileIO fileIO;
    private TableMetadata metadata;
    private final ReentrantLock commitLock = new ReentrantLock();

    LocalTableImpl(Path tableDir, LocalFileIO fileIO, TableMetadata metadata) {
        this.tableDir = tableDir;
        this.fileIO = fileIO;
        this.metadata = metadata;
    }

    private Path metadataFile() {
        return tableDir.resolve("metadata.json");
    }

    private Path snapshotFile(long version) {
        return tableDir.resolve("snapshots").resolve(String.format("%019d.json", version));
    }

    private Path compactionDir() {
        return tableDir.resolve("compaction");
    }

    /** 读取快照并按其 schema 归一化数值类型，保证磁盘往返后类型确定。 */
    private SnapshotRecord loadRecord(long version) {
        Path f = snapshotFile(version);
        SnapshotRecord raw = fileIO.readSnapshot(f);
        return fileIO.readSnapshotNormalized(f, schemaFor(raw.schemaId()));
    }

    private Schema schemaFor(int schemaId) {
        for (TableMetadata.SchemaEntry e : metadata.schemas()) {
            if (e.schemaId() == schemaId) {
                return new Schema(e.schemaId(), metadata.primaryKey(), e.columns());
            }
        }
        throw new TableException("schema id " + schemaId + " not found for table " + metadata.tableName());
    }

    @Override
    public String name() {
        return metadata.tableName();
    }

    @Override
    public long currentVersion() {
        commitLock.lock();
        try {
            return metadata.currentVersion();
        } finally {
            commitLock.unlock();
        }
    }

    @Override
    public Schema currentSchema() {
        commitLock.lock();
        try {
            return schemaFor(metadata.nextSchemaId() - 1);
        } finally {
            commitLock.unlock();
        }
    }

    @Override
    public List<SnapshotInfo> listSnapshots() {
        commitLock.lock();
        try {
            List<SnapshotInfo> infos = new ArrayList<>();
            for (long v = 1; v <= metadata.currentVersion(); v++) {
                infos.add(toInfo(loadRecord(v)));
            }
            return infos;
        } finally {
            commitLock.unlock();
        }
    }

    private static SnapshotInfo toInfo(SnapshotRecord r) {
        return new SnapshotInfo(r.version(), r.schemaId(), r.rows().size(),
                r.createdAt(), r.operation(), r.description());
    }

    @Override
    public CommitResult write(WriteRequest request) {
        commitLock.lock();
        try {
            long base = metadata.currentVersion();
            if (request.expectedBaseVersion() != base) {
                log.info("[table={}] 写入被拒绝(版本冲突): 期望基准版本={}, 当前版本={}, 输入行数={}",
                        name(), request.expectedBaseVersion(), base,
                        request.operations() == null ? 0 : request.operations().size());
                throw new ConflictException(request.expectedBaseVersion(), base);
            }

            Schema schema = schemaFor(metadata.nextSchemaId() - 1);
            var reasons = WriteValidator.validate(request, schema);
            if (!reasons.isEmpty()) {
                log.info("[table={}] 写入批次被拒绝: 输入行数={}, 拒绝条数={}, 原因={}",
                        name(), request.operations().size(), reasons.size(), reasons);
                throw new BatchRejectedException(reasons);
            }

            BatchMerger.Collapsed collapsed = BatchMerger.collapse(request, schema);
            List<Row> baseline = base == 0 ? List.of()
                    : loadRecord(base).rows();
            List<Row> nextRows = BatchMerger.apply(baseline, collapsed.effective(), schema);

            long nextVersion = base + 1;
            String desc = "input=" + request.operations().size()
                    + ", effective=" + collapsed.effective().size()
                    + ", duplicatesCollapsed=" + collapsed.duplicates()
                    + ", rule=last-op-wins (DELETE final if trailing)";
            SnapshotRecord record = new SnapshotRecord(nextVersion, base, schema.schemaId(),
                    "WRITE", Instant.now(), desc, nextRows);
            fileIO.writeSnapshot(snapshotFile(nextVersion), record);
            updateMetadataAfterCommit(nextVersion, metadata.nextSchemaId());

            log.info("[table={}] 写入提交成功: 输入行数={}, 去重后生效={}, 收敛重复={}, 版本 {} -> {}, 存活行数={}",
                    name(), request.operations().size(), collapsed.effective().size(),
                    collapsed.duplicates(), base, nextVersion, nextRows.size());
            return new CommitResult(true, nextVersion, request.operations().size(),
                    collapsed.effective().size(), desc);
        } finally {
            commitLock.unlock();
        }
    }

    @Override
    public CommitResult addColumn(ColumnDef column, long expectedBaseVersion) {
        commitLock.lock();
        try {
            long base = metadata.currentVersion();
            if (expectedBaseVersion != base) {
                log.info("[table={}] 加列被拒绝(版本冲突): 期望基准版本={}, 当前版本={}",
                        name(), expectedBaseVersion, base);
                throw new ConflictException(expectedBaseVersion, base);
            }
            if (column == null || column.name() == null || column.name().isBlank()) {
                throw new SchemaEvolutionException("new column name must not be blank");
            }
            if (column.defaultValue() == null) {
                throw new SchemaEvolutionException(
                        "adding column '" + column.name() + "' requires a non-null default value");
            }
            Schema schema = schemaFor(metadata.nextSchemaId() - 1);
            if (schema.hasColumn(column.name())) {
                throw new SchemaEvolutionException(
                        "column '" + column.name() + "' already exists; type change/rename is not supported by addColumn");
            }
            if (!column.type().accepts(column.defaultValue())) {
                throw new SchemaEvolutionException(
                        "default value " + column.defaultValue() + " is not assignable to "
                                + column.type() + " for new column '" + column.name() + "'");
            }
            ColumnDef canonical = new ColumnDef(column.name(), column.type(),
                    column.type().normalize(column.defaultValue()));
            int newSchemaId = metadata.nextSchemaId();
            Schema newSchema = schema.withAppendedColumn(newSchemaId, canonical);

            List<Row> baseRows = base == 0 ? List.of()
                    : loadRecord(base).rows();
            List<Row> rewritten = new ArrayList<>();
            for (Row row : baseRows) {
                Row copy = new Row();
                copy.values().putAll(row.values());
                copy.put(canonical.name(), canonical.defaultValue());
                rewritten.add(copy);
            }

            long nextVersion = base + 1;
            String desc = "ADD_COLUMN '" + canonical.name() + "' " + canonical.type()
                    + " default=" + canonical.defaultValue()
                    + " (schema v" + schema.schemaId() + " -> v" + newSchemaId + ")";
            SnapshotRecord record = new SnapshotRecord(nextVersion, base, newSchemaId,
                    "ADD_COLUMN", Instant.now(), desc, rewritten);
            fileIO.writeSnapshot(snapshotFile(nextVersion), record);

            List<TableMetadata.SchemaEntry> newEntries = new ArrayList<>(metadata.schemas());
            newEntries.add(new TableMetadata.SchemaEntry(newSchemaId, newSchema.columns()));
            metadata = new TableMetadata(metadata.tableName(), metadata.primaryKey(),
                    nextVersion, newSchemaId + 1, newEntries);
            fileIO.writeJsonAtomically(metadataFile(), metadata);

            log.info("[table={}] 结构演进提交成功: 新增列 '{}' 类型={} 缺省值={}, 版本 {} -> {}, 回填历史行={}",
                    name(), canonical.name(), canonical.type(), canonical.defaultValue(),
                    base, nextVersion, rewritten.size());
            return new CommitResult(true, nextVersion, 0, 0, desc);
        } finally {
            commitLock.unlock();
        }
    }

    private void updateMetadataAfterCommit(long nextVersion, int nextSchemaId) {
        metadata = new TableMetadata(metadata.tableName(), metadata.primaryKey(),
                nextVersion, nextSchemaId, metadata.schemas());
        fileIO.writeJsonAtomically(metadataFile(), metadata);
    }

    @Override
    public List<Row> readAt(long version) {
        commitLock.lock();
        try {
            if (version < 1 || version > metadata.currentVersion()) {
                throw new TableException("version " + version + " does not exist for table "
                        + name() + " (current=" + metadata.currentVersion() + ")");
            }
            SnapshotRecord record = loadRecord(version);
            // 防御性拷贝，调用方无法篡改已提交快照
            List<Row> copy = new ArrayList<>();
            for (Row r : record.rows()) {
                Row clone = new Row();
                clone.values().putAll(r.values());
                copy.add(clone);
            }
            log.info("[table={}] 时间旅行读取: 版本={}, schema=v{}, 存活行数={}",
                    name(), version, record.schemaId(), copy.size());
            return copy;
        } finally {
            commitLock.unlock();
        }
    }

    @Override
    public List<Row> readLatest() {
        commitLock.lock();
        try {
            long v = metadata.currentVersion();
            if (v == 0) {
                log.info("[table={}] 读取最新版本: 表尚无数据版本, 返回空", name());
                return List.of();
            }
            return readAt(v);
        } finally {
            commitLock.unlock();
        }
    }

    @Override
    public CompactionResult compact() {
        commitLock.lock();
        try {
            long current = metadata.currentVersion();
            log.info("[table={}] 开始存储整理: 版本数={}", name(), current);

            Map<Long, String> before = new LinkedHashMap<>();
            Map<Long, String> after = new LinkedHashMap<>();
            int rewritten = 0;

            for (long v = 1; v <= current; v++) {
                Path file = snapshotFile(v);
                before.put(v, fileIO.sha256(file));
                SnapshotRecord record = loadRecord(v);
                // 规范化：按主键重排、行内列顺序按 schema 重排，语义不变
                Schema schema = schemaFor(record.schemaId());
                List<Row> canonicalRows = new ArrayList<>();
                for (Row row : record.rows()) {
                    Row fixed = new Row();
                    for (ColumnDef def : schema.columns()) {
                        fixed.put(def.name(), row.get(def.name()));
                    }
                    canonicalRows.add(fixed);
                }
                canonicalRows.sort((a, b) -> String.valueOf(a.get(schema.primaryKey()))
                        .compareTo(String.valueOf(b.get(schema.primaryKey()))));

                SnapshotRecord rewrittenRecord = new SnapshotRecord(record.version(),
                        record.parentVersion(), record.schemaId(), record.operation(),
                        record.createdAt(), record.description(), canonicalRows);
                Path tmp = tableDir.resolve("snapshots")
                        .resolve(String.format(".compact-%019d.tmp", v));
                fileIO.writeJsonAtomically(tmp, rewrittenRecord);
                String tmpChecksum = fileIO.sha256(tmp);
                // 语义一致性先比对行数与逐行内容，再决定是否替换字节布局
                verifySemanticEquals(record.rows(), canonicalRows, schema, v);
                try {
                    java.nio.file.Files.move(tmp, file,
                            java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                            java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
                after.put(v, fileIO.sha256(file));
                rewritten++;
                log.info("[table={}] 整理重写版本 v{}: 校验和 {} -> {} (字节布局可能变化, 语义一致)",
                        name(), v, before.get(v), after.get(v));
            }

            CompactionLog cLog = new CompactionLog(
                    String.format("compaction-%d-%d", current, System.currentTimeMillis()),
                    Instant.now(), rewritten,
                    Map.copyOf(before), Map.copyOf(after));
            fileIO.writeJsonAtomically(
                    compactionDir().resolve(cLog.id() + ".json"), cLog);

            // 整理不改 currentVersion，结论稳定性以校验日志为准
            boolean semanticStable = semanticChecksumsEqual(before, after);
            String reason = "compaction rewrote " + rewritten + " snapshot file(s); "
                    + "byte-checksums " + (semanticStable ? "identical" : "changed but row-level semantics verified identical")
                    + "; currentVersion remains " + current;
            log.info("[table={}] 存储整理完成: 重写版本数={}, {}", name(), rewritten, reason);
            return new CompactionResult(rewritten > 0, rewritten,
                    Map.copyOf(before), Map.copyOf(after), reason);
        } finally {
            commitLock.unlock();
        }
    }

    private void verifySemanticEquals(List<Row> a, List<Row> b, Schema schema, long version) {
        if (a.size() != b.size()) {
            throw new TableException("compaction safety check failed at v" + version
                    + ": row count drifted " + a.size() + " -> " + b.size());
        }
        for (int i = 0; i < a.size(); i++) {
            for (ColumnDef def : schema.columns()) {
                Object x = a.get(i).get(def.name());
                Object y = b.get(i).get(def.name());
                if (x == null ? y != null : !x.equals(y)) {
                    throw new TableException("compaction safety check failed at v" + version
                            + " column '" + def.name() + "': " + x + " -> " + y);
                }
            }
        }
    }

    private boolean semanticChecksumsEqual(Map<Long, String> before, Map<Long, String> after) {
        return before.equals(after);
    }
}
