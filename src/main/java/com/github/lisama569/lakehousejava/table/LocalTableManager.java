package com.github.lisama569.lakehousejava.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Filesystem-backed {@link TableManager}.
 *
 * <p>Layout under the table directory:</p>
 * <pre>
 *   _lakehouse.json                 current metadata (version + schema), atomically replaced
 *   commits/&lt;v&gt;.commit.json          per-version manifest (delta or full snapshot)
 *   data/&lt;v&gt;.data.json              upserted rows of one write / the compacted snapshot
 *   data/&lt;v&gt;.deletes.json           deleted primary keys of one write
 * </pre>
 *
 * <p>Every successful commit appends a new version. Writes, schema evolution and
 * compaction never delete files; only an explicit {@link #vacuum(Long)} (after a
 * {@link RetentionPolicy} has been set) reclaims data files that no retained version
 * needs. Commit manifests under {@code commits/} are never deleted, so a reclaimed
 * version remains distinguishable from a version that never existed.</p>
 */
public class LocalTableManager implements TableManager {

    private static final Logger log = LoggerFactory.getLogger(LocalTableManager.class);
    private static final String KEY_SEP = "\u001f";

    /** Per-table-directory JVM locks linearizing commits across manager instances. */
    private static final ConcurrentHashMap<String, ReentrantLock> LOCKS = new ConcurrentHashMap<>();

    static final String META_FILE = "_lakehouse.json";

    private final Path tableDir;
    private final String name;
    private final JsonStore json;
    private final ObjectMapper mapper = new ObjectMapper();

    public LocalTableManager(Path tableDir, String name) {
        this(tableDir, name, new JsonStore());
    }

    LocalTableManager(Path tableDir, String name, JsonStore json) {
        this.tableDir = tableDir;
        this.name = name;
        this.json = json;
    }

    private ReentrantLock lockFor() {
        return LOCKS.computeIfAbsent(tableDir.toAbsolutePath().toString(), k -> new ReentrantLock());
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Path tableDir() {
        return tableDir;
    }

    @Override
    public long currentVersion() {
        return loadMeta().currentVersion;
    }

    @Override
    public Schema currentSchema() {
        return loadMeta().toSchema();
    }

    @Override
    public Schema schemaAt(Long versionOrNull) {
        long v = versionOrNull == null ? currentVersion() : versionOrNull;
        return readCommit(v).toSchema();
    }

    @Override
    public List<CommitInfo> history() {
        Path dir = commitsDir();
        List<CommitInfo> out = new ArrayList<>();
        try (var stream = Files.list(dir)) {
            stream.filter(p -> p.getFileName().toString().endsWith(".commit.json")).forEach(p -> {
                CommitRecord cr = json.read(p, CommitRecord.class);
                out.add(new CommitInfo(cr.version, cr.commitKind(), cr.timestampMs, cr.description));
            });
        } catch (IOException e) {
            throw new UncheckedIOException("failed to list commits of " + name, e);
        }
        out.sort(Comparator.comparingLong(CommitInfo::version));
        return out;
    }

    // ------------------------------------------------------------------ writes

    @Override
    public WriteResult write(WriteBatch batch) {
        ReentrantLock lock = lockFor();
        lock.lock();
        try {
            TableMetadata meta = loadMeta();
            long base = meta.currentVersion;
            if (batch.expectedBaseVersion() != null && batch.expectedBaseVersion() != base) {
                log.info("table={} write batch REJECTED decision=VERSION_CONFLICT expectedBase={} actualBase={} ops={}",
                        name, batch.expectedBaseVersion(), base, batch.ops().size());
                throw new TableException(RejectReasonCode.VERSION_CONFLICT,
                        "expected base version " + batch.expectedBaseVersion()
                                + " but current version is " + base);
            }
            Schema schema = meta.toSchema();

            List<RejectedRow> rejections = new ArrayList<>();
            List<PreparedOp> prepared = new ArrayList<>();
            for (int i = 0; i < batch.ops().size(); i++) {
                RowOp op = batch.ops().get(i);
                try {
                    prepared.add(new PreparedOp(encodeKey(op, schema), op.type(),
                            op.type() == RowOpType.UPSERT ? projectForWrite(op.row(), schema) : null));
                } catch (TableException e) {
                    rejections.add(new RejectedRow(i, e.code(), e.getMessage()));
                }
            }
            if (!rejections.isEmpty()) {
                log.info("table={} write batch REJECTED decision=ROW_VALIDATION_FAILED base={} ops={} rejections={}",
                        name, base, batch.ops().size(), rejections);
                return WriteResult.rejected(rejections);
            }

            // Deterministic duplicate-key convergence within the batch: last op wins.
            Map<String, PreparedOp> collapsed = new LinkedHashMap<>();
            for (PreparedOp p : prepared) {
                collapsed.put(p.key, p);
            }
            int duplicatesCollapsed = prepared.size() - collapsed.size();

            List<Map<String, Object>> upsertRows = new ArrayList<>();
            List<String> deletedKeys = new ArrayList<>();
            int upserts = 0;
            int deletes = 0;
            for (PreparedOp p : collapsed.values()) {
                if (p.type == RowOpType.UPSERT) {
                    upsertRows.add(p.row);
                    upserts++;
                } else {
                    deletedKeys.add(p.key);
                    deletes++;
                }
            }

            long newVersion = base + 1;
            List<String> dataFiles = new ArrayList<>();
            List<String> deleteFiles = new ArrayList<>();
            if (!upsertRows.isEmpty()) {
                String rel = "data/v" + newVersion + ".data.json";
                json.writeAtomic(tableDir.resolve(rel), upsertRows);
                dataFiles.add(rel);
            }
            if (!deletedKeys.isEmpty()) {
                String rel = "data/v" + newVersion + ".deletes.json";
                json.writeAtomic(tableDir.resolve(rel), deletedKeys);
                deleteFiles.add(rel);
            }

            CommitRecord cr = new CommitRecord(newVersion, base, CommitKind.APPEND_WRITE,
                    System.currentTimeMillis(),
                    "write batch: " + upserts + " upsert(s), " + deletes + " delete(s)",
                    false, schema);
            cr.dataFiles = dataFiles;
            cr.deleteFiles = deleteFiles;
            appendCommit(cr);

            meta.currentVersion = newVersion;
            json.writeAtomic(metaPath(), meta);

            log.info("table={} write batch COMMITTED decision=OK base={} newVersion={} ops={} upserts={} deletes={} duplicatesCollapsed={} files={}{}",
                    name, base, newVersion, batch.ops().size(), upserts, deletes,
                    duplicatesCollapsed, dataFiles, deleteFiles);
            return new WriteResult(true, newVersion, upserts, deletes, duplicatesCollapsed, List.of());
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ reads

    @Override
    public List<Row> readSnapshot(Long versionOrNull) {
        long version = versionOrNull == null ? currentVersion() : versionOrNull;
        if (!Files.isRegularFile(commitPath(version))) {
            throw new TableException(RejectReasonCode.VERSION_NOT_FOUND,
                    "table '" + name + "' has no version " + version);
        }
        CommitRecord target = readCommit(version);
        Schema schema = target.toSchema();

        // Walk back to the nearest full snapshot, then replay deltas in order.
        List<CommitRecord> chain = new ArrayList<>();
        long cursor = version;
        while (true) {
            CommitRecord cr = readCommit(cursor);
            chain.add(0, cr);
            if (cr.fullSnapshot) {
                break;
            }
            if (cursor == 0) {
                throw new IllegalStateException("no full snapshot reached before version 0");
            }
            cursor--;
        }

        TreeMap<String, Map<String, Object>> rows = new TreeMap<>();
        for (CommitRecord cr : chain) {
            for (String rel : cr.dataFiles) {
                if (!Files.isRegularFile(tableDir.resolve(rel))) {
                    throw expired(version, rel);
                }
            }
            for (String rel : cr.deleteFiles) {
                if (!Files.isRegularFile(tableDir.resolve(rel))) {
                    throw expired(version, rel);
                }
            }
            for (String rel : cr.dataFiles) {
                for (Map<String, Object> raw : readRows(rel)) {
                    rows.put(encodeStoredKey(raw, cr.primaryKey), raw);
                }
            }
            for (String rel : cr.deleteFiles) {
                for (String key : readKeys(rel)) {
                    rows.remove(key);
                }
            }
        }

        List<Row> result = new ArrayList<>();
        for (Map<String, Object> raw : rows.values()) {
            result.add(projectForRead(raw, schema));
        }
        log.info("table={} read snapshot version={} rows={} snapshotKind={}",
                name, version, result.size(), target.fullSnapshot ? "FULL" : "DELTA_REPLAY");
        return result;
    }

    // -------------------------------------------------------- schema evolution

    @Override
    public long addColumn(String columnName, DataType type, boolean nullable, Object defaultValue) {
        ReentrantLock lock = lockFor();
        lock.lock();
        try {
            TableMetadata meta = loadMeta();
            Schema schema = meta.toSchema();
            if (columnName == null || columnName.isBlank()) {
                throw reject(RejectReasonCode.SCHEMA_COLUMN_DUPLICATE, "column name must not be blank");
            }
            if (schema.column(columnName) != null) {
                throw reject(RejectReasonCode.SCHEMA_COLUMN_DUPLICATE,
                        "column already exists: " + columnName);
            }
            Column column = new Column(columnName, type, nullable, defaultValue);
            Schemas.checkDefault(column);
            if (!nullable && defaultValue == null) {
                throw reject(RejectReasonCode.SCHEMA_EVOLUTION_INCOMPATIBLE,
                        "new NOT NULL column '" + columnName + "' must declare a non-null default");
            }
            return commitSchemaChange(meta, appendColumn(schema, column),
                    "add column " + columnName + " " + type + " default=" + defaultValue);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long changeColumnType(String columnName, DataType newType) {
        ReentrantLock lock = lockFor();
        lock.lock();
        try {
            TableMetadata meta = loadMeta();
            Schema schema = meta.toSchema();
            Column old = schema.column(columnName);
            if (old == null) {
                throw reject(RejectReasonCode.SCHEMA_EVOLUTION_INCOMPATIBLE,
                        "unknown column: " + columnName);
            }
            if (!newType.isWideningOf(old.type())) {
                throw reject(RejectReasonCode.SCHEMA_EVOLUTION_INCOMPATIBLE,
                        "incompatible type change for column '" + columnName + "': "
                                + old.type() + " -> " + newType + " (only widening, e.g. LONG -> DOUBLE, is allowed)");
            }
            if (newType == old.type()) {
                log.info("table={} changeColumnType {} no-op (already {})", name, columnName, newType);
                return meta.currentVersion;
            }
            Object newDefault = old.defaultValue() == null ? null : Schemas.coerce(old.defaultValue(), newType);
            Column updated = new Column(columnName, newType, old.nullable(), newDefault);
            return commitSchemaChange(meta, replaceColumn(schema, updated),
                    "widen column " + columnName + " " + old.type() + " -> " + newType);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long dropColumn(String columnName) {
        Schema schema = currentSchema();
        if (schema.primaryKey().contains(columnName)) {
            throw reject(RejectReasonCode.SCHEMA_COLUMN_DROP_FORBIDDEN,
                    "primary-key column '" + columnName + "' cannot be dropped");
        }
        if (schema.column(columnName) == null) {
            throw reject(RejectReasonCode.SCHEMA_COLUMN_DROP_FORBIDDEN,
                    "column does not exist: " + columnName);
        }
        throw reject(RejectReasonCode.SCHEMA_COLUMN_DROP_FORBIDDEN,
                "dropping existing column '" + columnName + "' is forbidden to keep all versions interpretable");
    }

    // -------------------------------------------------------------- compaction

    @Override
    public long compact(Long expectedBaseVersion) {
        ReentrantLock lock = lockFor();
        lock.lock();
        try {
            TableMetadata meta = loadMeta();
            long base = meta.currentVersion;
            if (expectedBaseVersion != null && expectedBaseVersion != base) {
                log.info("table={} compact REJECTED decision=COMPACTION_CONFLICT expected={} actual={}",
                        name, expectedBaseVersion, base);
                throw new TableException(RejectReasonCode.COMPACTION_CONFLICT,
                        "expected base version " + expectedBaseVersion + " but current version is " + base);
            }
            Schema schema = meta.toSchema();

            TreeMap<String, Map<String, Object>> current = new TreeMap<>();
            long cursor = base;
            List<CommitRecord> chain = new ArrayList<>();
            while (true) {
                CommitRecord cr = readCommit(cursor);
                chain.add(0, cr);
                if (cr.fullSnapshot) {
                    break;
                }
                cursor--;
            }
            for (CommitRecord cr : chain) {
                for (String rel : cr.dataFiles) {
                    for (Map<String, Object> raw : readRows(rel)) {
                        current.put(encodeStoredKey(raw, cr.primaryKey), raw);
                    }
                }
                for (String rel : cr.deleteFiles) {
                    for (String key : readKeys(rel)) {
                        current.remove(key);
                    }
                }
            }

            long newVersion = base + 1;
            List<Map<String, Object>> compacted = new ArrayList<>();
            for (Map<String, Object> raw : current.values()) {
                compacted.add(rowToOrderedMap(projectForRead(raw, schema)));
            }
            String rel = "data/v" + newVersion + ".compact.data.json";
            json.writeAtomic(tableDir.resolve(rel), compacted);

            CommitRecord cr = new CommitRecord(newVersion, base, CommitKind.COMPACTION,
                    System.currentTimeMillis(),
                    "full snapshot compaction of " + compacted.size() + " live row(s)", true, schema);
            cr.dataFiles = List.of(rel);
            appendCommit(cr);

            meta.currentVersion = newVersion;
            json.writeAtomic(metaPath(), meta);
            log.info("table={} compact COMMITTED decision=OK base={} newVersion={} liveRows={} file={} (old files retained)",
                    name, base, newVersion, compacted.size(), rel);
            return newVersion;
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------- internals

    // ------------------------------------------------- retention / vacuum

    @Override
    public void setRetentionPolicy(RetentionPolicy policy) {
        if (policy == null) {
            throw reject(RejectReasonCode.RETENTION_POLICY_INVALID, "retention policy must not be null");
        }
        ReentrantLock lock = lockFor();
        lock.lock();
        try {
            TableMetadata meta = loadMeta();
            for (Long pinned : policy.pinnedVersions()) {
                if (pinned == null || pinned < 0 || pinned > meta.currentVersion) {
                    throw reject(RejectReasonCode.RETENTION_POLICY_INVALID,
                            "pinned version " + pinned + " does not exist on table '" + name
                                    + "' (current version " + meta.currentVersion + ")");
                }
            }
            meta.minVersionsToKeep = policy.minVersionsToKeep();
            meta.pinnedVersions = new ArrayList<>(policy.pinnedVersions());
            json.writeAtomic(metaPath(), meta);
            log.info("table={} retention policy SET minVersionsToKeep={} pinnedVersions={} (no data changed)",
                    name, policy.minVersionsToKeep(), policy.pinnedVersions());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public RetentionPolicy retentionPolicy() {
        return loadMeta().toRetentionPolicy();
    }

    @Override
    public VacuumResult vacuum(Long expectedBaseVersion) {
        ReentrantLock lock = lockFor();
        lock.lock();
        VacuumPlan plan;
        try {
            TableMetadata meta = loadMeta();
            long base = meta.currentVersion;
            if (expectedBaseVersion != null && expectedBaseVersion != base) {
                log.info("table={} vacuum ABORTED decision=VACUUM_CONFLICT expected={} actual={}",
                        name, expectedBaseVersion, base);
                throw new TableException(RejectReasonCode.VACUUM_CONFLICT,
                        "expected base version " + expectedBaseVersion + " but current version is " + base);
            }
            RetentionPolicy policy = meta.toRetentionPolicy();
            if (policy == null) {
                throw new TableException(RejectReasonCode.RETENTION_POLICY_MISSING,
                        "table '" + name + "' has no retention policy; call setRetentionPolicy(...) before vacuum");
            }

            // Versions that must stay fully readable: latest, the recent window and every pinned version.
            TreeSet<Long> retained = new TreeSet<>();
            retained.add(base);
            long windowStart = Math.max(0L, base - (long) policy.minVersionsToKeep() + 1);
            for (long v = windowStart; v <= base; v++) {
                retained.add(v);
            }
            for (Long pinned : policy.pinnedVersions()) {
                retained.add(pinned);
            }

            // Every file in each retained version's replay chain (anchor snapshot -> version) is protected.
            Set<String> protectedFiles = new HashSet<>();
            for (long retainedVersion : retained) {
                for (CommitRecord cr : replayChain(retainedVersion)) {
                    protectedFiles.addAll(cr.dataFiles);
                    protectedFiles.addAll(cr.deleteFiles);
                }
            }

            // Everything a manifest ever referenced but no retained version needs is garbage.
            TreeSet<String> garbage = new TreeSet<>();
            for (long v = 0; v <= base; v++) {
                CommitRecord cr = readCommit(v);
                for (String rel : cr.dataFiles) {
                    if (!protectedFiles.contains(rel) && Files.isRegularFile(tableDir.resolve(rel))) {
                        garbage.add(rel);
                    }
                }
                for (String rel : cr.deleteFiles) {
                    if (!protectedFiles.contains(rel) && Files.isRegularFile(tableDir.resolve(rel))) {
                        garbage.add(rel);
                    }
                }
            }

            if (garbage.isEmpty()) {
                log.info("table={} vacuum NOOP base={} retainedVersions={} (nothing eligible)",
                        name, base, retained);
                return new VacuumResult(base, List.of(), 0L, new ArrayList<>(retained), List.of(), true);
            }
            plan = new VacuumPlan(base, new ArrayList<>(retained), new ArrayList<>(garbage));
        } finally {
            lock.unlock();
        }

        // Seam used by interleaving tests: a concurrent commit may land here, invalidating the premise.
        beforeVacuumDelete(plan.baseVersion);

        lock.lock();
        try {
            // Re-take the premise under the lock: any newer commit aborts the whole vacuum atomically.
            long current = loadMeta().currentVersion;
            if (current != plan.baseVersion) {
                log.info("table={} vacuum ABORTED decision=VACUUM_CONFLICT plannedBase={} actualBase={} (nothing deleted)",
                        name, plan.baseVersion, current);
                throw new TableException(RejectReasonCode.VACUUM_CONFLICT,
                        "table '" + name + "' advanced from " + plan.baseVersion + " to " + current
                                + " while vacuum was pending; aborted without deleting any file");
            }

            List<String> deleted = new ArrayList<>();
            long bytesReclaimed = 0L;
            for (int i = 0; i < plan.garbageFiles.size(); i++) {
                String rel = plan.garbageFiles.get(i);
                Path file = tableDir.resolve(rel);
                long size = fileSizeIfRegular(file);
                // Seam used by interruption tests; re-running the vacuum resumes deterministically.
                beforeVacuumFileDelete(plan.baseVersion, i, plan.garbageFiles.size());
                if (deleteIfExists(file)) {
                    deleted.add(rel);
                    bytesReclaimed += size;
                }
            }

            // A version is expired exactly when its replay chain lost at least one file.
            List<Long> expired = new ArrayList<>();
            for (long v = 0; v < plan.baseVersion; v++) {
                if (plan.retainedVersions.contains(v)) {
                    continue;
                }
                for (CommitRecord cr : replayChain(v)) {
                    boolean missing = false;
                    for (String rel : cr.dataFiles) {
                        if (!Files.isRegularFile(tableDir.resolve(rel))) {
                            missing = true;
                            break;
                        }
                    }
                    if (!missing) {
                        for (String rel : cr.deleteFiles) {
                            if (!Files.isRegularFile(tableDir.resolve(rel))) {
                                missing = true;
                                break;
                            }
                        }
                    }
                    if (missing) {
                        expired.add(v);
                        break;
                    }
                }
            }

            VacuumRecord record = new VacuumRecord(nextVacuumSequence(), plan.baseVersion,
                    System.currentTimeMillis());
            record.completed = true;
            record.deletedFiles = deleted;
            record.bytesReclaimed = bytesReclaimed;
            record.retainedVersions = plan.retainedVersions;
            record.expiredVersions = expired;
            json.writeAtomic(vacuumPath(record.sequence), record);

            log.info("table={} vacuum COMMITTED decision=OK base={} deletedFiles={} bytesReclaimed={} retained={} expired={}",
                    name, plan.baseVersion, deleted.size(), bytesReclaimed,
                    plan.retainedVersions, expired);
            return new VacuumResult(plan.baseVersion, deleted, bytesReclaimed,
                    plan.retainedVersions, expired, false);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<VacuumRecord> vacuumHistory() {
        Path dir = vacuumsDir();
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        List<VacuumRecord> out = new ArrayList<>();
        try (var stream = Files.list(dir)) {
            stream.filter(p -> p.getFileName().toString().endsWith(".vacuum.json")).forEach(p ->
                    out.add(json.read(p, VacuumRecord.class)));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to list vacuum ledger of " + name, e);
        }
        out.sort(Comparator.comparingLong(r -> r.sequence));
        return out;
    }

    /** Build the replay chain of a version: nearest full snapshot first, then deltas up to it. */
    private List<CommitRecord> replayChain(long version) {
        List<CommitRecord> chain = new ArrayList<>();
        long cursor = version;
        while (true) {
            CommitRecord cr = readCommit(cursor);
            chain.add(0, cr);
            if (cr.fullSnapshot) {
                break;
            }
            cursor--;
        }
        return chain;
    }

    /** Interleaving seam: invoked after planning, before the deletion phase reacquires the lock. */
    void beforeVacuumDelete(long baseVersion) {
        // no-op by default; tests override to run a concurrent commit
    }

    /** Interruption seam: invoked immediately before physically deleting the file at {@code index}. */
    void beforeVacuumFileDelete(long baseVersion, int index, int total) {
        // no-op by default; tests override to simulate a crash mid-vacuum
    }

    private long nextVacuumSequence() {
        long max = -1L;
        Path dir = vacuumsDir();
        if (Files.isDirectory(dir)) {
            try (var stream = Files.list(dir)) {
                for (Path p : stream.toList()) {
                    String fn = p.getFileName().toString();
                    if (fn.endsWith(".vacuum.json")) {
                        try {
                            max = Math.max(max, Long.parseLong(fn.substring(0, fn.length() - ".vacuum.json".length())));
                        } catch (NumberFormatException ignored) {
                            // skip unrelated file
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("failed to scan vacuum ledger of " + name, e);
            }
        }
        return max + 1;
    }

    private Path vacuumsDir() {
        return tableDir.resolve("vacuums");
    }

    private long fileSizeIfRegular(Path file) {
        try {
            return Files.isRegularFile(file) ? Files.size(file) : 0L;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to stat " + file, e);
        }
    }

    private boolean deleteIfExists(Path file) {
        try {
            return Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to delete " + file, e);
        }
    }

    private Path vacuumPath(long sequence) {
        return vacuumsDir().resolve(sequence + ".vacuum.json");
    }

    private record VacuumPlan(long baseVersion, List<Long> retainedVersions, List<String> garbageFiles) {
    }

    private long commitSchemaChange(TableMetadata meta, Schema newSchema, String description) {
        long base = meta.currentVersion;
        long newVersion = base + 1;
        CommitRecord cr = new CommitRecord(newVersion, base, CommitKind.SCHEMA_CHANGE,
                System.currentTimeMillis(), description, false, newSchema);
        appendCommit(cr);

        meta.currentVersion = newVersion;
        meta.schema.clear();
        for (String columnName : newSchema.columnNames()) {
            meta.schema.add(new TableMetadata.StoredColumn(newSchema.column(columnName)));
        }
        json.writeAtomic(metaPath(), meta);
        log.info("table={} schema change COMMITTED decision=OK base={} newVersion={} detail='{}'",
                name, base, newVersion, description);
        return newVersion;
    }

    private Schema appendColumn(Schema schema, Column column) {
        List<String> names = new ArrayList<>(schema.columnNames());
        names.add(column.name());
        Map<String, Column> cols = new LinkedHashMap<>(schema.columns());
        cols.put(column.name(), column);
        return new Schema(List.copyOf(names), Map.copyOf(cols), schema.primaryKey());
    }

    private Schema replaceColumn(Schema schema, Column updated) {
        Map<String, Column> cols = new LinkedHashMap<>(schema.columns());
        cols.put(updated.name(), updated);
        return new Schema(schema.columnNames(), Map.copyOf(cols), schema.primaryKey());
    }

    private void appendCommit(CommitRecord cr) {
        Path target = commitPath(cr.version);
        try {
            Files.createDirectories(target.getParent());
            Path tmp = Files.createTempFile(target.getParent(), "v" + cr.version + ".", ".tmp");
            mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), cr);
            // A version number is committed at most once: existence => lost the race.
            try {
                Files.move(tmp, target, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicFailed) {
                if (Files.exists(target)) {
                    Files.deleteIfExists(tmp);
                    throw new TableException(RejectReasonCode.VERSION_CONFLICT,
                            "commit file for version " + cr.version + " already exists");
                }
                Files.move(tmp, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append commit v" + cr.version, e);
        }
    }

    private String encodeKey(RowOp op, Schema schema) {
        Row row = op.row();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < schema.primaryKey().size(); i++) {
            if (i > 0) {
                sb.append(KEY_SEP);
            }
            String pk = schema.primaryKey().get(i);
            Object value = row.get(pk);
            if (value == null) {
                throw reject(RejectReasonCode.MISSING_PRIMARY_KEY,
                        op.type() + " at index is missing primary-key column '" + pk + "'");
            }
            if (op.type() == RowOpType.UPSERT) {
                value = Schemas.coerce(value, schema.column(pk).type());
            } else if (!isPlainKeyCompatible(value, schema.column(pk).type())) {
                throw reject(RejectReasonCode.TYPE_MISMATCH,
                        "delete key value '" + value + "' is not assignable to " + schema.column(pk).type());
            }
            sb.append(value);
        }
        return sb.toString();
    }

    private boolean isPlainKeyCompatible(Object value, DataType type) {
        return switch (type) {
            case STRING -> value instanceof String || value instanceof Number || value instanceof Boolean;
            case LONG -> value instanceof Long || value instanceof Integer
                    || (value instanceof String s && isLong(s));
            case DOUBLE -> value instanceof Number || (value instanceof String s && isDouble(s));
            case BOOLEAN -> value instanceof Boolean
                    || (value instanceof String s && (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")));
        };
    }

    private static boolean isLong(String s) {
        try {
            Long.parseLong(s.trim());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isDouble(String s) {
        try {
            Double.parseDouble(s.trim());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private Map<String, Object> projectForWrite(Row row, Schema schema) {
        for (String given : row.values().keySet()) {
            if (schema.column(given) == null) {
                throw reject(RejectReasonCode.UNKNOWN_COLUMN, "unknown column '" + given + "'");
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (String columnName : schema.columnNames()) {
            Column column = schema.column(columnName);
            Object value = Schemas.coerce(row.get(columnName), column.type());
            if (value == null) {
                value = column.defaultValue() == null ? null : Schemas.coerce(column.defaultValue(), column.type());
            }
            if (value == null && !column.nullable()) {
                throw reject(RejectReasonCode.MISSING_REQUIRED_VALUE,
                        "missing value for NOT NULL column '" + columnName + "'");
            }
            out.put(columnName, value);
        }
        return out;
    }

    private Row projectForRead(Map<String, Object> raw, Schema targetSchema) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (String columnName : targetSchema.columnNames()) {
            Column column = targetSchema.column(columnName);
            Object value;
            if (raw.containsKey(columnName)) {
                value = Schemas.coerce(raw.get(columnName), column.type());
            } else {
                value = column.defaultValue() == null ? null : Schemas.coerce(column.defaultValue(), column.type());
            }
            out.put(columnName, value);
        }
        return new Row(out);
    }

    private Map<String, Object> rowToOrderedMap(Row row) {
        return new LinkedHashMap<>(row.values());
    }

    private String encodeStoredKey(Map<String, Object> raw, List<String> primaryKey) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < primaryKey.size(); i++) {
            if (i > 0) {
                sb.append(KEY_SEP);
            }
            sb.append(raw.get(primaryKey.get(i)));
        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readRows(String rel) {
        return (List<Map<String, Object>>) (List<?>) json.readList(tableDir.resolve(rel), Map.class);
    }

    @SuppressWarnings("unchecked")
    private List<String> readKeys(String rel) {
        return (List<String>) (List<?>) json.readList(tableDir.resolve(rel), String.class);
    }

    private CommitRecord readCommit(long version) {
        Path p = commitPath(version);
        if (!Files.isRegularFile(p)) {
            throw new TableException(RejectReasonCode.VERSION_NOT_FOUND,
                    "table '" + name + "' has no version " + version);
        }
        return json.read(p, CommitRecord.class);
    }

    private TableMetadata loadMeta() {
        if (!Files.isRegularFile(metaPath())) {
            throw new TableException(RejectReasonCode.EMPTY_TABLE_NAME,
                    "table metadata missing under " + tableDir);
        }
        return json.read(metaPath(), TableMetadata.class);
    }

    private Path metaPath() {
        return tableDir.resolve(META_FILE);
    }

    private Path commitsDir() {
        return tableDir.resolve("commits");
    }

    private Path commitPath(long version) {
        return commitsDir().resolve(version + ".commit.json");
    }

    private static TableException reject(RejectReasonCode code, String detail) {
        return new TableException(code, detail);
    }

    /** The version's commit manifest exists, but data it needs was reclaimed: distinct from "never existed". */
    private TableException expired(long version, String missingRel) {
        return new TableException(RejectReasonCode.VERSION_EXPIRED,
                "version " + version + " of table '" + name
                        + "' existed but its history was reclaimed by an explicit vacuum"
                        + " (missing data file '" + missingRel + "'); it is no longer readable."
                        + " Use currentVersion() for readable versions, or pin versions via setRetentionPolicy(...).");
    }

    private record PreparedOp(String key, RowOpType type, Map<String, Object> row) {
    }
}
