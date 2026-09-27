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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
 * <p>Every successful commit appends a new version; files are never deleted, so
 * any historical version remains reproducible after later writes and compactions.</p>
 */
public final class LocalTableManager implements TableManager {

    private static final Logger log = LoggerFactory.getLogger(LocalTableManager.class);
    private static final String KEY_SEP = "\u001f";

    /** Per-table-directory JVM locks linearizing commits across manager instances. */
    private static final ConcurrentHashMap<String, ReentrantLock> LOCKS = new ConcurrentHashMap<>();

    static final String META_FILE = "_lakehouse.json";
    static final String RETENTION_FILE = "_retention.json";

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
        if (isExpired(v)) {
            throw new TableException(RejectReasonCode.VERSION_EXPIRED,
                    "table '" + name + "' version " + v + " was expired by retention");
        }
        return readCommit(v).toSchema();
    }

    @Override
    public List<CommitInfo> history() {
        Path dir = commitsDir();
        java.util.Set<Long> expiredSet = new java.util.HashSet<>(expiredVersions());
        List<CommitInfo> out = new ArrayList<>();
        try (var stream = Files.list(dir)) {
            stream.filter(p -> p.getFileName().toString().endsWith(".commit.json")).forEach(p -> {
                CommitRecord cr = json.read(p, CommitRecord.class);
                out.add(new CommitInfo(cr.version, cr.commitKind(), cr.timestampMs, cr.description,
                        expiredSet.contains(cr.version)));
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
        TableMetadata meta = loadMeta();
        RetentionState retention = loadRetentionStateOrNull();
        long version = versionOrNull == null ? meta.currentVersion : versionOrNull;
        if (retention != null && retention.expired.stream().anyMatch(e -> e.version == version)) {
            throw new TableException(RejectReasonCode.VERSION_EXPIRED,
                    "table '" + name + "' version " + version
                            + " once existed but was expired by retention run generation "
                            + expiredGeneration(retention, version)
                            + "; it is no longer readable (distinct from a version that never existed)");
        }
        if (!Files.isRegularFile(commitPath(version))) {
            throw new TableException(RejectReasonCode.VERSION_NOT_FOUND,
                    "table '" + name + "' has no version " + version);
        }
        CommitRecord target = readCommit(version);
        Schema schema = target.toSchema();

        // Walk back to the nearest full snapshot (or a GC-materialized retained snapshot),
        // then replay deltas in order.
        List<ReplayStep> chain = collectChain(version, retention);

        TreeMap<String, Map<String, Object>> rows = replayChain(chain, target.primaryKey);

        List<Row> result = new ArrayList<>();
        for (Map<String, Object> raw : rows.values()) {
            result.add(projectForRead(raw, schema));
        }
        String kind = retention != null && retentionEntry(retention, version) != null
                ? (version == meta.currentVersion ? "RETAINED_LATEST" : "RETAINED")
                : (target.fullSnapshot ? "FULL" : "DELTA_REPLAY");
        log.info("table={} read snapshot version={} rows={} snapshotKind={}", name, version, result.size(), kind);
        return result;
    }

    private static long expiredGeneration(RetentionState state, long version) {
        return state.expired.stream().filter(e -> e.version == version).findFirst()
                .map(e -> e.gcGeneration).orElse(-1L);
    }

    private static RetentionState.Entry retentionEntry(RetentionState state, long version) {
        if (state == null) {
            return null;
        }
        return state.retained.stream().filter(e -> e.version == version).findFirst().orElse(null);
    }

    /**
     * Collect the ascending replay chain for {@code version}: backwards along the
     * parent chain until a commit marked full snapshot, or until a version that
     * the retention state materialized as a standalone snapshot (GC boundary).
     */
    private List<ReplayStep> collectChain(long version, RetentionState retention) {
        List<ReplayStep> chain = new ArrayList<>();
        long cursor = version;
        while (true) {
            RetentionState.Entry boundary = retentionEntry(retention, cursor);
            CommitRecord cr;
            if (boundary != null) {
                cr = readExistingCommit(cursor, retention);
                chain.add(0, new ReplayStep(cr, boundary.snapshotFile));
                return chain;
            }
            Path p = commitPath(cursor);
            if (!Files.isRegularFile(p)) {
                final long missing = cursor;
                // Manifest was physically removed as garbage: it can only be an expired version.
                if (retention != null && retention.expired.stream().anyMatch(e -> e.version == missing)) {
                    throw new TableException(RejectReasonCode.VERSION_EXPIRED,
                            "table '" + name + "' version " + missing
                                    + " once existed but was expired by retention and cannot be replayed");
                }
                throw new TableException(RejectReasonCode.VERSION_NOT_FOUND,
                        "table '" + name + "' has no version " + missing);
            }
            cr = json.read(p, CommitRecord.class);
            chain.add(0, new ReplayStep(cr, null));
            if (cr.fullSnapshot) {
                return chain;
            }
            if (cursor == 0) {
                throw new IllegalStateException("no full snapshot reached before version 0");
            }
            cursor--;
        }
    }

    private CommitRecord readExistingCommit(long version, RetentionState retention) {
        Path p = commitPath(version);
        if (!Files.isRegularFile(p)) {
            if (retention != null && retention.expired.stream().anyMatch(e -> e.version == version)) {
                throw new TableException(RejectReasonCode.VERSION_EXPIRED,
                        "table '" + name + "' version " + version + " was expired by retention");
            }
            throw new TableException(RejectReasonCode.VERSION_NOT_FOUND,
                    "table '" + name + "' has no version " + version);
        }
        return json.read(p, CommitRecord.class);
    }

    private record ReplayStep(CommitRecord commit, String fullSnapshotFile) {
    }

    /** Replay an ascending chain: a materialized full-snapshot step resets rows, then deltas apply. */
    private TreeMap<String, Map<String, Object>> replayChain(List<ReplayStep> chain, List<String> primaryKey) {
        TreeMap<String, Map<String, Object>> rows = new TreeMap<>();
        for (ReplayStep step : chain) {
            if (step.fullSnapshotFile != null) {
                // GC-materialized boundary: the snapshot already contains this
                // version's complete state, so its own delta files must not replay.
                rows.clear();
                for (Map<String, Object> raw : readRows(step.fullSnapshotFile)) {
                    rows.put(encodeStoredKey(raw, primaryKey), raw);
                }
                continue;
            }
            CommitRecord cr = step.commit;
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
        return rows;
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

            // Replay through GC-materialized snapshots as well, so compaction works
            // after expiration removed older delta files.
            TreeMap<String, Map<String, Object>> current = replayChain(collectChain(base, loadRetentionStateOrNull()),
                    schema.primaryKey());

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

    // ------------------------------------------------------------- retention

    @Override
    public void setRetentionPolicy(RetentionPolicy policy) {
        ReentrantLock lock = lockFor();
        lock.lock();
        try {
            TableMetadata meta = loadMeta();
            RetentionPolicy effective = policy == null ? RetentionPolicy.keepAll() : policy;
            long current = meta.currentVersion;
            for (Long pinned : effective.pinnedVersions()) {
                if (pinned < 0 || pinned > current) {
                    throw reject(RejectReasonCode.RETENTION_POLICY_INVALID,
                            "pinned version " + pinned + " does not exist (current version is " + current + ")");
                }
                if (isExpired(pinned)) {
                    throw reject(RejectReasonCode.RETENTION_POLICY_INVALID,
                            "version " + pinned + " was already expired and cannot be pinned");
                }
            }
            meta.retainLatestVersions = effective.retainLatestVersions();
            meta.pinnedVersions = new ArrayList<>(effective.pinnedVersions());
            json.writeAtomic(metaPath(), meta);
            log.info("table={} retention policy SET retainLatest={} pinned={}",
                    name, effective.retainLatestVersions(), effective.pinnedVersions());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public RetentionPolicy retentionPolicy() {
        TableMetadata meta = loadMeta();
        List<Long> pinned = meta.pinnedVersions == null ? List.of() : List.copyOf(meta.pinnedVersions);
        return new RetentionPolicy(meta.retainLatestVersions, pinned);
    }

    @Override
    public List<Long> readableVersions() {
        ReentrantLock lock = lockFor();
        lock.lock();
        try {
            RetentionState state = loadRetentionStateOrNull();
            if (state == null) {
                long current = loadMeta().currentVersion;
                List<Long> all = new ArrayList<>();
                for (long v = 0; v <= current; v++) {
                    all.add(v);
                }
                return all;
            }
            return state.retained.stream().map(e -> e.version).sorted().toList();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<Long> expiredVersions() {
        RetentionState state = loadRetentionStateOrNull();
        if (state == null) {
            return List.of();
        }
        return state.expired.stream().map(e -> e.version).sorted().toList();
    }

    private RetentionState loadRetentionStateOrNull() {
        Path p = retentionPath();
        if (!Files.isRegularFile(p)) {
            return null;
        }
        return json.read(p, RetentionState.class);
    }

    private boolean isExpired(long version) {
        RetentionState state = loadRetentionStateOrNull();
        if (state == null) {
            return false;
        }
        return state.expired.stream().anyMatch(e -> e.version == version);
    }

    private Path retentionPath() {
        return tableDir.resolve(RETENTION_FILE);
    }

    @Override
    public ExpireResult expireSnapshots(Long expectedBaseVersion) {
        return expireSnapshots(expectedBaseVersion, ExpireHook.NOOP);
    }

    // package-private variant with a crash-injection hook (tests only)
    ExpireResult expireSnapshots(Long expectedBaseVersion, ExpireHook hook) {
        ReentrantLock lock = lockFor();
        lock.lock();
        try {
            TableMetadata meta = loadMeta();
            long base = meta.currentVersion;
            if (expectedBaseVersion != null && expectedBaseVersion != base) {
                log.info("table={} expire REJECTED decision=RETENTION_CONFLICT expected={} actual={}",
                        name, expectedBaseVersion, base);
                throw new TableException(RejectReasonCode.RETENTION_CONFLICT,
                        "expiration expected base version " + expectedBaseVersion
                                + " but current version is " + base + "; nothing was deleted");
            }
            RetentionPolicy policy = retentionPolicy();
            RetentionState oldState = loadRetentionStateOrNull();

            // ----- decide the retained set under a fixed base -----
            List<Long> retained = computeRetainedVersions(base, policy);
            List<Long> previouslyExpired = oldState == null
                    ? new ArrayList<>()
                    : oldState.expired.stream().map(e -> e.version).sorted().toList();

            // Keep-all policy with no prior expiration: nothing to do, nothing to write.
            if (oldState == null && retained.size() == base + 1) {
                log.info("table={} expire decision=NOOP reason=keep-all base={}", name, base);
                return ExpireResult.noop(base, List.copyOf(retained), List.of());
            }

            List<Long> newlyExpired = new ArrayList<>();
            for (long v = 0; v <= base; v++) {
                if (!retained.contains(v) && !previouslyExpired.contains(v)) {
                    newlyExpired.add(v);
                }
            }

            // ----- resume case: a previous run published this exact base AND retained set -----
            if (oldState != null && oldState.gcBaseVersion == base
                    && stateRetainedMatches(oldState, retained)) {
                return finishDeletion(meta, oldState, retained, newlyExpired, previouslyExpired, hook,
                        oldState.gcGeneration, false);
            }

            // ----- materialize every retained version as a standalone full snapshot -----
            long generation = oldState == null ? 1L : oldState.gcGeneration + 1;
            List<RetentionState.Entry> entries = materializeRetained(retained, base, generation);

            // ----- publish the new retention state atomically BEFORE deleting anything -----
            RetentionState newState = new RetentionState();
            newState.gcBaseVersion = base;
            newState.gcGeneration = generation;
            newState.updatedAtMs = System.currentTimeMillis();
            newState.retained = entries;
            newState.expired = new ArrayList<>();
            if (oldState != null) {
                newState.expired.addAll(oldState.expired);
            }
            for (long v : newlyExpired) {
                newState.expired.add(new RetentionState.ExpiredEntry(v, newState.updatedAtMs, generation));
            }
            newState.expired.sort(Comparator.comparingLong(e -> e.version));
            json.writeAtomic(retentionPath(), newState);
            log.info("table={} retention state PUBLISHED generation={} base={} retained={} expiredNow={} previouslyExpired={}",
                    name, generation, base, retained, newlyExpired, previouslyExpired.size());

            ExpireResult published = new ExpireResult(true, base, List.copyOf(retained),
                    List.copyOf(newlyExpired), List.copyOf(previouslyExpired), new ArrayList<>(), 0L);
            hook.afterStatePublished(published);

            return finishDeletion(meta, newState, retained, newlyExpired, previouslyExpired, hook,
                    generation, true);
        } finally {
            lock.unlock();
        }
    }

    private static boolean stateRetainedMatches(RetentionState state, List<Long> retained) {
        if (state.retained.size() != retained.size()) {
            return false;
        }
        List<Long> onDisk = state.retained.stream().map(e -> e.version).sorted().toList();
        return onDisk.equals(retained);
    }

    private List<Long> computeRetainedVersions(long base, RetentionPolicy policy) {
        java.util.Set<Long> keep = new java.util.TreeSet<>();
        keep.add(base); // the current/latest version is always retained
        if (policy.retainLatestVersions() != null) {
            int n = policy.retainLatestVersions();
            long from = Math.max(0L, base - n + 1L);
            for (long v = from; v <= base; v++) {
                keep.add(v);
            }
        } else {
            for (long v = 0; v <= base; v++) {
                keep.add(v);
            }
        }
        keep.addAll(policy.pinnedVersions()); // already validated against the current table
        return new ArrayList<>(keep);
    }

    /**
     * Build one independent full-snapshot data file per retained version, projected
     * with that version's own schema. Replay is incremental: reuse the replayed rows
     * at the lowest retained version, then apply each subsequent commit once.
     */
    private List<RetentionState.Entry> materializeRetained(List<Long> retained, long base, long generation) {
        long min = retained.get(0);
        RetentionState currentState = loadRetentionStateOrNull();
        TreeMap<String, Map<String, Object>> rows =
                replayChain(collectChain(min, currentState), readCommit(min).primaryKey);

        List<RetentionState.Entry> entries = new ArrayList<>();
        long cursor = min;
        for (long target : retained) {
            while (cursor < target) {
                cursor++;
                CommitRecord cr = readCommit(cursor);
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
            CommitRecord targetCommit = readCommit(target);
            Schema targetSchema = targetCommit.toSchema();
            List<Map<String, Object>> snapshot = new ArrayList<>();
            for (Map<String, Object> raw : rows.values()) {
                snapshot.add(rowToOrderedMap(projectForRead(raw, targetSchema)));
            }
            String rel = "data/v" + target + ".retain.v" + generation + ".json";
            json.writeAtomic(tableDir.resolve(rel), snapshot);
            entries.add(new RetentionState.Entry(target, rel,
                    "commits/" + target + ".commit.json"));
        }
        return entries;
    }

    /**
     * Delete only files that no retained version (and no commit newer than the GC
     * base) can still need. Safe to run again: the new state is already on disk, so
     * re-execution simply deletes whatever remains of the garbage set.
     */
    private ExpireResult finishDeletion(TableMetadata meta, RetentionState state, List<Long> retained,
                                        List<Long> newlyExpired, List<Long> previouslyExpired,
                                        ExpireHook hook, long generation, boolean freshPublish) {
        long base = state.gcBaseVersion;
        java.util.Set<String> keepDataFiles = new java.util.HashSet<>();
        for (RetentionState.Entry e : state.retained) {
            keepDataFiles.add(e.snapshotFile);
        }
        // Files produced by commits newer than the GC base are still on the live chain.
        for (long v = base + 1; v <= meta.currentVersion; v++) {
            CommitRecord cr = readCommit(v);
            keepDataFiles.addAll(cr.dataFiles);
            keepDataFiles.addAll(cr.deleteFiles);
        }

        List<String> deleted = new ArrayList<>();
        long bytes = 0L;

        // Garbage data files: any data/ JSON not referenced by the new state or newer commits.
        Path dataDir = tableDir.resolve("data");
        if (Files.isDirectory(dataDir)) {
            List<Path> candidates = new ArrayList<>();
            try (var stream = Files.list(dataDir)) {
                stream.filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().endsWith(".json"))
                        .forEach(candidates::add);
            } catch (IOException e) {
                throw new UncheckedIOException("failed to list data files of " + name, e);
            }
            candidates.sort(Comparator.comparing(p -> p.getFileName().toString()));
            for (Path f : candidates) {
                String rel = "data/" + f.getFileName();
                if (keepDataFiles.contains(rel)) {
                    continue;
                }
                long size = fileSize(f);
                if (deleteQuietly(f)) {
                    deleted.add(rel);
                    bytes += size;
                    hook.afterDeleteStep(rel);
                }
            }
        }

        log.info("table={} expire {} decision=OK generation={} base={} retained={} expiredNow={} filesDeleted={} bytesReclaimed={}",
                name, freshPublish ? "COMMITTED" : "RETRY-CLEANUP", generation, base, retained,
                newlyExpired, deleted.size(), bytes);
        return new ExpireResult(freshPublish, base, List.copyOf(retained),
                List.copyOf(newlyExpired), List.copyOf(previouslyExpired),
                List.copyOf(deleted), bytes);
    }

    private static long fileSize(Path f) {
        try {
            return Files.size(f);
        } catch (IOException e) {
            return 0L;
        }
    }

    /** Delete one file; returns false if it was already gone (safe retry). */
    private static boolean deleteQuietly(Path f) {
        try {
            return Files.deleteIfExists(f);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to delete obsolete file " + f, e);
        }
    }

    // ------------------------------------------------------------- internals

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

    private record PreparedOp(String key, RowOpType type, Map<String, Object> row) {
    }
}
