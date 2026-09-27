# lakehouse-java：本地数据湖表（写入 / 快照 / 时间旅行 / 结构演进 / 整理 / 保留与回收）

一套**完全离线、仅依赖本地文件系统**的迷你数据湖表实现。不需要任何外部服务、数据库或真实凭据，
用 JSON 文件模拟数据表的数据文件、删除标记与提交清单（commit manifest），在重复操作与并发提交下
给出一致且可解释的结论。历史版本默认**全部保留**；只有在显式设置保留策略并手动触发回收时，
才会删除确实不再需要的版本数据。

核心代码位于 `src/main/java/com/github/lisama569/lakehousejava/table/`，
测试位于 `src/test/java/com/github/lisama569/lakehousejava/table/`。

---

## 1. 快速开始（离线本地验证）

要求：JDK 21+、Maven（依赖已在本地缓存时可全程离线，加 `-o`）。

```bash
# 1) 跑全部单测（43 个），日志中会打印写入批次、版本号、判定依据
mvn -o test

# 2) 运行端到端演示（建表、写入、拒绝、结构演进、冲突、时间旅行、整理、保留与回收）
mvn -o spring-boot:run -Dspring-boot.run.arguments=demo
# 可指定数据目录（演示可重复运行，观察 GC 代次递增）：
# mvn -o spring-boot:run -Dspring-boot.run.arguments="demo ./tmp-wh"
```

演示/测试产生的数据就是目录下的普通 JSON，可直接打开检查；演示默认目录 `lakehouse-demo-data/` 已被 git 忽略。

### 最小代码用法

```java
LocalLakehouseEngine engine = new LocalLakehouseEngine(Path.of("./wh"));

Schema schema = Schemas.builder()
        .column("id", DataType.LONG, false, null)
        .column("name", DataType.STRING, true, "unknown")
        .primaryKey("id")
        .build();

TableManager t = engine.createTable("users", schema);

WriteResult r = t.write(new WriteBatch(List.of(
        RowOp.upsert(Row.of("id", 1L, "name", "alice")),
        RowOp.upsert(Row.of("id", 1L, "name", "alice2")),   // 同批重复主键
        RowOp.delete(Row.of("id", 7L)))));

r.committed();              // true
r.newVersion();             // 1
r.duplicateKeysCollapsed(); // 1（同批重复主键按“最后一条生效”收敛）

t.readSnapshot(1L);         // 时间旅行：读指定版本，后续写入/整理后结论不变
t.readSnapshot(null);       // 读最新版本

// 历史保留与回收（默认全部保留；只有显式触发才删除）
t.setRetentionPolicy(RetentionPolicy.of(2, List.of(1L))); // 保留最近2个 + 钉住v1（仅登记，不删除）
ExpireResult gc = t.expireSnapshots(t.currentVersion());  // 显式回收，携带并发版本前提
gc.retainedVersions();      // [1, ..., 最新-1, 最新]
gc.bytesReclaimed();        // 实际回收的字节
t.readSnapshot(2L);         // 被回收的版本 -> VERSION_EXPIRED（区别于 VERSION_NOT_FOUND）
```

---

## 2. 磁盘布局与版本语义

```
<warehouse>/<table>/
  _lakehouse.json                当前元数据（当前版本号 + 当前 schema + 保留策略），临时文件 + 原子 move 替换
  _retention.json                最近一次回收（GC）发布的保留状态：仍可读的版本/物化快照、已过期版本台账
  commits/0.commit.json          v0：建表时的空全量快照（full snapshot），所有回溯链的锚点
  commits/1.commit.json          每次成功提交一个不可变清单（回收后仍保留，供 history / schema 查询）
  commits/2.commit.json
  data/v1.data.json              该次写入的 upsert 行（delta）
  data/v2.data.json
  data/v2.deletes.json           该次写入的删除主键（delete marker）
  data/v5.compact.data.json      整理产生的全量快照数据文件
  data/v4.retain.v1.json         回收时为仍保留版本 v4 物化出的独立全量快照（.retain.v<GC代>）
```

### 版本与提交

- 版本号从 **0 开始单调递增**。0 是建表时的空全量快照；每次**成功**提交（写入 / 加列 / 类型拓宽 / 整理）恰好 +1。
- 提交清单 `commits/<v>.commit.json` 一经写入**不可变、不删除**，记录：
  父版本、提交类型（`APPEND_WRITE` / `SCHEMA_CHANGE` / `COMPACTION`）、时间戳、该版本 schema 快照、
  数据/删除文件列表、是否全量快照。
- `_lakehouse.json` 只保存“当前指针”。崩溃或读到旧指针也不会破坏历史版本文件。
- `history()` 按版本号返回所有提交信息。

### 快照读取与时间旅行

- 普通写入是 **delta**：读取版本 V 时，从 V 沿 `parentVersion` 向回走到**最近的全量快照**
  （整理产生的 `COMPACTION` 快照，或回收时为保留版本物化出的 `*.retain.*` 快照），
  再按版本升序重放：upsert 文件按主键覆盖，delete 文件移除主键。
- 整理（compaction）写出一个**新的全量快照**，使后续回溯只需从该点重放；
  **旧文件一律保留**，所以整理前的每个版本仍按原来的 delta 链解释，结论不变。
- 回收（见第 7 节）会为每个仍保留的版本物化一份独立全量快照，并删除其原始 delta；
  该版本之后的读取直接从物化快照开始，结论与回收前**逐行一致**。
- 行结果按主键排序返回，保证重复读取结论稳定、可直接断言。
- 读取不存在的版本抛 `TableException(VERSION_NOT_FOUND)`；读取**曾经存在、但已被回收**的版本抛
  `TableException(VERSION_EXPIRED)`（信息里带导致过期的 GC 代次），两者可区分——
  “被回收不可读”不等于“这个版本从来没存在过”。

### 每个版本用各自的 schema 解释

提交清单内嵌 schema 快照：

- 读版本 V 使用 **V 当时的 schema**：V 之后新增的列不会出现在 V 的行里；V 当时为 `LONG` 的列，
  即使后来拓宽为 `DOUBLE`，在 V 中仍按 `LONG` 读出。
- 读**当前**数据时，对建表后新增的列，旧行按该列声明的**确定性缺省值**补齐。

---

## 3. 写入规则与拒绝原因

一个批次（`WriteBatch`）内所有 op 先做校验：

- **同批重复主键确定性收敛**：按批次顺序，同一主键的多个 op 以**最后一条**为准
  （后出现的 delete 会覆盖先出现的 upsert）。`WriteResult.duplicateKeysCollapsed()` 报告收敛数量。
- **原子性**：批次中只要有一个 op 非法，整批拒绝，不写任何文件、不产生新版本、无部分生效。
  每个非法 op 独立校验并返回其在批次中的下标与原因。

可区分的拒绝原因（`RejectReasonCode`，存在于 `WriteResult.rejections()` 或 `TableException.code()`）：

| 代码 | 触发条件 |
| --- | --- |
| `MISSING_PRIMARY_KEY` | upsert/delete 的行缺少任一主键列，或主键值为 null |
| `TYPE_MISMATCH` | 值无法按目标列类型解释（数字串会尝试解析，布尔仅接受 `true/false`） |
| `UNKNOWN_COLUMN` | 行里出现 schema 中不存在的列（整列缺失的多余字段不静默丢弃） |
| `MISSING_REQUIRED_VALUE` | 非空列既无值也没有可用的缺省值 |
| `SCHEMA_EVOLUTION_INCOMPATIBLE` | 不兼容的类型变更、NOT NULL 新列无缺省值、缺省值与类型不符、未知列 |
| `SCHEMA_COLUMN_DUPLICATE` | 新增列重名（建表时重名列同样拒绝） |
| `SCHEMA_COLUMN_DROP_FORBIDDEN` | 删除既有列（一律拒绝，主键列也不允许） |
| `VERSION_CONFLICT` | 写入基于过期的 `expectedBaseVersion`（乐观锁失败） |
| `VERSION_NOT_FOUND` | 读取/打开不存在的版本或表（从未存在过） |
| `VERSION_EXPIRED` | 读取一个**曾经存在、但已被回收**的版本（区别于从未存在） |
| `RETENTION_POLICY_INVALID` | 保留策略非法：保留数量 < 1、钉住不存在/负数/已过期的版本 |
| `RETENTION_CONFLICT` | 回收基于过期的 `expectedBaseVersion`，期间有新提交成功，整体放弃、不删除任何文件 |
| `COMPACTION_CONFLICT` | 整理基于过期的 `expectedBaseVersion` |
| `EMPTY_TABLE_NAME` | 表名为空或含非法字符 |

upsert 语义：主键存在则更新，不存在则插入。写入时按 schema 列顺序投影，缺省值在写入与读取时都确定。

---

## 4. 结构演进兼容规则

- **新增列**：允许。必须声明类型；若为 `NOT NULL` 必须同时给出非空缺省值（否则拒绝）。
  缺省值必须与类型匹配。新列对旧版本不可见，对当前版本的旧行用缺省值解释——新旧版本都能解释。
- **类型变更**：仅允许**拓宽**，目前唯一支持 `LONG -> DOUBLE`。
  其余变更（如 `STRING -> LONG`、`DOUBLE -> BOOLEAN`、反向收窄）一律 `SCHEMA_EVOLUTION_INCOMPATIBLE`。
  类型相同时为无操作，返回当前版本号。
- **删除列**：**始终拒绝**（`SCHEMA_COLUMN_DROP_FORBIDDEN`），以保证所有历史版本仍可解释。
- 结构变更本身也是一次带新版本号的提交，内嵌变更后 schema；不重写任何历史数据文件。

---

## 5. 并发与冲突处理（快照隔离）

- 写入支持乐观并发控制：`new WriteBatch(ops, expectedBaseVersion)`。
  提交时若 `expectedBaseVersion != 当前版本`，直接抛 `VERSION_CONFLICT`，**不静默覆盖、不部分生效**，
  版本号不前进；调用方应以新版本重读并重试（测试演示了“冲突 -> 用新 base 重试 -> 成功”）。
  `expectedBaseVersion` 传 `null` 表示不做前置版本检查（仍串行提交）。
- 同一 JVM 内，针对同一张表目录使用按表加锁（`ReentrantLock`）将提交串行化，
  提交清单以“版本号文件已存在即冲突”的原子方式落盘；元数据用临时文件 + 原子 move 更新。
  因此多线程并发提交结果唯一确定：同一过期 base 上只有一个提交能赢，其余全部得到明确冲突。
- 整理同样接受 `expectedBaseVersion`，过期则 `COMPACTION_CONFLICT`。
- 隔离级别语义：每个成功提交是一个不可变版本；读版本 V 在任何后续写入/整理后都返回同一结论
  （快照隔离 / 时间旅行）。

> 说明：按表锁是单 JVM 内的保证；跨进程的极端竞争由“提交文件原子存在性检查 + 原子元数据替换”兜底，
> 本仓库的并发测试覆盖 JVM 内多线程场景。

---

## 6. 存储整理（Compaction）

- `compact(expectedBaseVersion)` 重放当前表到最新版本，把全部存活行写成一个新的全量快照数据文件，
  并产生一个 `COMPACTION` 新版本。
- 只**新增**文件、不修改/删除历史文件：
  - 最新版本的读取结论与整理前逐行相等；
  - 所有历史版本读取结论与整理前完全一致（测试逐版本比对）；
  - 重复整理结果稳定，不出现数据丢失、重复计数或结论漂移。
- 整理可重复执行；基于过期版本的整理被明确拒绝。
- 回收（compaction 之后）也能正常工作：整理与回收都通过同一条“走到最近全量/物化快照再重放”的链路读取，
  回收后整理仍逐行保持当前内容。

---

## 7. 保留策略与历史回收（Retention & GC，显式触发）

**默认行为不变**：不设置策略、不调用回收时，任何文件都不会被删除（与旧版本完全一致）。
回收只在显式调用 `expireSnapshots(...)` 时发生。

### 7.1 设置保留策略（只登记意图，不删任何东西）

```java
// 只保留最近 2 个版本
t.setRetentionPolicy(RetentionPolicy.retainLatest(2));
// 保留最近 2 个版本，并长期钉住 v1
t.setRetentionPolicy(RetentionPolicy.of(2, List.of(1L)));
// 不按数量裁剪，只额外钉住某些版本
t.setRetentionPolicy(RetentionPolicy.pin(List.of(1L, 3L)));
// 恢复“全部保留”
t.setRetentionPolicy(null); // 或 RetentionPolicy.keepAll()
```

- `retainLatestVersions`：保留最近 N 个版本，**N 必须 ≥ 1**；为 `null` 表示不按数量裁剪。
- `pinnedVersions`：显式**钉住**、需要长期保留的版本；自动去重并排序。
  - 钉住的版本必须真实存在（不能是未来版本或负数），且**不能是已经被回收的版本**，否则
    `RETENTION_POLICY_INVALID`。
- 最新（current）版本**始终保留**。
- 设置策略只是把意图原子写入 `_lakehouse.json`，**不产生删除、不让任何版本变得不可读**。

### 7.2 触发回收：`expireSnapshots(expectedBaseVersion)`

```java
ExpireResult r = t.expireSnapshots(5L); // 基于“当前=5”这一前提执行
r.committed();            // 是否发布了新的保留状态（重试只做剩余清理时为 false）
r.baseVersion();          // 本次回收依据的当前版本
r.retainedVersions();     // 回收后仍可读的版本（钉住 ∪ 最近 N 个 ∪ 最新）
r.expiredVersions();      // 本次新变为不可读的版本
r.previouslyExpired();    // 之前就已不可读的版本
r.filesDeleted();         // 实际删除的文件
r.bytesReclaimed();       // 实际回收的字节数（被删文件大小之和）
```

回收做了什么（固定在一个 base 版本上计算，整个过程持表锁）：

1. **决定保留集**：`{最新版本} ∪ {最近 N 个版本} ∪ {钉住的版本}`；其余版本标记为“过期”。
2. **物化保护（dependency protection）**：为每个仍保留的版本，用**该版本自己当时的 schema**
   重放出一份独立全量快照 `data/v<R>.retain.v<代>.json`。因此即使该版本原本依赖的中间 delta
   被删除，它仍然可以独立读出，且结论与回收前**逐行一致**（测试逐版本比对，不丢行、不多行、不漂移）。
3. **先发布、后删除**：把新的保留状态（保留集 + 物化快照位置 + 累积的过期台账）原子写入
   `_retention.json`，**然后**才删除物理文件。删除集合只包含“新保留集与 base 之后的新提交都不再引用”
   的数据文件；上一代物化快照、过期版本的原始 delta/delete 文件都会被回收。
4. 提交清单 `commits/<v>.commit.json` **始终保留**：`history()` 仍列出每个版本，过期版本带
   `expired=true` 标记，其 schema 仍可解释；被删的只是数据/删除文件本身。

### 7.3 哪些版本在什么情况下变得不可读

- 版本 V 在一次成功回收后变得不可读，当且仅当：`V ≤ 本次 base` 且 V **既不在最近 N 个版本里，
  也没有被钉住，也不是最新版本**。
- 之后再读 V 抛 `VERSION_EXPIRED`（消息含 GC 代次），与读取一个**从未存在**的版本得到的
  `VERSION_NOT_FOUND` 明确区分。
- base 之后新提交产生的版本与文件**绝不会**被本次回收删除（回收只处理 `≤ base` 的文件，
  新提交在锁上严格发生在回收之前或之后）。
- 一旦某版本被回收即不可恢复，也不能再被钉住（防止给出“已钉住但读不出来”的假承诺）。
- 钉住一个旧 schema 下的版本：该版本按它**自己当时的 schema** 读出（之后新增的列不出现），
  结构演进 + 回收交错也不受影响。

### 7.4 并发控制：带版本前提，失效即整体放弃

- `expireSnapshots(expectedBaseVersion)` 携带“我认为当前版本是 X”的前提。执行时持表锁重新读取，
  若期间有新的写入/演进/整理提交成功导致当前版本 ≠ X，直接抛 `RETENTION_CONFLICT`：
  **不物化、不发布、不删除任何文件**，不会删到一半，也不会让某个已成功提交、仍在保留范围内的版本变不可读。
  调用方应以新版本重新计算保留集后重试。
- 传 `null` 表示不做前置版本检查（仍在表锁内串行执行）。
- 回收与写入共用按表 `ReentrantLock`：同一 JVM 内二者严格串行；并发测试验证了“写入先赢则回收得到
  明确冲突，回收先赢则已提交的当前版本一定仍可读”。

### 7.5 中断与重试：安全、结论唯一

回收采用“**先原子发布状态，再删除文件**”的顺序，因此在任意时刻崩溃都安全：

| 崩溃点 | 磁盘上的状态 | 重新执行 `expireSnapshots` 的结果 |
| --- | --- | --- |
| 发布状态之前（如前提冲突） | 完全没有改动 | 所有版本照常可读，无垃圾 |
| 状态已发布、尚未删除 | 新状态已生效，旧文件还在（多占空间但无害） | 识别为同一 base+保留集，只补做剩余删除，不重新发布 |
| 删除到一半 | 部分垃圾已删、部分还在 | 同上，`deleteIfExists` 幂等，最终删除集合唯一 |

- 重复回收：同一 base、同一策略下再次执行是**幂等 no-op**（`committed=false`、`filesDeleted=0`），
  保留版本的读取结论不变，无重复计数。
- 策略收紧但没有新提交（base 相同）时，再执行会按新保留集发布新一代物化快照并回收新过期的版本；
  策略放宽不会让已过期版本复活（其数据已删除）。
- 回收之后继续写入：新版本以最新保留版本的物化快照为锚点做 delta 重放，结论正确；
  再次回收会重新物化并保持结论一致。
- 为可验证中断恢复，代码提供了包内可见的崩溃注入钩子 `ExpireHook`
  （`failAfterPublish()` / `failAfterDeletes(n)`），仅测试使用。

### 7.6 磁盘占用可观察下降

`ExpireResult.bytesReclaimed()` 给出本次删除文件的字节总和；测试同时对整个表目录前后做
`Files.walk` 求和，断言回收后**总磁盘占用确实下降**（用大块 payload 放大旧 delta，使回收量
明显大于新写的物化快照）。

---

## 8. 测试覆盖与日志

`mvn -o test` 共 43 个用例：

- `WriteValidationTest`（9）：按主键 upsert/update/delete、同批重复主键收敛、delete 覆盖 upsert、
  四类写入拒绝原因可区分、非法批次无部分生效、重复批次最终状态唯一。
- `SnapshotIsolationTest`（4）：版本可寻址、时间旅行在写入/删除后稳定、`VERSION_NOT_FOUND`、
  过期 base 写入被拒后可重试、8 线程并发仅一赢且最终状态唯一、history 完整。
- `SchemaEvolutionCompactionTest`（8）：加列缺省值对新旧版本可解释、NOT NULL 无缺省拒绝、
  重名/坏缺省区分、`LONG->DOUBLE` 拓宽与反向拒绝、删列禁止、整理后逐版本结论一致、整理冲突、重复整理稳定。
- `PersistenceReopenTest`（1）：丢弃引擎后重新打开同一目录，版本/历史/冲突检测全部保持。
- `EndToEndIntegrationTest`（1）：完整生命周期串联。
- `RetentionPolicyTest`（6）：默认全部保留且“只设策略不删除”、保留数量校验、钉住版本必须存在、
  不能钉住已过期版本、策略持久化与重置、默认 keep-all 下回收为 no-op。
- `RetentionExpirationTest`（13）：
  - **依赖保护**：保留版本（含钉住）回收后逐行结论一致、过期版本不可读且与“从未存在”区分、history 标记；
  - **并发交错**：过期 base 回收整体 `RETENTION_CONFLICT` 且零删除、回收与写入并发互不破坏；
  - **中断重试**：发布后崩溃 / 删除中途崩溃后重试收敛、发布前冲突无副作用、无孤儿快照或临时文件；
  - **重复回收**幂等、无重复计数；
  - 回收后继续写入 / 整理 / 结构演进（钉住旧 schema 版本）结论正确；
  - 重新打开目录后保留/过期状态保持；
  - 大块历史下回收后**整目录磁盘占用下降**。

单测与演示日志（SLF4J / stdout）会打印：

```
table=users write batch COMMITTED decision=OK base=0 newVersion=1 ops=4 upserts=2 ...
table=users write batch REJECTED decision=VERSION_CONFLICT expectedBase=0 actualBase=1 ops=1
table=ledger compact COMMITTED decision=OK base=4 newVersion=5 liveRows=2 ... (old files retained)
table=t retention policy SET retainLatest=2 pinned=[1]
table=t retention state PUBLISHED generation=1 base=6 retained=[2, 5, 6] expiredNow=[0, 1, 3, 4] ...
table=t expire COMMITTED decision=OK generation=1 base=6 ... filesDeleted=7 bytesReclaimed=1234
table=t expire RETRY-CLEANUP decision=OK ... filesDeleted=3 ...
table=t expire REJECTED decision=RETENTION_CONFLICT expected=4 actual=5
```

即“写入批次内容 / 基准版本 / 新版本号 / upsert/delete/收敛计数 / 保留集 / 过期集 / 删除文件数与
回收字节数 / 判定依据”均可在日志中解释。

---

## 9. 主要类型

| 类型 | 作用 |
| --- | --- |
| `LakehouseEngine` / `LocalLakehouseEngine` | 本地仓库入口：建表、打开表 |
| `TableManager` / `LocalTableManager` | 单表：写入、快照读取、history、加列、改类型、删列、整理、保留策略与回收 |
| `RetentionPolicy` | 保留策略：最近 N 个版本 + 钉住版本（`retainLatest/of/pin/keepAll`） |
| `ExpireResult` | 一次回收的结论：保留/过期版本、删除文件、回收字节、是否新发布 |
| `RetentionState` | `_retention.json`：保留版本→物化快照映射与累积过期台账 |
| `ExpireHook` | 仅测试用的崩溃注入钩子（发布后 / 删除到第 N 个文件时中断） |
| `Schema` / `Column` / `DataType` / `Schemas` | 表结构、类型（STRING/LONG/DOUBLE/BOOLEAN）与校验/强转 |
| `Row` / `RowOp` / `WriteBatch` | 行、upsert/delete 操作、带基准版本的批次 |
| `WriteResult` / `RejectedRow` / `RejectReasonCode` | 提交结果与可区分拒绝原因 |
| `CommitRecord` / `CommitInfo` / `TableMetadata` / `JsonStore` | 提交清单（含过期标记）、表元数据与原子 JSON 持久化 |

---

## 10. 本地验证方法

```bash
# 全部单测（43 个），离线可复现
mvn -o test

# 端到端演示：建表、写入、拒绝、结构演进、冲突、时间旅行、整理、保留与回收
mvn -o spring-boot:run -Dspring-boot.run.arguments="demo"
# 指定数据目录，并可重复运行（演示对重复运行幂等；可观察 GC 代次 gcGeneration 递增）：
mvn -o spring-boot:run -Dspring-boot.run.arguments="demo ./tmp-wh"
mvn -o spring-boot:run -Dspring-boot.run.arguments="demo ./tmp-wh"
```

回收后可直接打开检查：

- `_lakehouse.json`：当前版本、schema 与保留策略（`retainLatestVersions` / `pinnedVersions`）。
- `_retention.json`：`gcBaseVersion`、`gcGeneration`、`retained[].snapshotFile` 与累积 `expired[]` 台账。
- `commits/`：提交清单始终都在（history 可查，过期版本带标记）。
- `data/`：只剩“保留版本的物化快照（`*.retain.v*`）”与“base 之后新提交”的数据文件。

