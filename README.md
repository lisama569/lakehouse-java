# lakehouse-java：本地数据湖表（写入 / 快照 / 时间旅行 / 结构演进 / 整理）

一套**完全离线、仅依赖本地文件系统**的迷你数据湖表实现。不需要任何外部服务、数据库或真实凭据，
用 JSON 文件模拟数据表的数据文件、删除标记与提交清单（commit manifest），在重复操作与并发提交下
给出一致且可解释的结论。

核心代码位于 `src/main/java/com/github/lisama569/lakehousejava/table/`，
测试位于 `src/test/java/com/github/lisama569/lakehousejava/table/`。

---

## 1. 快速开始（离线本地验证）

要求：JDK 21+、Maven（依赖已在本地缓存时可全程离线，加 `-o`）。

```bash
# 1) 跑全部单测（24 个），日志中会打印写入批次、版本号、判定依据
mvn -o test

# 2) 运行端到端演示（建表、写入、拒绝、结构演进、冲突、时间旅行、整理）
mvn -o spring-boot:run -Dspring-boot.run.arguments=demo
# 可指定数据目录：
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
```

---

## 2. 磁盘布局与版本语义

```
<warehouse>/<table>/
  _lakehouse.json                当前元数据（当前版本号 + 当前 schema），临时文件 + 原子 move 替换
  commits/0.commit.json          v0：建表时的空全量快照（full snapshot），所有回溯链的锚点
  commits/1.commit.json          每次成功提交一个不可变清单
  commits/2.commit.json
  data/v1.data.json              该次写入的 upsert 行（delta）
  data/v2.data.json
  data/v2.deletes.json           该次写入的删除主键（delete marker）
  data/v5.compact.data.json      整理产生的全量快照数据文件
```

### 版本与提交

- 版本号从 **0 开始单调递增**。0 是建表时的空全量快照；每次**成功**提交（写入 / 加列 / 类型拓宽 / 整理）恰好 +1。
- 提交清单 `commits/<v>.commit.json` 一经写入**不可变、不删除**，记录：
  父版本、提交类型（`APPEND_WRITE` / `SCHEMA_CHANGE` / `COMPACTION`）、时间戳、该版本 schema 快照、
  数据/删除文件列表、是否全量快照。
- `_lakehouse.json` 只保存“当前指针”。崩溃或读到旧指针也不会破坏历史版本文件。
- `history()` 按版本号返回所有提交信息。

### 快照读取与时间旅行

- 普通写入是 **delta**：读取版本 V 时，从 V 沿 `parentVersion` 向回走到**最近的全量快照**，
  再按版本升序重放：upsert 文件按主键覆盖，delete 文件移除主键。
- 整理（compaction）写出一个**新的全量快照**，使后续回溯只需从该点重放；
  **旧文件一律保留**，所以整理前的每个版本仍按原来的 delta 链解释，结论不变。
- 行结果按主键排序返回，保证重复读取结论稳定、可直接断言。
- 读取不存在的版本抛 `TableException(VERSION_NOT_FOUND)`。

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
| `VERSION_NOT_FOUND` | 读取/打开不存在的版本或表 |
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

---

## 7. 测试覆盖与日志

`mvn -o test` 共 24 个用例：

- `WriteValidationTest`（9）：按主键 upsert/update/delete、同批重复主键收敛、delete 覆盖 upsert、
  四类写入拒绝原因可区分、非法批次无部分生效、重复批次最终状态唯一。
- `SnapshotIsolationTest`（4）：版本可寻址、时间旅行在写入/删除后稳定、`VERSION_NOT_FOUND`、
  过期 base 写入被拒后可重试、8 线程并发仅一赢且最终状态唯一、history 完整。
- `SchemaEvolutionCompactionTest`（8）：加列缺省值对新旧版本可解释、NOT NULL 无缺省拒绝、
  重名/坏缺省区分、`LONG->DOUBLE` 拓宽与反向拒绝、删列禁止、整理后逐版本结论一致、整理冲突、重复整理稳定。
- `PersistenceReopenTest`（1）：丢弃引擎后重新打开同一目录，版本/历史/冲突检测全部保持。
- `EndToEndIntegrationTest`（1）：完整生命周期串联。

单测与演示日志（SLF4J / stdout）会打印：

```
table=users write batch COMMITTED decision=OK base=0 newVersion=1 ops=4 upserts=2 ...
table=users write batch REJECTED decision=VERSION_CONFLICT expectedBase=0 actualBase=1 ops=1
table=ledger compact COMMITTED decision=OK base=4 newVersion=5 liveRows=2 ... (old files retained)
```

即“写入批次内容 / 基准版本 / 新版本号 / upsert/delete/收敛计数 / 判定依据”均可在日志中解释。

---

## 8. 主要类型

| 类型 | 作用 |
| --- | --- |
| `LakehouseEngine` / `LocalLakehouseEngine` | 本地仓库入口：建表、打开表 |
| `TableManager` / `LocalTableManager` | 单表：写入、快照读取、history、加列、改类型、删列、整理 |
| `Schema` / `Column` / `DataType` / `Schemas` | 表结构、类型（STRING/LONG/DOUBLE/BOOLEAN）与校验/强转 |
| `Row` / `RowOp` / `WriteBatch` | 行、upsert/delete 操作、带基准版本的批次 |
| `WriteResult` / `RejectedRow` / `RejectReasonCode` | 提交结果与可区分拒绝原因 |
| `CommitRecord` / `TableMetadata` / `JsonStore` | 提交清单、表元数据与原子 JSON 持久化 |
