# lakehouse-java：本地数据湖表（写入 / 快照 / 时间旅行 / 结构演进 / 整理 / 历史保留与回收）

一套**完全离线、仅依赖本地文件系统**的迷你数据湖表实现。不需要任何外部服务、数据库或真实凭据，
用 JSON 文件模拟数据表的数据文件、删除标记与提交清单（commit manifest），在重复操作与并发提交下
给出一致且可解释的结论。

核心代码位于 `src/main/java/com/github/lisama569/lakehousejava/table/`，
测试位于 `src/test/java/com/github/lisama569/lakehousejava/table/`。

---

## 1. 快速开始（离线本地验证）

要求：JDK 21+、Maven（依赖已在本地缓存时可全程离线，加 `-o`）。

```bash
# 1) 跑全部单测（36 个），日志中会打印写入批次、版本号、判定依据
mvn -o test

# 2) 运行端到端演示（建表、写入、拒绝、结构演进、冲突、时间旅行、整理、保留策略与回收）
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

// 显式历史回收（默认不删任何东西；通常先 compact 产生新的全量锚点）
t.compact(null);                                  // 产生全量快照锚点
t.setRetentionPolicy(RetentionPolicy.of(1, 1L));  // 钉住 v1，其余只保留最新版本
VacuumResult vr = t.vacuum(null);                 // 删除不再需要的数据文件
vr.bytesReclaimed();          // 释放字节数（磁盘占用可观测下降）
vr.expiredVersions();         // 变为不可读的旧版本
t.readSnapshot(2L);           // 抛 VERSION_EXPIRED（区别于从未存在的 VERSION_NOT_FOUND）
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
  vacuums/0.vacuum.json          每次“成功完成”的回收记录（台账，append，不影响回收判定）
```

### 版本与提交

- 版本号从 **0 开始单调递增**。0 是建表时的空全量快照；每次**成功**提交（写入 / 加列 / 类型拓宽 / 整理）恰好 +1。
- 提交清单 `commits/<v>.commit.json` 一经写入**不可变、不删除**，记录：
  父版本、提交类型（`APPEND_WRITE` / `SCHEMA_CHANGE` / `COMPACTION`）、时间戳、该版本 schema 快照、
  数据/删除文件列表、是否全量快照。
- `_lakehouse.json` 只保存“当前指针”（含可选保留策略）。崩溃或读到旧指针也不会破坏历史版本文件。
- `history()` 按版本号返回所有提交信息（包括已被回收的版本：其清单仍在，只是数据不再可读）。
- 写入 / 结构演进 / 整理都**不删除文件**；只有显式 `vacuum` 会按保留策略删除不再需要的**数据文件**，
  提交清单始终保留（详见第 7 节）。

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
| `VERSION_NOT_FOUND` | 读取/打开不存在的版本或表（版本号从未存在） |
| `VERSION_EXPIRED` | 版本曾经存在，但所需数据文件已被显式 `vacuum` 回收，现已不可读 |
| `COMPACTION_CONFLICT` | 整理基于过期的 `expectedBaseVersion` |
| `VACUUM_CONFLICT` | 回收基于过期的当前版本前提（开始前已过期，或删除前发现并发新提交） |
| `RETENTION_POLICY_MISSING` | 未设置保留策略就调用 `vacuum` |
| `RETENTION_POLICY_INVALID` | 保留策略非法（钉住不存在的版本、窗口为负等） |
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

## 7. 历史版本保留与回收（Retention & Vacuum）

默认情况下**任何操作都不删除文件**；回收只在**显式调用 `vacuum(...)` 时**发生，且必须先设置保留策略。

### 7.1 设置保留策略

```java
// 保留最近 3 个版本（最新版本永远隐含保留）
t.setRetentionPolicy(RetentionPolicy.keepRecent(3));

// 长期钉住 v1、v7，同时至少保留最近 2 个版本
t.setRetentionPolicy(RetentionPolicy.of(2, 1L, 7L));

t.retentionPolicy();   // 读取当前策略；从未设置时返回 null（默认：永久保留全部历史）
```

- 策略持久化在 `_lakehouse.json` 中，**不产生新版本、不修改任何数据**，重开表后仍在。
- 钉住的版本号必须已存在（`0..currentVersion`），否则 `RETENTION_POLICY_INVALID`；
  `minVersionsToKeep` 必须 `>= 0`。
- 一个版本在回收时被**保留（仍可读）**，当且仅当它满足任一条件：
  1. 是**当前（最新）版本**；
  2. 在 `pinnedVersions` 中被显式钉住；
  3. 落在“最近 `minVersionsToKeep` 个版本”的窗口内。

### 7.2 回收规则与依赖保护

`vacuum(expectedBaseVersion)` 只删除**数据/删除文件**，提交清单 `commits/*.commit.json` **永远保留**：

- 对每个“要保留的版本”，沿其读取链（最近全量快照锚点 → 该版本的所有 delta）收集其需要的
  全部 `dataFiles/deleteFiles`，这些文件组成**受保护集合**。
- 出现在任意提交清单里、但不在受保护集合中、且磁盘上仍存在的文件，才会被删除。
- 因此：**钉住版本与最新版本回收后逐行结论与回收前完全一致**，不会丢行/多行/结论漂移。
- 一个重要前提：要真正能回收历史，需要先有一个**更晚的全量快照锚点**（即先 `compact(...)`）。
  若没有整理过，所有版本共享 v0 锚点和同一条 delta 链，此时即使策略只保留最新版也**没有可删文件**，
  `vacuum` 返回 `noOp=true`（这是依赖保护的正确结果，而不是 bug）。

```java
long base = t.currentVersion();
VacuumResult r = t.vacuum(base);
r.noOp();               // 没有可回收文件时为 true
r.deletedFiles();       // 本次实际删除的文件（相对路径）
r.bytesReclaimed();     // 本次释放的字节数（>0 即磁盘占用可观测下降）
r.retainedVersions();   // 回收后仍可读的版本
r.expiredVersions();    // 回收后变得不可读的版本
t.vacuumHistory();      // 历轮“成功完成”的回收台账
```

### 7.3 哪些版本会变得不可读、如何与“从未存在”区分

- 被回收的旧版本，其**提交清单仍在**（仍出现在 `history()` 中、`schemaAt(v)` 仍可取其 schema），
  但读取链上的某个数据文件已删除；读取它抛 **`VERSION_EXPIRED`**，错误信息明确说明
  “该版本曾经存在、已被显式回收、缺哪个文件”。
- 一个从来不存在的版本号，没有提交清单，读取抛 **`VERSION_NOT_FOUND`**。
- 二者是**不同的错误码**，调用方可以明确区分“被策略回收”与“版本号不存在”。
- 最新版本与被钉住/窗口内的版本**永远不会**变成 `VERSION_EXPIRED`。

### 7.4 并发前提、失败与重试

- `vacuum(expectedBaseVersion)` 带**当前版本前提**：
  - 进入时若前提已过期，立即抛 `VACUUM_CONFLICT`，不做任何工作、不删任何文件；
  - 计划与删除分两阶段：计划阶段在表锁内完成后释放锁；删除前**重新取锁并再次校验当前版本**。
    若期间有新提交成功（当前版本前进），**整体放弃**，抛 `VACUUM_CONFLICT`，一个文件都不删，
    已成功提交的新版本及所有在保留范围内的历史版本都依旧可读。
  - `expectedBaseVersion` 传 `null` 表示不检查前提（仍串行）。
- **中断安全 / 可重试**：回收只删除“计划时已判定为垃圾”的文件，不依赖删除过程中的状态变化；
  中途崩溃（删了一部分）后用**同一前提**重新执行即可安全收敛——已删文件自动跳过（`deleteIfExists`），
  剩余文件继续删除，台账只在**全部删除成功后**写一条 `completed=true` 记录，
  因此不会重复计数、不会数据丢失，最终可见结论唯一确定。
- **重复清理幂等**：对同一状态再次 `vacuum` 返回 `noOp=true`，磁盘与读取结论保持不变。

> 与写/整理一致，串行化与前提校验主要保证单 JVM 内多线程；跨进程的极端竞争由
> “提交清单原子存在性 + 元数据原子替换 + 删除前重取前提”兜底。

---

## 8. 测试覆盖与日志

`mvn -o test` 共 36 个用例：

- `WriteValidationTest`（9）：按主键 upsert/update/delete、同批重复主键收敛、delete 覆盖 upsert、
  四类写入拒绝原因可区分、非法批次无部分生效、重复批次最终状态唯一。
- `SnapshotIsolationTest`（4）：版本可寻址、时间旅行在写入/删除后稳定、`VERSION_NOT_FOUND`、
  过期 base 写入被拒后可重试、8 线程并发仅一赢且最终状态唯一、history 完整。
- `SchemaEvolutionCompactionTest`（8）：加列缺省值对新旧版本可解释、NOT NULL 无缺省拒绝、
  重名/坏缺省区分、`LONG->DOUBLE` 拓宽与反向拒绝、删列禁止、整理后逐版本结论一致、整理冲突、重复整理稳定。
- `RetentionVacuumTest`（12）：
  - 保留策略持久化/重开仍在、默认行为不变（无策略绝不删除）、无策略回收被拒、非法策略被拒；
  - 回收后钉住版本与最新版本逐行一致、磁盘字节数下降、过期版本与“从未存在”错误码可区分；
  - 钉住版本的整条读取链（锚点 + delta）受依赖保护，未整理时无可删文件（noOp）；
  - 回收期间并发新提交导致前提失效时整体放弃、一个文件不删；过期前提在工作前即被拒；
  - 中断（删到一半崩溃）后以同一前提重试安全收敛，无丢失/重复计数/漂移；
  - 重复回收幂等；回收后新写入 + 再次整理 + 再次回收；策略、过期集合与台账重开后保持。
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

## 9. 主要类型

| 类型 | 作用 |
| --- | --- |
| `LakehouseEngine` / `LocalLakehouseEngine` | 本地仓库入口：建表、打开表 |
| `TableManager` / `LocalTableManager` | 单表：写入、快照读取、history、加列、改类型、删列、整理、保留策略与回收 |
| `Schema` / `Column` / `DataType` / `Schemas` | 表结构、类型（STRING/LONG/DOUBLE/BOOLEAN）与校验/强转 |
| `Row` / `RowOp` / `WriteBatch` | 行、upsert/delete 操作、带基准版本的批次 |
| `WriteResult` / `RejectedRow` / `RejectReasonCode` | 提交结果与可区分拒绝原因 |
| `RetentionPolicy` | 历史保留策略：最近版本窗口 + 显式钉住版本 |
| `VacuumResult` / `VacuumRecord` | 一次回收的结果（删除文件/释放字节/保留与过期版本）与持久化台账 |
| `CommitRecord` / `TableMetadata` / `JsonStore` | 提交清单、表元数据（含保留策略）与原子 JSON 持久化 |
