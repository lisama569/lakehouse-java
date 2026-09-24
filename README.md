# lakehouse-java-bootstrap：本地数据湖表（写入 / 快照 / 时间旅行 / 结构演进 / 整理）

用**纯本地文件系统**模拟一张数据湖表，不依赖任何外部服务或真实凭据。
支持按主键的插入/更新/删除、快照与时间旅行读取、乐观并发的快照隔离、
表结构演进与存储整理。整条链路离线可复现。

- Java 21 语法，Spring Boot 3.4 / JUnit 5 / Jackson（仅用其本地 JSON 能力，不起服务）
- 存储根目录：任意本地路径（测试用临时目录，演示默认 `./lakehouse-demo`）

## 1. 目录与存储布局

```
<warehouse>/
  <table>/
    metadata.json              # 表名、主键、currentVersion、nextSchemaId、全部历史 schema
    snapshots/
      0000000000000000001.json # 每个成功提交一个不可变全量快照（零填充文件名 => 字典序即版本序）
      0000000000000000002.json
    compaction/
      compaction-<v>-<ts>.json # 每次整理一条记录：整理前后每个版本文件的 SHA-256
```

关键实现：

| 类 | 职责 |
| --- | --- |
| `LocalLakehouse` | 本地 warehouse 管理：建表（幂等）/加载/列出 |
| `LocalTable(Impl)` | 写入、时间旅行、加列、整理；单表一把 `ReentrantLock` 串行化提交 |
| `WriteValidator` | 提交前整批校验，一次性产出可区分的拒绝原因 |
| `BatchMerger` | 批次内重复主键的确定性收敛与应用 |
| `LocalFileIO` | Jackson JSON 读写；**临时文件 + 原子 move**，不留半成品 |

## 2. 快照与版本语义

1. **版本从 1 开始单调递增**；刚建表 `currentVersion=0`，表示尚无数据版本（读取为空）。
2. **每次成功提交（写入或加列）恰好产生一个新版本**，写入一个不可变快照文件。
   快照内容是该提交完成时刻的**全量存活行**（按主键升序），因此：
   - 按任意历史版本读取（时间旅行 `readAt(version)`）都只依赖该不可变文件；
   - 后续写入、加列、整理之后，旧版本结果逐行稳定、可复现。
3. 写入与加列都**先落盘新版本快照，再原子更新 `metadata.json` 指针**。
   崩溃时要么指针未动（新版本不可见），要么两部分都已就绪；不会出现"指针指向半个版本"。
4. 行按 schema 列顺序规范化、数值归一化（JSON 读回的小整数 `Integer` 会归一化为 `Long` 等），
   保证"写进去 -> 落盘 -> 重新打开进程读回来"的结论字节级、类型级一致。
5. 查询接口返回的是**防御性拷贝**，调用方篡改返回结果不会污染已提交快照。

时间旅行示例：

```java
table.readAt(1);   // v1 的历史结论，永远不变
table.readLatest();
table.listSnapshots(); // 版本号 / schemaId / 行数 / 操作 / 判定依据
```

## 3. 批次写入与重复主键收敛规则

一次写入 = `WriteRequest(expectedBaseVersion, 有序操作列表)`，操作类型 `UPSERT` / `DELETE`。

同批次重复主键按**确定性规则**收敛，结果只取决于输入顺序：

1. 按输入顺序遍历，同一主键**只保留最后一条操作（last-op-wins）**；
2. 因此同一主键同时出现 UPSERT 与 DELETE 时，**排在最后的一条生效**
   （DELETE 在最后则最终删除，UPSERT 在最后则最终写入）；
3. 收敛后再按主键排序、统一应用到基线，得到新版本全量存活行。

相同输入在相同基线上重放，最终状态唯一确定；提交结果里带有
`inputRowCount / effectiveRowCount / duplicatesCollapsed` 与文字判定依据。

### 写入拒绝原因（可区分，整批拒绝、绝不部分生效、绝不静默丢弃）

| RejectCode | 触发条件 |
| --- | --- |
| `EMPTY_BATCH` | 批次为空 |
| `PRIMARY_KEY_MISSING` | 某行缺少主键列，或主键值为 null / 空串 |
| `TYPE_MISMATCH` | 某字段取值与列声明类型不匹配（如 STRING 列给整数） |
| `UNKNOWN_COLUMN` | 某行出现当前 schema 中不存在的列 |
| `MISSING_COLUMN` | 某非主键列在整批**所有 UPSERT 行**中均未提供 |

- 校验在提交前一次性完成，同一批次的多个问题会**一起报告**（含行号、列名、细节），
  任一条失败则整批拒绝，`currentVersion` 不变、不产生快照文件。
- 纯 DELETE 批次只需要主键，不触发 `MISSING_COLUMN`。
- 支持的类型：`STRING / LONG / DOUBLE / BOOLEAN`。

## 4. 结构演进与兼容规则

- 唯一支持的演进是**追加新列**：`addColumn(ColumnDef, expectedBaseVersion)`。
- 新列**必须提供非 null 的确定缺省值**，且缺省值类型必须与列类型兼容。
  加列会：
  1. 生成新 schema（schemaId 递增），**旧 schema 原样保留**，旧版本继续按旧 schema 解释
     （旧版本快照里没有新列，读旧版本时该列不出现，而不是被伪造数据）；
  2. 产生一个新版本快照，历史行在新版本中被回填为该缺省值；
  3. 之后的新写入必须遵循新 schema（整列缺失规则同样适用）。
- **明确拒绝**的演进（抛 `SchemaEvolutionException`，不产生版本）：
  - 删除既有列（不提供删除入口，防止历史版本无法解释）；
  - 对既有列改名或改型（对已存在列名再次 `addColumn` 即拒绝）；
  - 缺省值缺失或缺省值与类型不匹配。
- 类型放宽规则仅有一条可解释规则：`LONG -> DOUBLE` 视为兼容放宽，
  其余类型变更（含 `DOUBLE -> LONG`、与 `STRING` 互转等）均不兼容。
  当前入口只允许加列，该规则集中在 `DataType#isWideningTo`，便于审计与扩展。
- 加列与普通写入一样受快照隔离保护：基准版本过期同样抛 `ConflictException`。

## 5. 并发冲突处理（快照隔离 + 乐观并发控制）

- 每个提交（写入/加列）都必须携带客户端所基于的 `expectedBaseVersion`。
- 提交临界区内先比对：`expectedBaseVersion != metadata.currentVersion` 即抛
  **`ConflictException`（含期望版本与当前版本）**，明确拒绝；
  不会静默覆盖，也不会部分生效。
- 同一 JVM 内对同一张表，一把可重入锁把提交串行化；版本号分配、快照落盘、
  metadata 指针更新在同一临界区完成，因此版本号无空洞、无重复。
- 典型用法是失败后重新读最新版本再重试（OCC）。测试 `ConcurrentCommitTest` 验证：
  N 个线程同基于 v0 提交时**恰好 1 个成功、其余全部冲突**；全部线程带重试后，
  最终版本数与行数唯一确定（无丢失、无重复计数）。
- 说明：本实现的锁保证单 JVM 多线程隔离；跨进程并发不在范围内
  （本地模拟、单进程演示/测试场景），但过期版本写入在任何场景都会被拒绝。

## 6. 存储整理（compaction）

`compact()` **不产生新版本、不改变 currentVersion**，只做规范化文件维护：

- 逐个版本重写快照文件：行按主键排序、行内列顺序按该版本 schema 排列；
- 重写前后逐行逐列做**语义一致性校验**，行数或任一值漂移立即抛异常并中止；
- 每次整理写一条 `compaction/*.json` 日志，记录每个版本文件整理前后的 SHA-256；
- 规范化是幂等的：再次整理校验和不再变化。

因此整理后任意历史版本的查询结论与整理前完全一致：不丢数据、不重复计数、不漂移。

## 7. 本地验证方法（全程离线）

```bash
# 1) 编译
mvn -o compile

# 2) 全量单测（覆盖重复写入、拒绝原因、快照时间旅行、并发冲突、结构演进、整理）
mvn -o test

# 只跑某一类场景
mvn -o test -Dtest=DuplicateWriteTest
mvn -o test -Dtest=RejectionReasonTest
mvn -o test -Dtest=ConcurrentCommitTest
mvn -o test -Dtest=SchemaEvolutionTest
mvn -o test -Dtest=CompactionTest
mvn -o test -Dtest=SnapshotTimeTravelTest
```

测试日志会打印**写入批次、版本号（v1/v2…）、去重条数、拒绝原因码、
冲突时的期望/当前版本、整理前后校验和**等判定依据（见 `LocalTableImpl` 与各测试类的 SLF4J 输出）。

### 端到端演示（不依赖测试框架）

```bash
mvn -o compile
CP="target/classes:$(find ~/.m2/repository -name 'jackson-databind-*.jar'|head -1):\
$(find ~/.m2/repository -name 'jackson-core-*.jar'|head -1):\
$(find ~/.m2/repository -name 'jackson-annotations-*.jar'|head -1):\
$(find ~/.m2/repository -name 'jackson-datatype-jsr310-*.jar'|grep -v sources|head -1):\
$(find ~/.m2/repository -name 'slf4j-api-*.jar'|grep -v sources|head -1)"
java -cp "$CP" com.github.lisama569.lakehousejava.demo.LakehouseDemo /tmp/lakehouse-demo
```

演示顺序：重复主键收敛 → 删除/插入 → v1/v2 时间旅行 → 过期版本被拒绝 →
三类非法写入被可区分拒绝 → 加列回填缺省值 → 整理并验证旧版本不变。
也可以在 IDE 中直接运行 `com.github.lisama569.lakehousejava.demo.LakehouseDemo`。

## 8. 场景到测试的对照表

| 需求场景 | 测试类 |
| --- | --- |
| 同批次重复主键确定性收敛、删除/写入先后判定、重复执行状态唯一 | `DuplicateWriteTest` |
| 主键缺失 / 类型不匹配 / 整列缺失 / 未知列 / 空批次，可区分且整批拒绝 | `RejectionReasonTest` |
| 版本可寻址、时间旅行稳定、快照防篡改、重开进程离线复现 | `SnapshotTimeTravelTest` |
| 过期版本拒绝、并发仅一个成功、失败重试后最终状态唯一 | `ConcurrentCommitTest` |
| 加列缺省值确定、旧版本按旧 schema 解释、不兼容变更拒绝 | `SchemaEvolutionTest` |
| 整理不产生新版本、各版本结论不变、重复整理幂等 | `CompactionTest` |
