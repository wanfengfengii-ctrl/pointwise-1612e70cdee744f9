# 计量结算数仓：重构后职责与依赖边界说明

本轮只调整职责与依赖边界，不新增业务能力。原 `warehouse.py` 中混在一起的版本判定、
失效传播、两级关联、聚合、回执与事务，被拆成无 I/O 的领域层、只依赖仓储端口的应用层，
以及两个可互换的适配器。所有既有断言、样例、旧库格式与 CLI 入口保持不变。

## 分层与依赖方向

依赖只能由外向内：`warehouse`（兼容门面 / CLI）→ `reconcile.application`
→ `reconcile.domain` 与 `reconcile.ports`；适配器实现 `ports`，不被领域或应用具体依赖。

| 模块 | 唯一职责 | 不允许承载的内容 |
| --- | --- | --- |
| `reconcile/domain.py` | 纯领域计算：`normalize` 校验与 canonical 身份、`revise` 修订选择与闭包种子、`associate_first/second` 两路多重集全外关联、`rollup` NULL 感知分组聚合、`delta` 正负变更、token 与全部排序规则 | 任何 I/O、连接、仓储对象或回调；只接收/返回普通内建值 |
| `reconcile/ports.py` | 仓储协议（方法签名与 IS 语义约定），不含实现 | 业务规则、具体存储 |
| `reconcile/application.py` | `Engine(repository)`：事务边界、端口调用顺序、纯计划解释、五个故障点位置、回执/代次流程 | 不实现关联/聚合/修订规则，不引用适配器私有属性，不直接导入 sqlite |
| `reconcile/adapters/sqlite.py` | 原表/列/索引布局、SAVEPOINT 嵌套事务、单表索引行存取、JSON 编解码 | JOIN/UNION/GROUP BY/聚合函数/UDF/触发器/视图，以及任何关联、聚合、修订规则 |
| `reconcile/adapters/memory.py` | 同一端口的内存实现，快照式嵌套提交/回滚，IS 语义索引，深拷贝隔离 | 不导入 sqlite 或旧 `Store`，不执行任何领域规则 |
| `warehouse.py` | 仅做构造装配与兼容转发（`Store` 方法体只有一条 return），保留 `conn`、CLI、`Error`、`FAULTS`、`canonical` | 不保留备用执行器，不写业务规则 |

包初始化文件 `reconcile/__init__.py`、`reconcile/adapters/__init__.py` 均为空，
业务不藏入初始化文件，也不使用动态导入或读源码执行。

## 行为兼容性要点

- **修订与墓碑**：每个 `(source,id)` 只保留最高 rev；低版本忽略、同版本同内容忽略、
  同版本异内容 `REVISION_CONFLICT`；高版本可换键、换路由、删除或复活，墓碑保留。
- **两级关联**：第一层 a×b 按 `k`、第二层 j1×c 按 `route` 的多重集全外关联，
  NULL 不与 NULL 等值匹配，空串正常匹配；无匹配保留左/右孤行，缺失列为 null。
  关联在领域内完成，适配器只存取已算好的行与 token。
- **局部失效闭包**：K 取被接受新版本涉及的 a/b 新旧存活行的 `k`；R 取 c 新旧存活
  `route` 加 K 桶 j1 的 `route`；G 取 R 桶 j2 的 `(day,region)`。NULL 作为一个索引桶
  整体重算但不参与等值匹配。闭包外的数据不读取、不全量重建、不借 id 查询枚举整表；
  同批换键/换路由与交换均以同批最终状态重算，避免按旧状态错账。
- **NULL 桶两趟语义**：第一趟先收割受影响桶（含 NULL 桶）既有 j1 的 route 并清空，
  使移出某键的孤行旧 route 仍进入闭包；第二趟按最终源行重建并再次收割，与原库一致。
- **聚合与 delta**：`n/valued/total/samples` 含 NULL 分组、全空 total=null、零值组保留；
  delta 只输出 token 相同但列变化或消失/新增的行，按 `(canonical(row),weight)` 排序，
  不泄漏中间失配行。
- **回执与代次**：canonical changes 字节相同的回放原样返回首次结果，不比较
  expected_generation、不触发故障、不改数据；不同则 `BATCH_CONFLICT`；代次不符
  `GENERATION_CONFLICT`。每个首次成功批次（含空批/全过期/全相同）代次加一。
- **事务与故障**：五个故障点 `after_sources/after_first/after_second/after_rollup/
  after_receipt` 位置与 `INJECTED:` 语义保持；嵌套事务内层失败只回滚内层、外层回滚
  撤销已返回内层回执与全部派生结果；读在事务内见本事务结果。SQLite 用
  `BEGIN IMMEDIATE` + SAVEPOINT，竞争写入仍在提交时按代次裁决，无进程内锁替代。
- **旧库**：SQLite 适配器沿用原表、列、索引、body、token 与回执格式，`CREATE TABLE IF
  NOT EXISTS` 打开旧库不迁移、不改 schema，直接继续处理。

## 验证入口

`python3 checks.py verify`（即运行清单 verify）依次执行：

1. 原业务随机回归（96 步 + 故障注入 + 重开 + 回放 + 嵌套事务）；
2. 导入 `samples/legacy.sql` 的旧库兼容；
3. `boundary_checks`：模块导入白名单、适配器禁用 SQL 算子、门面纯转发、
   仅实现端口的内存替身驱动完整 Engine（含故障与嵌套回滚）、竞争写入代次裁决；
4. `adapter_equivalence.py` 另在 SQLite 与内存两适配器上交叉核对同批换键换路由、
   删除复活、重复映射、NULL/空串桶与冲突回滚。

`python3 demo.py` 继续输出真实修订前后的回执与快照。
