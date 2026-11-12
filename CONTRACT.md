# 计量结算关联数仓：现有行为与重构交付契约

## 真实入口
Python 3 标准库和 SQLite。现有 warehouse.py 同时负责版本判定、失效传播、两级关联、聚合、回执与事务。
python3 warehouse.py --db PATH apply BATCH.json
python3 warehouse.py --db PATH read
不存在的数据库自动建库。批次为 UTF-8 JSON；成功向 stdout 输出一个 JSON 对象并退出 0；业务错误输出 {"error":"CODE"} 并退出 2。对象字段顺序无关，数组顺序有关。输入不含重复 JSON 对象键，字符串使用 Unicode 标量值。
Python API 为 Store(path=":memory:", *, connection=None)、apply(batch, fail_at=None)、read()、transaction(write=True)、close()；Error 和 FAULTS 仍可从 warehouse 导入。connection 是空闲的 sqlite3.Connection；conn 属性保留以便现有诊断读取原表。CLI 仅为数据链路入口。
python3 checks.py 核对业务并导入 samples/legacy.sql 验证旧库存续；python3 demo.py 展示真实修订前后快照及回执。初始运行清单 verify 只做业务回归，结构检查在重构后启用。

## 输入、版本和事务
批次恰含 batch_id（非空字符串）、expected_generation（非负整数，不接受 bool）、changes（数组，可空）。
changes 每项有 source（a/b/c）、id（非空字符串）、rev（正整数，不接受 bool）、deleted（bool）。同一批 source/id 不重复，字段不能多也不能少。
deleted=true 仅含这四项；否则 a 另含 k/day/amount，b 另含 k/route/region，c 另含 route/factor。
k、route、region 为字符串或 null，空字符串合法；day 为非空字符串。amount 为 null 或绝对值不超过 1000000 的整数；factor 为 null 或绝对值不超过 1000 的整数，不接受 bool。有效业务数据的各组金额和在有符号 64 位范围内。
每个 source/id 仅保留最高 rev，墓碑也保留。低版本忽略；同版本同内容忽略；同版本异内容 REVISION_CONFLICT；高版本可换键、换路由、删除或复活。内容是包含 source 和 deleted 的完整输入行的 canonical JSON。
canonical 为 json.dumps(x,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)。
校验整个输入并报 BAD_INPUT 优先；进入写事务后先查 batch_id 回执：changes 的 canonical 字节相同则原样返回第一次结果，不比较 expected_generation、不触发故障、不改变数据；不同则 BATCH_CONFLICT。数组次序属于身份，expected_generation 不属于身份。
新批次先比较 expected_generation，否则 GENERATION_CONFLICT；再按 (source,id) 的 Python 字符串顺序检查版本。错误次序及整批回滚保持不变。
每个首次成功批次 generation 加 1，空批、全部过期版本或相同版本也如此；回放不加。初始 generation=0。
source、j1、j2、g、generation 和 receipts 一起提交或回滚。五个故障点为 after_sources、after_first、after_second、after_rollup、after_receipt，抛出 Error("INJECTED:"+点名)；新批次即使无实际变更也依次经过这些点。
transaction 支持嵌套：外层可把多个 apply 组成一次原子发布；内层失败只撤销内层，调用方可继续外层。外层回滚撤销已返回的内层回执和所有派生结果。read 在外层内看到本事务结果，其他连接只能看到已提交结果；外层 write=False 只读，不在其中 apply。竞争新批次仍按提交时 generation 裁决；不能使用进程内锁替代 SQLite 写事务。

## 关联、聚合、局部性和输出
a 为计量样本，b 为计量键到计价路由及区域的多重映射，c 为路由上的计价因子。重复属性的不同 id 是不同业务记录，不能去重。
第一层 j1 是 a 与 b 按 k 的多重集全外关联；第二层 j2 是 j1 与 c 按 route 的多重集全外关联。两层均仅取未删除源记录，NULL 不与 NULL 匹配，空字符串正常匹配。没有匹配也保留左行或右行，缺失列为 null。
j1 token 是 canonical([aid,bid])；j2 token 是 canonical([aid,bid,cid])；缺失身份为 null。j1 的 k 来自存在的一侧；j2 的 k 来自 j1、route 来自存在的一侧；day/amount 来自 a，region 来自 b，factor 来自 c。charge 为 amount*factor，任一为 null 则为 null；负数、零都保留。
g 按 (day,region) 分组，NULL 分组与 SQLite GROUP BY 相同：n 是行数，valued 是非空 charge 数，total 是非空 charge 的和且全空时为 null，samples 是非空 aid 的去重数。total=0 的非空组仍保留；没有行才删组。g token 是 canonical([day,region])。
read 输出 {"generation":N,"rows":[j2完整行...],"groups":[g完整行...]}，两个数组均按 token 字符串排序。行字段、SQL 表列和索引以 warehouse.py 和 samples/legacy.sql 为准。
apply 输出 {"batch_id":ID,"generation":N,"delta":[...],"work":{"keys":[...],"routes":[...],"groups":[[day,region],...]}}。
delta 对比批次前后最终 j2；相同 token 且所有列相同的行不输出，消失或改变的旧行输出 {"weight":-1,"row":旧完整行}，新增或改变的新行输出 {"weight":1,"row":新完整行}，按 (canonical(row),weight) 排序。不能泄漏中间失配行或把跨阶段抵消留给调用者。
K 是实际接受的新版本涉及的 a/b 旧、新未删除行的 k；R 是实际接受的 c 旧、新未删除行的 route，加上批次前后 k 落在 K 的 j1 的 route；G 是批次前后 route 落在 R 的 j2 的 (day,region)。NULL 作为索引桶会使整个 NULL 桶重算，但不会使 NULL 参与等值匹配。
work 必须恰好报告 K/R/G，各自按 canonical 排序；G 元素以二元数组输出。它们包含重建后内容相同的桶。不能扩大闭包、全表关联、全量导出后重建，或先更新一键再按另一键的旧状态计算；同批移动和交换必须以同一批最终状态为准。
原始示例 samples.py 的 INITIAL 后 east 组为 n=4、valued=4、total=1000、samples=1；CORRECTION 后 west 的 total=-100，原 r 路由的两条 c 成为右侧孤行。s2 的 NULL 键始终不能匹配其他 NULL 键。

## 本轮结构交付
业务不新增能力。交付 reconcile/domain.py、reconcile/application.py、reconcile/ports.py、reconcile/adapters/sqlite.py、reconcile/adapters/memory.py；包初始化文件为空。
领域计算无 I/O，只接收普通数据并返回普通数据或可继续的纯计划，不得接收连接、仓储对象或 I/O 回调。修订判定、两级关联、失效闭包、分组及 delta 规则全部属于领域。
应用 Engine(repository) 暴露 apply/read/transaction/close，负责事务、端口调用、计划解释和故障位置；不得自行实现上述领域规则，也不依赖具体适配器。
SQLiteRepository(path=":memory:", *, connection=None) 和 MemoryRepository() 实现同一端口。适配器只做索引行存取、事务和 JSON 编解码，不承载关联、聚合或修订选择规则。SQLite 适配器不得使用 JOIN、UNION、GROUP BY、聚合函数、业务 UDF、触发器或视图代替领域计算。
原 Store 只保留构造装配和兼容转发，不得保留备用执行器；CLI 保留。SQLite 原表、列、索引、源 body、token、回执格式和原库数据均不迁移，直接打开旧库继续处理。
允许的模块依赖及公开结构由 boundary_checks.py 固定；不得动态导入、读源码执行或把业务藏进包初始化文件。结构规则适用于新增的辅助代码，不能通过辅助代码绕过。这些检查是可重跑证据，不代替对业务归属的源码审查。

仓储端口只含以下方法；Engine 不能读取适配器私有属性：
transaction(write=True)：嵌套事务上下文，异常撤销当前层；
generation() / set_generation(n)；
receipt(batch_id)：返回 None 或 {"payload":canonical_changes字符串,"result":结果对象}；
put_receipt(batch_id,payload,result)；
select(table,index,value)：返回完整行字典的独立副本，包含墓碑，不执行 live 筛选；
upsert(table,rows)：按原表主键替换完整行；
delete(table,ids)：按原表主键删除；
export()：返回 read 的完整对象，仅供只读入口；close()。
table 仅 a/b/c/j1/j2/g。id 索引对应各表主键；另外 a/b 支持 k，c 支持 route，j1 支持 k/route，j2 支持 route/group。group 的 value 为 (day,region)，其他 value 为一个标量；索引以 IS 语义比较，NULL 桶包含所有 NULL。方法不得返回共享可变引用。
apply 只可按本批 source/id 点查源版本，按 K 读 a.k/b.k/j1.k，按 R 读 c.route/j1.route/j2.route，按 G 读 j2.group；不能读取其他索引值，也不能调用 export。上面允许的查询可以重复，不能借 id 查询枚举整表。receipt 和 generation 查询不计入闭包。
内存实现不能调用 SQLite 或原 Store，也不能执行领域规则；事务必须同样支持嵌套提交与回滚。仅实现上述方法的替身应能驱动完整 Engine，而不是用于输出展示的空替身。
保留 checks.py、boundary_checks.py、样例及其中断言；可增加验证。重构后运行清单 verify 改为 ["python3","checks.py","verify"]，它同时执行原业务、旧库、结构、端口替身、故障和竞争写入验证；demo 继续展示真实业务结果。交付代码和简短职责说明。
