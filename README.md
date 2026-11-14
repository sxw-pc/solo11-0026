# 湖仓回溯台 / LakeBack

LakeBack 是一个**完全离线运行**的本地湖仓版本账本、时间旅行查询器与回填一致性审计器。它把“本地 Parquet/CSV 目录”当作轻量湖表，为每次快照建立不可变 manifest，并围绕历史版本提供只读查询与审计能力。

## 1. 项目定位

它是：

- 本地湖表的**版本账本**：每个快照记录表结构、分区、文件清单、行数、文件大小与 sha256；
- **时间旅行查询器**：按版本号或时间点读取历史快照，支持 Hive 风格分区裁剪；
- **回填一致性审计器**：对比回填前后两套目录，给出结构化、可复核的 JSON/Markdown 报告；
- 一个只监听 loopback、只读、无外部依赖的本地 HTTP 查询服务。

它不是：

- Spark / Trino / Iceberg / Hudi / Delta 的实现或替代品；
- 分布式表服务、对象存储网关或并发事务系统；
- BI、看板、ETL 编排平台或通用 SQL 引擎；
- 生产级数据湖安全边界；其账本可检测篡改，但不是带密钥的数字签名系统。

运行期不联网、不调用外部数据库、不依赖远程存储。演示数据全部由带显式种子的脚本生成。

## 2. 快速上手

要求 Python 3.10+。完整 Parquet 路径建议 `pyarrow>=14`（本地验证使用 25.0.1）；无 pyarrow 时可使用 CSV 降级路径。

```bash
# 可选：创建虚拟环境
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1

# 带 Parquet 与测试依赖
pip install -e ".[dev]"

# 或者只安装基础包，之后使用 CSV 后端
pip install -e .
```

环境自检（正常时退出码 0）：

```bash
lakeback doctor
lakeback doctor --json
lakeback selftest
```

生成确定性的合成 fixture。默认后端取自 `lakeback.toml`（`parquet`）；若没有 pyarrow，可加 `--backend csv`：

```bash
lakeback gen-fixtures
lakeback gen-fixtures --backend csv
```

无 pyarrow 时，最稳妥的做法是先把 `lakeback.toml` 的 `[storage] backend` 改成 `csv`，再执行完整流程；也可以给 `gen-fixtures`、`snapshot`、`backfill-audit` 逐一传 `--backend csv`。`travel`/`diff` 读取 manifest 中记录的后端，不需要另传参数。

生成布局：

- `data/fixtures/before/<table>/dt=.../part-00000.<ext>`：回填前；
- `data/fixtures/after/<table>/dt=.../part-00000.<ext>`：回填后；
- `data/lake/<table>/dt=.../part-00000.<ext>`：当前湖表镜像（内容同 after）。

同一 seed、同一后端重复执行，`digests` 完全一致；命令输出了 before/after/lake 的树摘要。

建立两个有先后关系的快照（baseline 使用 `--table-root` 显式指定回填前目录，当前湖表则用默认目录）：

```bash
lakeback snapshot --table orders --table-root data/fixtures/before/orders
lakeback snapshot --table orders
```

输出会包含 `version`、`snapshot_id`、`created_at`、`row_count`、`file_count` 和文件校验和。重复对同一内容执行 snapshot 会返回**同一个 snapshot_id 与同一个 version**，不会追加账本记录。

时间旅行（按版本号）：

```bash
lakeback travel --table orders --at 1 --limit 5
lakeback travel --table orders --at 1 --limit 5 --json
```

时间旅行（按时间点：取 `created_at <= at` 的最近快照；远未来时间等价于最新快照）：

```bash
lakeback travel --table orders --at 2099-01-01T00:00:00Z --limit 5
```

分区裁剪：

```bash
lakeback travel --table orders --at 2 --partition dt=2024-01-01 --limit 5 --json
```

版本差异（按配置中的主键对齐，字段级明细）：

```bash
lakeback diff --table orders --from 1 --to 2
lakeback diff --table orders --from 1 --to 2 --json
```

跨表参照完整性审计（分别固定事实表与维表的快照版本，逐行核对外键）：

```bash
lakeback referential-audit \
  --fact-table orders --fact-version 1 \
  --dimension-table users --dimension-version 1 \
  --key user_id=user_id

# 联合外键：重复 --key，顺序即元组顺序
lakeback referential-audit \
  --fact-table orders --fact-version 1 \
  --dimension-table users --dimension-version 1 \
  --key user_id=user_id --key region=region \
  --null-policy skip --json
```

回填审计（默认 fixture 会得到 `residual_diff`，所以退出码为 **4**）：

```bash
lakeback backfill-audit \
  --table orders \
  --before data/fixtures/before/orders \
  --after data/fixtures/after/orders \
  --json
```

同一份数据自比会得到 `consistent` 且退出码 0：

```bash
lakeback backfill-audit \
  --table orders \
  --before data/fixtures/after/orders \
  --after data/fixtures/after/orders
```

启动只读 HTTP 服务，默认 `127.0.0.1:5000`。以下两种方式等价：

```bash
lakeback serve
python -m lakeback
```

```bash
curl http://127.0.0.1:5000/api/tables
curl "http://127.0.0.1:5000/api/travel?table=orders&at=2&limit=5"
curl "http://127.0.0.1:5000/api/diff?table=orders&from=1&to=2"
curl "http://127.0.0.1:5000/api/backfill/report?report_id=<report_id>"
```

浏览器打开 `http://127.0.0.1:5000/` 可使用内置极简查询页；页面资源全部内联，无 CDN。

运行测试：

```bash
pytest -q
```

## 3. 快照语义与时间旅行语义

### 快照与版本

- 版本号按表独立、从 **1** 开始递增，仅在新内容被追加时分配。
- `snapshot_id = "snap-" + sha256(canonical_json(content_payload))[:32]`。content payload 包含表名、后端、字段顺序与类型、分区清单，以及每个文件的相对路径、分区、行数、大小和 sha256。
- `snapshot_id` **不包含**创建时间、版本号和 parent，因此“同一数据重复生成”得到同一个 ID；账本按 ID 去重并直接返回既有版本。
- 新快照的 `parent_id` 是该表上一个已追加快照的 ID；账本用 `previous_hash + canonical_json(manifest)` 形成逐条 hash 链。
- 账本文件 `data/ledger/snapshots.jsonl` 只允许追加。索引 `data/ledger/index.json` 是派生视图；启动读取时会同时校验 hash 链、版本连续性、parent 链和索引一致性。发现历史行或索引被改写会拒绝加载。
- 快照数据文件会复制到 `data/snapshots/<snapshot_id>/<原相对路径>`。因此之后覆盖 `data/lake/<table>` 不会破坏历史快照；时间旅行只读快照副本。

### 时间点解析

- 版本选择器接受 `1`、`v1`；也接受 `latest`。
- 时间选择器接受 ISO-8601；无时区按 UTC，存储精度为秒。按时间查询命中的是 **`created_at <= at` 的最近快照**，等于边界时刻的快照也会命中；同一秒多个快照时取版本号更大的那个。
- 不存在对应时间点/版本时返回明确错误；CLI 退出码为 3，HTTP 返回结构化 404。
- 时间旅行返回 JSON 行集（不提供 SQL/SQLite 视图）。行默认按配置主键升序；CLI 支持 `--limit`/`--offset`，HTTP 默认 limit=50、最多 500。
- 分区过滤格式为 Hive 风格 `name=value`，可重复传 `--partition`。读取层只选择 manifest 中匹配的分区文件，不会物化未选分区；读取时还会核对文件 sha256。

### 差异语义

`diff` 以 `lakeback.toml` 的 `[keys].<table>` 为主键（表必须有该键）。输出新增行、删除行和修改行的字段级 before/after；三种列表均按主键升序（数字键按数值排序），不依赖文件系统遍历顺序。字段变更顺序遵循 snapshot schema，确定可复现。

### 跨表参照完整性语义

`referential-audit` 独立于 `compute_diff`：调用方分别用 `--fact-version` 与 `--dimension-version` 固定两张表各自的版本，逐行核对事实表外键能否在维表同一版本中找到父行。

- **版本必须显式**：只接受正整数或 `vN`；`latest`、ISO 时间戳与空值都会被拒绝（退出码 2），不会回退到当前湖表或最新快照。
- **先固定再读取**：两个 manifest 都先解析并固定，之后才读取任一快照；读取后重新核对每个快照文件的 sha256，并从磁盘重读账本，证明两个已固定版本之前的历史未被改写。对固定版本之后追加的新版本不影响判定。
- **联合键按类型元组比较**：`--key fact_col=dim_col` 可重复，顺序即联合键顺序。每个分量用各自快照 schema 归一后保留为独立元组元素，绝不拼接字符串，因此 `("x","yz")` 与 `("xy","z")` 不会碰撞。int/float 互比时各自保留本侧类型，避免大整数被 float 舍入而产生假匹配。
- **缺失列或类型不兼容明确拒绝**：外键列在任一侧 schema 中缺失，或两侧类型既不相同也非 int/float 数值域时，直接报错（`data_error`），既不记为孤儿也不记为匹配。
- **NULL 策略显式**：`--null-policy skip`（默认）跳过外键任一分量为 NULL 的行并计入 `null_skipped`；`--null-policy error` 遇到此类行即报错。维表中键分量为 NULL 的行不参与匹配。
- **父键重复单独报告**：同一固定版本内父键重复时，不静默取一条、也不退化为多对一；该键列入 `parent_key_duplicates`（含证据行与行号），相应事实行计入 `ambiguous_parent_matches` 并单列明细，与真实 `orphans` 分开。
- **结果可复核**：`counts` 给出 `fact_rows / checked / matched / ambiguous_parent_matches / orphans / null_skipped` 等计数，`checks` 给出行数守恒、参照完整性与父键唯一性三项判定，`verified` 记录快照文件 sha256 核验数量、账本记录数与固定版本的历史摘要。
- 结论 `status` 为 `consistent`、`orphan_references`、`duplicate_parent_keys` 或 `orphan_references_and_duplicate_parent_keys`；非 `consistent` 时退出码 4（与 backfill-audit 的 CI 卡点一致），版本不存在为 3，参数/数据错误为 2。

## 4. 回填审计的判定规则与三类结论

`backfill-audit` 比较两套目录并执行五项检查，报告中的 `checks` 会给出每项的判定依据：

1. **schema**：字段名、字段类型、字段顺序是否一致；
2. **partition_structure**：分区目录列与分区值集合是否一致；
3. **row_count**：总行数是否一致；
4. **primary_key_set**：主键集合是否一致；
5. **partition_checksums**：共同分区内，按主键排序后每个字段的 sha256 以及分区级汇总摘要是否一致。

判定优先级与三类结论：

- `structural_mismatch`：schema 或分区结构不一致（优先级最高）；
- `residual_diff`：结构一致，但行数、主键集合或分区字段校验和存在残留差异；
- `consistent`：全部检查通过。

报告同时包含 `counts`、缺失/多出主键、修改行的字段级明细、分区校验和摘要、缺失/多出分区，以及 `checks[].evidence`。JSON 报告写入 `data/reports/<report_id>.json`，Markdown 摘要写入同名 `.md`；`report_id` 由两端内容指纹决定，可稳定对应同一次审计。审计没有“预期变更白名单”：任何差异都会体现为 residual，必须由调用方另行确认。

CLI 退出码：`consistent` 为 0；`residual_diff` 与 `structural_mismatch` 均为 4，方便 CI 卡点；参数/配置错误为 2；数据缺失或版本不存在为 3。

## 5. 目录结构

```text
lakeback/
├── README.md
├── pyproject.toml
├── lakeback.toml
├── lakeback/
│   ├── __init__.py
│   ├── __main__.py
│   ├── cli.py / cli_render.py / cli_diagnostics.py
│   ├── config.py / errors.py / _util.py / _toml.py / tables.py
│   ├── backends/
│   │   ├── __init__.py / base.py
│   │   ├── csv_backend.py
│   │   └── parquet_backend.py
│   ├── snapshot/
│   │   ├── ledger.py / manifest.py / snapshotter.py
│   ├── travel/
│   │   ├── resolver.py / reader.py / diff.py / referential.py
│   ├── backfill/
│   │   ├── auditor.py / report.py
│   ├── service/
│   │   ├── app.py / routes.py
│   │   └── static/index.html
│   └── fixtures/
│       ├── generate.py / seed.toml
├── data/                       # 运行时生成；仓库内仅保留 .gitkeep
│   ├── lake/
│   ├── fixtures/{before,after}/
│   ├── ledger/{snapshots.jsonl,index.json}
│   ├── snapshots/<snapshot_id>/
│   └── reports/
└── tests/
    ├── conftest.py
    ├── test_config.py / test_backends.py / test_snapshot.py
    ├── test_travel.py / test_diff.py / test_referential.py / test_backfill_audit.py
    ├── test_service.py / test_cli.py / test_fixtures.py
```

`data/` 下所有内容都由命令生成，已加入 `.gitignore`；不提交二进制 fixture。

## 6. 配置项（`lakeback.toml`）

配置文件默认从当前工作目录读取；也可在子命令前使用 `--config PATH`。路径键相对配置文件所在目录解析。

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `paths.lake` | `data/lake` | 当前湖表根目录 |
| `paths.ledger` | `data/ledger` | 追加式 JSONL 账本与索引 |
| `paths.snapshots` | `data/snapshots` | 快照数据文件不可变副本 |
| `paths.reports` | `data/reports` | 回填审计 JSON/Markdown 报告 |
| `paths.fixtures` | `data/fixtures` | before/after fixture 根目录 |
| `paths.seed` | `lakeback/fixtures/seed.toml` | 合成数据参数文件 |
| `storage.backend` | `parquet` | `parquet` 或 `csv` |
| `service.host` | `127.0.0.1` | HTTP 绑定地址；只允许 loopback |
| `service.port` | `5000` | HTTP 端口，1–65535 |
| `clock.mode` | `system` | `system` 或 `fixed` |
| `clock.fixed_time` | 无 | `fixed` 模式必填，ISO-8601 UTC 时间 |
| `keys.<table>` | 无 | 该表主键字段；diff/audit 必需 |

CLI 的 `--backend` 可对生成 fixture、snapshot 和 backfill-audit 临时覆盖 `storage.backend`。`serve --host`/`--port` 可覆盖服务地址，但非 loopback 地址会被拒绝。

## 7. 离线与安全说明

- 运行期不发起网络请求；应用代码只使用标准库的本地文件、哈希、`http.server` 和可选 pyarrow。测试中的 HTTP 客户端仅访问测试进程自己监听的 loopback 临时端口。
- 服务只允许绑定 `127.0.0.1`、`localhost`、`::1`，绑定前会再次校验；不提供写入表数据或生成快照的 HTTP 接口。
- 表名使用白名单正则；快照文件路径经过 `safe_child` 防止目录穿越；`report_id` 必须匹配 `rpt-<16 hex>`；未知表、版本、报告返回结构化 4xx JSON。
- 所有 fixture 的表名、分区、字段值均为合成值；不包含 token、密码、真实域名、真实外部 IP 或公司信息。默认样例仅出现 loopback 地址。
- 报告和账本都写入配置指定的本地目录，不向外发送。

## 8. 后端降级说明

- `parquet` 后端依赖 `pyarrow>=14`，仅在调用该后端时导入；缺失时 `lakeback doctor` 会显示不可用，数据命令会给出可执行的降级提示。
- `csv` 后端完全基于 Python 标准库，是官方降级路径。切换方式二选一：
  - 修改 `lakeback.toml`：`[storage] backend = "csv"`；
  - 对支持该选项的命令传 `--backend csv`（`gen-fixtures`、`snapshot`、`backfill-audit`、`selftest`）。
- 已生成的快照 manifest 记录自己的后端；`travel`/`diff` 按 manifest 读取，不依赖当前默认后端。若 manifest 使用 parquet 而环境没有 pyarrow，会明确报错。
- 测试对两种后端都覆盖：`test_backends.py` 参数化 CSV 与 Parquet，并检查逻辑结果一致；未安装 pyarrow 时 Parquet 用例自动跳过，CSV 路径与其余测试仍全绿。
- CSV 限制：类型由各文件内容推断，纯数字字符串会按整数/浮点数读取，`true`/`false` 字符串会按布尔读取；需要保留文本语义时应显式约定 schema 或改用 Parquet。所有文件字段集合必须一致。

## 9. 测试运行方式与覆盖率说明

```bash
pytest -q
# 覆盖率命令另需开发期工具 pytest-cov；它不属于运行依赖
pytest --cov=lakeback --cov-report=term-missing
```

测试全部使用 `tmp_path`，不访问网络、不写用户 home。覆盖范围包括：配置错误分支、两种后端、快照幂等、账本 hash 链/索引篡改检测、版本与时间边界、分区裁剪、diff 确定性、三种审计结论、HTTP 正常与 4xx/405、非法绑定、CLI 退出码 0/2/3/4、fixture 字节确定性。

最近一次本地统计（详见文末验证记录）：**97 passed**，`lakeback` 包行覆盖率 **84%**；测试 Python 物理行约 1292 行，占 Python 总物理行 4469 行的约 29%（要求 ≥25%）。

## 10. 已知边界与明确不实现

已知边界：

- 时间精度为秒；同一秒内的多个快照按版本号取较新的。
- 同一内容重复 snapshot 不会创建新版本；如果业务需要“同内容新版本”，需要后续引入显式版本语义。
- 本地账本使用进程内锁，只保证单进程追加；没有跨进程/多主机并发控制，也不提供 ACID 写事务。
- 快照数据文件会完整复制，因此磁盘占用随快照数增长；没有压缩、去重存储或清理机制。
- 时间旅行读取会把所选分区的命中行载入内存；fixture 规模为数千行，不面向大数据量。
- 一个表的物理文件必须有一致字段集合和一致的分区目录布局；否则拒绝快照/审计并报结构错误。
- CSV 类型推断存在上述文本语义边界；Parquet 行为由 pyarrow schema 决定。
- 审计没有“允许的预期变更”白名单，任何差异都会作为 residual 呈现。

明确不实现：

- 真正的 ACID 表事务、并发写冲突解决、快照压缩（compaction）与过期清理（vacuum）；
- 跨表血缘与谱系分析；
- 流式摄入、CDC、消息队列；
- 多用户鉴权、租户隔离、远程对象存储后端；
- SQL 解析与任意查询下推（只支持按表/分区/版本的时间旅行读取与差异）；
- 写入 HTTP 接口、前端框架工程化；
- 性能基准与大数据量压测。

## 附录：本地验证记录

验证日期：2026-11-13（Asia/Shanghai）。主环境：Windows 11、Python 3.13.15、pyarrow 25.0.1、pytest 8.3.5。另建一个不含 pyarrow 的干净 venv（构建依赖只使用本地缓存的 setuptools 84.0.0，全程无网络）验证安装与 CSV 降级。

| 命令 | 退出码 | 关键结果 |
| --- | --- | --- |
| 干净 venv 中 `pip install --no-index --no-deps --no-build-isolation -e .` | 0 | `Successfully installed lakeback-0.1.0`；`lakeback` 命令可用；`pip check` 无损坏依赖 |
| 干净 venv（无 pyarrow）中 `lakeback --config <csv-config> doctor --json` | 0 | backend=`csv`、parquet=`false`、`ok=true`；CSV 自检通过 |
| `python -m lakeback doctor` | 0 | 当前配置 backend=`parquet`；csv/parquet 均可用；账本状态 ok |
| `lakeback selftest --backend csv --json` | 0 | write/read_roundtrip/checksum/scan/tree_digest 全部 ok |
| `lakeback gen-fixtures`（连续两次） | 0 / 0 | 两次 JSON 与 `digests` 完全相同；before=`8fb9eab…`、after/lake=`eb6fe2a…` |
| `lakeback snapshot --table orders --table-root data/fixtures/before/orders` | 0 | version=1、`snap-681399b…`、1500 行、5 个文件 |
| `lakeback snapshot --table orders` | 0 | version=2、`snap-2aadcd5…`、1501 行；parent 指向 version 1 |
| 再次执行当前目录 snapshot | 0 | 仍为 version=2 与同一 snapshot_id；账本保持 2 行 |
| `lakeback travel --table orders --at 1 --limit 3 --json` | 0 | version=1、`total_rows=1500`，返回 3 行且按主键排序 |
| `lakeback travel --table orders --at 2 --limit 3 --json` | 0 | version=2、`total_rows=1501` |
| `lakeback travel --table orders --at 1 --partition dt=2024-01-01 --limit 2 --json` | 0 | `total_rows=300`，只读该分区 |
| `lakeback travel --table orders --at 2099-01-01T00:00:00Z --limit 1 --json` | 0 | 时间点解析到最新 version=2 |
| `lakeback diff --table orders --from 1 --to 2 --json` | 0 | added=3、removed=2、modified=6、unchanged=1492；schema_changed=false |
| `lakeback backfill-audit … --before data/fixtures/before/orders --after data/fixtures/after/orders` | 4 | `residual_diff`、`rpt-dc6d2003fd3baf5a`；missing=2、extra=3、modified=6 |
| 同目录自比 `backfill-audit` | 0 | `consistent`、`rpt-b5ef89f760bcbe09`；全部计数为 0 |
| `lakeback travel --table orders --at 99` | 3 | 输出 `no snapshot version 99` 到 stderr |
| `lakeback snapshot --table ../escape` | 2 | 参数校验拒绝并输出 `lakeback:` 错误 |
| `pytest -q` | 0 | `97 passed` |
| 无 pyarrow 屏蔽测试（CSV 降级） | 0 | `93 passed, 1 skipped`；仅 Parquet 对照用例跳过 |
| `pytest -q --cov=lakeback --cov-report=term` | 0 | 1876 statements / 296 missed / 总覆盖率 84% |
| 单文件/总量检查 | — | 最大 Python 文件 282 行（≤300）；Python 总计 4469 行（2500–4500） |

HTTP 验证：以无参数 `python -m lakeback` 在 `127.0.0.1:5000` 启动；`/`、`/api/tables`、`/api/travel`、`/api/diff`、`/api/backfill/report` 均返回 200（例如报告接口返回 `rpt-dc6d2003fd3baf5a` 的 8273 字节 JSON）；未知版本返回结构化 404；`../../etc/passwd` 形式 `report_id` 返回结构化 400；POST 返回 405；`0.0.0.0` 等非 loopback 绑定由配置层和服务层拒绝。
