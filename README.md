# 湖仓工坊 · 联邦查询与多维自助分析平台

面向异构数据源联邦查询、语义模型构建、OLAP多维立方物化、聚合缓存预热、时序窗口分析、查询改写优化与数据集市发布的多维自助分析平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 数据源注册 | `source_registry` | 注册数据源 | 源编号、源类型、连接地址 |
| 语义模型 | `semantic_model` | 语义定义 | 模型编号、模型名称、源表映射 |
| 联邦查询 | `query_federation` | 联邦计划 | 计划编号、参与来源、关联键 |
| 多维立方 | `olap_cube` | OLAP立方 | 立方编号、立方名称、维度列表 |
| 指标工厂 | `metric_factory` | 计算指标 | 指标编号、指标名称、数据来源 |
| 维度树 | `dimension_tree` | 维度层级 | 层级编号、维度名称、层级路径 |
| 聚合缓存 | `aggregate_cache` | 缓存配置 | 配置编号、目标立方、缓存粒度 |
| 查询改写 | `query_rewrite` | 改写规则 | 规则编号、原始模式、替换模式 |
| 时序窗口 | `time_window` | 窗口定义 | 窗口编号、窗口类型、滑动宽度 |
| 数据切片 | `dataset_slice` | 切片配置 | 切片编号、源立方、维度过筛 |
| 下钻配置 | `drill_config` | 下钻路径 | 路径编号、起始维度、目标维度 |
| 结果集市 | `result_mart` | 数据Mart | 集市编号、集市名称、数据来源 |
| 基准集 | `benchmark_set` | 分析基线 | 基线编号、基线名称、数据范围 |
| 查询审计 | `query_audit` | 审计记录 | 记录编号、查询语句、执行耗时 |
| 成本治理 | `cost_govern` | 配额策略 | 策略编号、适用角色、查询上限 |
| 新鲜度监控 | `freshness_mon` | 新鲜度规则 | 规则编号、目标数据集、SLA秒数 |
| 分析导出 | `data_exporter` | 导出任务 | 任务编号、数据来源、筛选条件 |
| 定时计算 | `schedule_job` | 预计算任务 | 任务编号、目标立方、Cron表达式 |
| 湖仓读取 | `data_lake_read` | 湖仓路径 | 路径编号、存储位置、文件格式 |
| 结果缓存 | `result_cache` | 缓存条目 | 条目编号、查询摘要、结果哈希 |
| 数据契约 | `data_contract` | 契约定义 | 契约编号、数据集、Schema约束 |
| 实验配置 | `ab_config` | AB实验 | 实验编号、实验名称、分流比例 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 导出到集市：分段文件、分区回执与核账

分析导出把结果缓存里的查询明细按分区、按块写成 CSV，内存里不再出现整份
导出字符串。相关实现集中在 `app/services/export_files.py`（文件/回执/清单）
与 `app/services/data_exporter.py`（分区编排、集市发布、核账）。

- **字段顺序**：沿用导出任务申请里的「字段列表」，仅 `*`/空时回退到首行字段。
- **CSV 转义**：换行、逗号、引号交给标准 `csv` 模块处理，跨行记录保持为一条记录。
- **分区确认**：分区最后一段落盘并核对记录数闭合后才写 `confirmed` 回执；
  空分区也写回执。中断时留下未闭合文件，不写回执。
- **续跑**：已 `confirmed` 且磁盘校验通过的分区沿用原回执，只补写未完成分区。
- **行数清单**：`manifest_export_<id>.json` 与 `rowcount_export_<id>.csv` 同步落盘，
  记录状态、行数、校验和、回执编号与确认时间。
- **集市发布**：只发布已确认且磁盘核对通过的分区；未闭合部分不计入已发布行数。
  发布清单写入 `行数口径`、`有效分区`、`来源快照`，历史快照按 `legacy_all_rows`
  口径兼容读取。
- **领取回执**：领取时复制可领取 CSV 的快照并生成回执，内含覆盖说明与排除分区。
- **核账**：按同一申请汇总多次领取，同一分区只计一份，并按回执里的 CSV 快照
  重新数记录数复核。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/pipelines/export-to-mart` | 导出并（可选）发布到集市 |
| GET | `/api/exports/{id}/manifest` | 导出清单（分区状态、行数、清单核对） |
| GET | `/api/exports/{id}/row-counts` | 人可读行数清单 |
| GET | `/api/exports/{id}/files/{partition}` | 领取已确认分区的 CSV |
| POST | `/api/exports/{id}/resume` | 交付恢复后续跑 |
| POST | `/api/marts/{id}/claim` | 从集市领取，生成回执 |
| GET | `/api/marts/receipts/{claim_id}` | 回执详情（CSV、覆盖说明） |
| GET | `/api/exports/requests/{request_id}/reconcile` | 按申请核对两区总数 |
| GET | `/api/exports/requests/{request_id}/receipts` | 该申请全部领取回执 |

导出文件默认写在系统临时目录下的 `lakehouse_exports/`，可用环境变量
`DATA_EXPORT_DIR` 指定。

领域：数据工程
子方向：数据分析与数据应用
