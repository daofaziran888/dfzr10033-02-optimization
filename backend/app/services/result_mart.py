"""结果集市领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。

集市本身不直接读缓存拼数据，而是消费分析导出的可核对产物：

* 读取 manifest 与行数清单，确认哪些分区已闭合、各多少行；
* 发布清单里带行数口径和来源快照，历史快照按原口径兼容读取；
* 领取动作返回回执，关联到当时可领取的 CSV 与覆盖说明。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.data_exporter import DataExporterService
from app.store import store


class ResultMartService(BaseModuleService):
    MODULE = "result_mart"
    ENTITY = "数据Mart"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["集市编号", "集市名称", "数据来源", "主题域", "行数统计", "刷新周期", "下游消费", "最后刷新"]
    REQUIRED_FIELDS = ["集市编号", "集市名称", "数据来源"]
    STATUS_ORDER = ["草稿", "刷新中", "可用", "刷新失败"]
    ACTION_RULES = {"启动刷新": "刷新中", "标记可用": "可用", "标记失败": "刷新失败"}
    NEGATIVE_ACTIONS = []

    def publish_from_export(
        self,
        mart_id: int,
        *,
        export_id: int | None = None,
        request_id: str | None = None,
        filter_criteria: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """从导出清单发布有效分区：只计已闭合分区，行数口径写入集市。"""
        return DataExporterService().batch_export_to_mart(
            mart_id,
            filter_criteria,
            export_id=export_id,
            request_id=request_id,
        )

    def claim_detail(self, claim_id: str) -> dict[str, Any] | None:
        """回执详情：可领取 CSV、行数与覆盖说明。"""
        return DataExporterService().claim_receipt(claim_id)

    def publication_snapshot(self) -> dict[str, Any]:
        """集市发布快照：包含行数口径，便于历史快照兼容读取。"""
        rows = store.rows(self.MODULE)
        return {
            "module": self.MODULE,
            "count": len(rows),
            "publications": [
                {
                    "集市编号": row.get("集市编号"),
                    "行数统计": row.get("行数统计"),
                    "行数口径": row.get("行数口径") or "unknown",
                    "有效分区": row.get("有效分区", ""),
                    "发布清单": row.get("发布清单", ""),
                    "来源快照": row.get("来源快照", ""),
                }
                for row in rows
            ],
        }
