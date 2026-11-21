"""分析导出领域服务：将查询结果转换为多种格式导出，消费联邦查询结果和缓存数据。

导出按分区逐段落盘：

* 查询明细按 chunk 分段序列化，不再先拼成整份字符串；
* 每个分区完整落完最后一段才写 confirmed 回执，空分区也会写回执；
* 回执、manifest 和 rowcount 清单落盘，供结果集市核对有效分区与行数。
"""
from __future__ import annotations

import json
import random
import time
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Any, Iterable

from app.services import export_files
from app.services.base import BaseModuleService
from app.services.export_files import (
    PARTITION_CONFIRMED,
    PARTITION_PARTIAL,
    PARTITION_PENDING,
    ExportInterrupted,
)
from app.store import store

class DataExporterService(BaseModuleService):
    MODULE = "data_exporter"
    ENTITY = "导出任务"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["任务编号", "数据来源", "筛选条件", "输出格式", "字段列表", "排序规则", "导出上限", "接收地址", "分区字段"]
    REQUIRED_FIELDS = ["任务编号", "数据来源", "输出格式"]
    STATUS_ORDER = ["待执行", "导出中", "已完成", "导出失败"]
    ACTION_RULES = {"启动导出": "导出中", "取消导出": "已完成", "标记失败": "导出失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    def export_query_results(
        self,
        entry_id: int,
        query_result_ids: list[int] | None = None,
        *,
        partition_by: str | None = None,
        partitions: list[str] | None = None,
        interrupt: dict[str, Any] | None = None,
        chunk_size: int = 500,
    ) -> dict[str, Any]:
        """按分区流式导出查询结果，写 CSV、回执、manifest 与行数清单。

        查询明细按分区、按块流式序列化：第一遍只统计分区顺序，第二遍逐分区
        逐行写盘，内存中不再出现“整份导出字符串”，峰值内存与分区块成正比。
        中断时保留已落盘的未闭合分区文件，不写回执；再次调用同一导出任务时，
        已 confirmed 的分区直接沿用原回执，只补写未完成的分区。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "导出任务不存在"}

        fmt = entry.get("输出格式", "csv")
        request_id = str(entry.get("任务编号") or f"EXPORT-{entry_id}")

        manifest = export_files.load_manifest(entry_id)
        if manifest is not None and manifest.get("request_id") != request_id:
            manifest = None
        partition_field = (
            partition_by
            or str(entry.get("分区字段") or "").strip()
            # 续跑未显式指定时沿用上次申请的分区字段。
            or str((manifest or {}).get("partition_field") or "").strip()
            or None
        )

        source = _export_source(entry, query_result_ids)
        if not source.has_rows and source.error is None and not partitions:
            return {"ok": False, "message": "无可导出的数据"}

        fields = export_files.field_order(entry.get("字段列表"), source.sample)
        if not fields:
            return {"ok": False, "message": "导出字段为空"}

        if manifest is None:
            manifest = export_files.new_manifest(
                entry_id,
                request_id=request_id,
                format=fmt,
                fields=fields,
                partition_field=partition_field or "",
            )
        else:
            # 续跑沿用原申请与字段顺序，未闭合分区重新确认。
            manifest["fields"] = fields
            manifest["partition_field"] = partition_field or ""
        manifest["source_snapshot"] = source.snapshot
        manifest.setdefault("claims", [])

        # 第一遍：分区顺序（以及声明的零行分区）。
        order = source.partition_order(partition_field)
        for declared in partitions or []:
            if declared not in order:
                order.append(declared)

        existing = {part.get("name"): part for part in manifest.get("partitions", [])}
        reusable = {name: _reusable(existing.get(name)) for name in order}
        reused = [existing[name] for name in order if reusable[name]]
        pending_order = [name for name in order if not reusable[name]]

        writer = export_files.PartitionWriter(
            entry_id, fields, fmt=fmt, chunk_size=chunk_size
        )
        if interrupt:
            writer.set_interrupt(
                interrupt.get("partition"), int(interrupt.get("after_rows", 0))
            )

        started = time.time()
        finished: list[dict[str, Any]] = []
        interrupted: dict[str, Any] | None = None

        try:
            for name in pending_order:
                for row in source.iter_partition(partition_field, name):
                    writer.add(name, row)
                finished.append(_confirm_partition(writer, manifest, name))
        except ExportInterrupted as exc:
            writer.abort()
            track = writer.abort_info or {}
            interrupted = {
                "partition": str(exc.partition),
                "message": str(exc),
                "file": track.get("file"),
                "record_count": track.get("record_count", 0),
                "trailing_incomplete": track.get("trailing_incomplete", True),
            }
            finished.append(
                {
                    "name": str(exc.partition),
                    "status": PARTITION_PARTIAL,
                    "row_count": 0,
                    "record_count": track.get("record_count", 0),
                    "bytes": track.get("bytes", 0),
                    "checksum": "",
                    "file": track.get("file")
                    or str(export_files.partition_path(entry_id, str(exc.partition), fmt)),
                    "receipt_id": "",
                    "confirmed_at": "",
                    "note": "写到最后一条跨行记录中途断开，未闭合，不计入已发布行数",
                }
            )

        # 中断后，尚未开始的分区也登记为 pending，便于识别还未读完。
        done_names = {part["name"] for part in finished} | {part["name"] for part in reused}
        for name in order:
            if name not in done_names:
                finished.append(
                    {
                        "name": name,
                        "status": PARTITION_PENDING,
                        "row_count": 0,
                        "record_count": 0,
                        "bytes": 0,
                        "checksum": "",
                        "file": str(export_files.partition_path(entry_id, name, fmt)),
                        "receipt_id": "",
                        "confirmed_at": "",
                        "note": "尚未开始，待续跑确认",
                    }
                )

        manifest["partitions"] = _ordered_partitions(
            order, reused, finished, existing
        )
        export_files.save_manifest(manifest)

        published = _published_rows(manifest)
        all_confirmed = bool(manifest["partitions"]) and all(
            part.get("status") == PARTITION_CONFIRMED for part in manifest["partitions"]
        )
        entry["status"] = "已完成" if all_confirmed else "导出中"
        entry["pending"] = not all_confirmed

        _write_mart_entry(
            entry_id,
            fmt,
            published,
            sum(int(part.get("bytes", 0)) for part in manifest["partitions"]),
            request_id=request_id,
            semantics=manifest.get("row_count_semantics"),
        )
        _audit_export(
            entry_id,
            fmt,
            (time.time() - started) * 1000,
            published,
            manifest["partitions"][0].get("bytes", 0) if manifest["partitions"] else 0,
            statement=(
                f"EXPORT id={entry_id} fmt={fmt} partition={interrupted['partition']} "
                f"interrupted"
                if interrupted
                else None
            ),
            status="慢查询" if interrupted else "正常",
            abnormal=interrupted is not None,
        )

        result = {
            "ok": all_confirmed and interrupted is None,
            "format": fmt,
            "request_id": request_id,
            "row_count": published,
            "published_row_count": published,
            "published_partitions": manifest["published_partitions"],
            "partitions": manifest["partitions"],
            "manifest": str(export_files.manifest_path(entry_id)),
            "row_count_list": str(export_files.rowcount_path(entry_id)),
            "source_snapshot": source.snapshot,
            "elapsed_ms": (time.time() - started) * 1000,
            "interrupted": interrupted,
        }
        if len(manifest["partitions"]) == 1 and not interrupted:
            result["filename"] = manifest["partitions"][0]["file"]
        else:
            result["files"] = {
                part["name"]: part["file"]
                for part in manifest["partitions"]
                if part.get("receipt_id")
            }
        return result

    def resume_export(
        self, entry_id: int, *, chunk_size: int = 500, partitions: list[str] | None = None
    ) -> dict[str, Any]:
        """交付恢复后重跑：已确认分区沿用原回执，只补写未完成部分。"""
        return self.export_query_results(
            entry_id, partitions=partitions, chunk_size=chunk_size
        )

    def batch_export_to_mart(
        self,
        mart_id: int,
        filter_criteria: dict[str, str] | None = None,
        *,
        export_id: int | None = None,
        request_id: str | None = None,
        claim_id: str | None = None,
    ) -> dict[str, Any]:
        """从可核对的导出文件与行数清单发布到结果集市，并生成领取回执。

        只发布 confirmed 且磁盘核对通过的分区；未闭合分区明确排除。
        行数口径写入发布清单，历史快照仍按其原口径读取。
        """
        mart = store.find("result_mart", mart_id)
        if mart is None:
            return {"ok": False, "message": "数据集市不存在"}

        manifest = _resolve_manifest(export_id=export_id, request_id=request_id)
        if manifest is None:
            # 兼容旧快照：没有导出清单时退回原有缓存口径。
            return _legacy_batch_export(mart, filter_criteria)

        valid, excluded = _valid_partitions(manifest)
        if filter_criteria:
            valid = [
                part
                for part in valid
                if all(str(value) in str(part.get("name", "")) for value in filter_criteria.values())
            ]
        rowcount_audit = _cross_check_row_count_list(manifest)

        claim_id = claim_id or f"CLAIM-{uuid.uuid4().hex[:8]}"
        # 领取时把可领取的 CSV 各留一份快照，续跑改写原文件后仍可按回执核对。
        claim_files = []
        for part in valid:
            snapshot = export_files.snapshot_claim_file(claim_id, part)
            claim_files.append(
                {
                    "partition": part.get("name"),
                    "file": part.get("file"),
                    "snapshot": snapshot,
                    "row_count": part.get("row_count", 0),
                    "checksum": part.get("checksum"),
                    "receipt_id": part.get("receipt_id"),
                }
            )
        claim = {
            "receipt_id": claim_id,
            "type": "mart_claim",
            "mart_id": mart_id,
            "export_id": manifest["export_id"],
            "request_id": manifest.get("request_id"),
            "claimed_at": export_files.now_text(),
            "row_count_semantics": manifest.get("row_count_semantics"),
            "source_snapshot": manifest.get("source_snapshot", []),
            "coverage_text": export_files.coverage_text(valid, excluded),
            "files": claim_files,
            "excluded_partitions": [
                {
                    "partition": part.get("name"),
                    "status": part.get("status"),
                    "reason": part.get("note") or "未闭合",
                }
                for part in excluded
            ],
            "claimed_rows": sum(int(part.get("row_count", 0)) for part in valid),
        }
        export_files.save_claim_receipt(claim)
        manifest.setdefault("claims", []).append(
            {
                "claim_id": claim_id,
                "claimed_at": claim["claimed_at"],
                "partitions": [part.get("name") for part in valid],
                "excluded": [part.get("name") for part in excluded],
                "claimed_rows": claim["claimed_rows"],
                "coverage_text": claim["coverage_text"],
            }
        )

        published = sum(int(part.get("row_count", 0)) for part in valid)
        mart["行数统计"] = str(published)
        mart["最后刷新"] = export_files.now_text()
        mart["status"] = "可用" if valid else "刷新失败"
        mart["pending"] = not valid
        mart["有效分区"] = ",".join(str(part.get("name")) for part in valid)
        mart["排除分区"] = ",".join(str(part.get("name")) for part in excluded)
        mart["覆盖说明"] = claim["coverage_text"]
        mart["来源快照"] = ",".join(
            str(item.get("cache_id")) for item in manifest.get("source_snapshot", [])
        )
        mart["发布清单"] = claim_id
        mart["行数口径"] = manifest.get("row_count_semantics") or export_files.ROW_COUNT_SEMANTICS
        mart["导出任务"] = manifest["export_id"]
        mart["申请编号"] = manifest.get("request_id") or mart.get("申请编号", "")

        export_files.save_manifest(manifest)
        _verify_contract(mart_id, published > 0)
        _audit_mart_claim(mart_id, claim)

        return {
            "ok": True,
            "mart_id": mart_id,
            "claim_id": claim_id,
            "export_id": manifest["export_id"],
            "request_id": manifest.get("request_id"),
            "exported_rows": published,
            "published_row_count": published,
            "valid_partitions": [part.get("name") for part in valid],
            "excluded_partitions": [part.get("name") for part in excluded],
            "coverage": claim["coverage_text"],
            "files": claim["files"],
            "row_count_semantics": mart["行数口径"],
            "source_snapshot": manifest.get("source_snapshot", []),
            "row_count_list_check": rowcount_audit,
        }

    def reconcile_request(self, request_id: str) -> dict[str, Any]:
        """按同一申请核对多次领取的覆盖范围，按分区去重后汇总已发布行数。

        * 行数以分区回执为准，跨多次领取同一分区只计一份（取最大已确认行数）；
        * 每个领取回执里的 CSV 快照会重新数一遍记录数，与当时声明的行数比对；
        * 未闭合分区明确排除，不计入已发布行数。
        """
        manifests = _manifests_for_request(request_id)
        if not manifests:
            return {"ok": False, "message": "未找到该申请的导出清单"}

        claims = export_files.load_claim_receipts(request_id)
        claims.sort(key=lambda item: (str(item.get("claimed_at")), str(item.get("receipt_id"))))

        claim_checks = [_verify_claim_files(claim) for claim in claims]

        by_partition: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
        for manifest in manifests:
            valid, excluded = _valid_partitions(manifest)
            for part in valid:
                name = str(part.get("name"))
                entry = by_partition.setdefault(
                    name,
                    {
                        "partition": name,
                        "status": PARTITION_CONFIRMED,
                        "row_count": int(part.get("row_count", 0)),
                        "receipt_id": part.get("receipt_id"),
                        "file": part.get("file"),
                        "checksum": part.get("checksum"),
                        "claims": [],
                        "excluded": False,
                    },
                )
                # 同一分区多次领取只保留一份（已确认的最大行数）。
                entry["row_count"] = max(entry["row_count"], int(part.get("row_count", 0)))
                for claim in claims:
                    if (
                        any(item.get("partition") == name for item in claim.get("files", []))
                        and claim["receipt_id"] not in entry["claims"]
                    ):
                        entry["claims"].append(claim["receipt_id"])
            for part in excluded:
                by_partition.setdefault(
                    str(part.get("name")),
                    {
                        "partition": part.get("name"),
                        "status": part.get("status"),
                        "row_count": 0,
                        "receipt_id": "",
                        "file": part.get("file"),
                        "checksum": "",
                        "claims": [],
                        "excluded": True,
                        "reason": part.get("note") or "未闭合，不计入已发布行数",
                    },
                )

        # 按领取时的 CSV 快照独立核算：同一分区同样只计一份。
        csv_rows: "OrderedDict[str, int]" = OrderedDict()
        for check in claim_checks:
            for item in check["files"]:
                if item["complete"] and item["ok"]:
                    csv_rows[item["partition"]] = int(item["row_count"])

        confirmed = [item for item in by_partition.values() if not item.get("excluded")]
        total = sum(int(item.get("row_count", 0)) for item in confirmed)
        csv_total = sum(csv_rows.values())
        discrepancies = [
            {
                "partition": item["partition"],
                "declared": item["declared_row_count"],
                "measured": item["row_count"],
                "reasons": item["reasons"],
            }
            for check in claim_checks
            for item in check["files"]
            if not item["ok"]
        ]
        return {
            "ok": True,
            "request_id": request_id,
            "export_ids": [manifest["export_id"] for manifest in manifests],
            "partitions": list(by_partition.values()),
            "published_row_count": total,
            "reconciled_row_count": total,
            "csv_reconciled_row_count": csv_total,
            "csv_matches_receipts": csv_total == total and not discrepancies,
            "discrepancies": discrepancies,
            "claim_count": len(claims),
            "claims": claims,
            "claim_checks": claim_checks,
            "coverage": export_files.coverage_text(
                confirmed, [item for item in by_partition.values() if item.get("excluded")]
            ),
            "row_count_semantics": export_files.ROW_COUNT_SEMANTICS,
        }

    def request_receipts(self, request_id: str) -> dict[str, Any]:
        """按申请返回全部集市回执、可领取 CSV 与覆盖说明，供核账追溯。"""
        manifests = _manifests_for_request(request_id)
        claims = export_files.load_claim_receipts(request_id)
        return {
            "ok": True,
            "request_id": request_id,
            "export_ids": [manifest["export_id"] for manifest in manifests],
            "receipts": claims,
            "claim_count": len(claims),
        }

    def claim_receipt(self, claim_id: str) -> dict[str, Any] | None:
        return export_files.load_claim_receipt(claim_id)

    def manifest_for_export(self, export_id: int) -> dict[str, Any] | None:
        manifest = export_files.load_manifest(export_id)
        if manifest is None:
            return None
        return {**manifest, "row_count_list_check": _cross_check_row_count_list(manifest)}

    def row_count_listing(self, export_id: int) -> dict[str, Any]:
        manifest = export_files.load_manifest(export_id)
        rows = export_files.read_row_count_list(export_id)
        return {
            "ok": manifest is not None,
            "export_id": export_id,
            "manifest": str(export_files.manifest_path(export_id)) if manifest else None,
            "row_count_list": str(export_files.rowcount_path(export_id)),
            "published_row_count": _published_rows(manifest) if manifest else 0,
            "list": rows,
            "check": _cross_check_row_count_list(manifest) if manifest else None,
        }

    def partition_file(self, export_id: int, partition: str) -> Path | None:
        """只允许领取已确认且磁盘核对通过的分区文件。"""
        manifest = export_files.load_manifest(export_id)
        if manifest is None:
            return None
        valid, _ = _valid_partitions(manifest)
        for part in valid:
            if str(part.get("name")) == partition:
                path = Path(str(part.get("file", "")))
                return path if path.is_file() else None
        return None


# ---------------------------------------------------------------------------
# 分区确认与合并
# ---------------------------------------------------------------------------

def _cross_check_row_count_list(manifest: dict[str, Any]) -> dict[str, Any]:
    """对照人可读行数清单与 manifest：已发布分区和行数必须一致。"""
    listing = export_files.read_row_count_list(int(manifest["export_id"]))
    listed = {
        str(row.get("分区")): {
            "row_count": int(row.get("行数") or 0),
            "published": str(row.get("已发布")) == "1",
            "receipt_id": row.get("回执编号", ""),
        }
        for row in listing
    }
    mismatches: list[str] = []
    for part in manifest.get("partitions", []):
        name = str(part.get("name"))
        row = listed.get(name)
        if row is None:
            mismatches.append(f"{name}: 行数清单缺失")
            continue
        if row["row_count"] != int(part.get("row_count", 0)):
            mismatches.append(
                f"{name}: 清单 {row['row_count']} 行，manifest {part.get('row_count', 0)} 行"
            )
        is_published = part.get("status") == PARTITION_CONFIRMED
        if row["published"] != is_published:
            mismatches.append(f"{name}: 已发布标记不一致")
        if is_published and row["receipt_id"] != str(part.get("receipt_id", "")):
            mismatches.append(f"{name}: 回执编号不一致")
    return {
        "ok": not mismatches,
        "row_count_list": str(export_files.rowcount_path(int(manifest["export_id"]))),
        "listed_partitions": list(listed),
        "mismatches": mismatches,
    }


def _verify_claim_files(claim: dict[str, Any]) -> dict[str, Any]:
    """按领取回执里的 CSV 快照重新数记录数，与当时声明行数比对。"""
    files: list[dict[str, Any]] = []
    for item in claim.get("files", []):
        path = Path(str(item.get("snapshot") or item.get("file") or ""))
        declared = int(item.get("row_count", 0))
        if not str(path) or not path.is_file():
            files.append(
                {
                    "partition": item.get("partition"),
                    "declared_row_count": declared,
                    "row_count": 0,
                    "complete": False,
                    "ok": False,
                    "reasons": ["领取文件不存在"],
                    "file": str(path),
                }
            )
            continue
        scan = export_files.scan_partition_file(
            path, "jsonl" if path.suffix.lower() in (".json", ".jsonl") else "csv"
        )
        reasons: list[str] = []
        if scan["trailing_incomplete"]:
            reasons.append("领取文件末尾存在未闭合记录")
        if scan["data_rows"] != declared:
            reasons.append(f"CSV 记录数 {scan['data_rows']} 与回执 {declared} 不一致")
        files.append(
            {
                "partition": item.get("partition"),
                "declared_row_count": declared,
                "row_count": scan["data_rows"],
                "complete": not scan["trailing_incomplete"],
                "ok": not reasons,
                "reasons": reasons,
                "file": str(path),
            }
        )
    return {
        "claim_id": claim.get("receipt_id"),
        "claimed_at": claim.get("claimed_at"),
        "coverage_text": claim.get("coverage_text"),
        "files": files,
        "ok": all(item["ok"] for item in files) if files else True,
        "row_count": sum(item["row_count"] for item in files),
    }


def _confirm_partition(
    writer: export_files.PartitionWriter, manifest: dict[str, Any], name: str
) -> dict[str, Any]:
    metrics = writer.finish(name)
    if not metrics["complete"]:
        return {
            "name": name,
            "status": PARTITION_PARTIAL,
            "row_count": 0,
            "record_count": metrics["record_count"],
            "bytes": metrics["bytes"],
            "checksum": metrics["checksum"],
            "file": metrics["file"],
            "receipt_id": "",
            "confirmed_at": "",
            "note": "文件未闭合，未写回执",
        }
    receipt = export_files.make_partition_receipt(
        manifest,
        name,
        row_count=metrics["row_count"],
        checksum=metrics["checksum"],
        path=Path(metrics["file"]),
        fields=manifest.get("fields", []),
    )
    export_files.save_receipt(receipt)
    return {
        "name": name,
        "status": PARTITION_CONFIRMED,
        "row_count": metrics["row_count"],
        "record_count": metrics["record_count"],
        "bytes": metrics["bytes"],
        "checksum": metrics["checksum"],
        "file": metrics["file"],
        "receipt_id": receipt["receipt_id"],
        "confirmed_at": receipt["confirmed_at"],
        "note": "空分区已读完，写回执" if metrics["row_count"] == 0 else "",
    }


def _reusable(part: dict[str, Any] | None) -> bool:
    """已确认且磁盘核对通过的分区可沿用原回执。"""
    return bool(
        part
        and part.get("status") == PARTITION_CONFIRMED
        and export_files.verify_partition_file(part)["ok"]
    )


def _ordered_partitions(
    order: list[str],
    reused: list[dict[str, Any]],
    finished: list[dict[str, Any]],
    existing: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for part in reused:
        merged[str(part["name"])] = part
    for part in finished:
        # 本轮重新落盘的分区覆盖旧回执。
        merged[str(part["name"])] = part
    for name, part in existing.items():
        if name not in merged and part.get("status") == PARTITION_CONFIRMED:
            merged[name] = part
    index = {name: position for position, name in enumerate(order)}
    return sorted(
        merged.values(), key=lambda part: index.get(str(part["name"]), len(index))
    )


# ---------------------------------------------------------------------------
# 数据源：两遍流式读取
# ---------------------------------------------------------------------------

class _ExportSource:
    """查询明细的流式视图：第一遍统计分区，第二遍按分区逐块产出。

    result_cache 的明细不会在导出时被复制成一份完整的合并列表；每遍只按
    缓存条目和行块推进，峰值内存与单个分区块成正比。
    """

    def __init__(
        self,
        batches: list[list[dict[str, Any]]],
        snapshot: list[dict[str, Any]],
        *,
        error: str | None = None,
    ) -> None:
        self._batches = batches
        self.snapshot = snapshot
        self.error = error
        self.fields: list[str] = []
        self.sample = batches[0][0] if batches and batches[0] else None
        self.has_rows = any(batch for batch in batches)

    def _iter_rows(self) -> Iterable[dict[str, Any]]:
        for batch in self._batches:
            for row in batch:
                yield row

    def partition_order(self, partition_field: str | None) -> list[str]:
        order: list[str] = []
        seen: set[str] = set()
        for row in self._iter_rows():
            name = _partition_key(row, partition_field)
            if name not in seen:
                seen.add(name)
                order.append(name)
        if not order:
            order.append("全部")
        return order

    def iter_partition(
        self, partition_field: str | None, partition: str
    ) -> Iterable[dict[str, Any]]:
        for row in self._iter_rows():
            if _partition_key(row, partition_field) == partition:
                yield row


def _partition_key(row: dict[str, Any], partition_field: str | None) -> str:
    key = str(row.get(partition_field, "")) if partition_field else "全部"
    return key or "空白"


def _export_source(
    entry: dict[str, Any], query_result_ids: list[int] | None
) -> _ExportSource:
    """按缓存条目顺序收集明细块与来源快照。"""
    batches: list[list[dict[str, Any]]] = []
    snapshot: list[dict[str, Any]] = []
    for cache in store.rows("result_cache"):
        if query_result_ids and cache["id"] not in query_result_ids:
            continue
        if cache.get("status") != "有效":
            continue
        data = cache.get("result_data", cache.get("cached_data", []))
        if not isinstance(data, list) or not data:
            continue
        snapshot.append(
            {
                "cache_id": cache["id"],
                "entry": cache.get("条目编号"),
                "summary": cache.get("查询摘要"),
                "hash": cache.get("结果哈希"),
                "row_count": len(data),
            }
        )
        batches.append(data)

    if batches:
        return _ExportSource(batches, snapshot)
    if query_result_ids:
        return _ExportSource([], snapshot, error="指定的查询结果为空")

    # 没有查询缓存时生成模拟数据，保证导出管线可演示。
    generated = [
        {
            "row_id": i + 1,
            "metric": f"metric_{random.choice(['A', 'B', 'C'])}",
            "value": random.randint(1, 1000),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        for i in range(3, 15)
    ]
    return _ExportSource([generated], snapshot)


def _published_rows(manifest: dict[str, Any]) -> int:
    return sum(
        int(part.get("row_count", 0))
        for part in manifest.get("partitions", [])
        if part.get("status") == PARTITION_CONFIRMED
    )


def _valid_partitions(
    manifest: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """读出已确认分区并逐个核对磁盘，未闭合/校验失败的不计入发布。"""
    semantics = manifest.get("row_count_semantics")
    legacy = semantics in (None, "", export_files.LEGACY_ROW_COUNT_SEMANTICS)
    valid: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for part in manifest.get("partitions", []):
        if legacy:
            # 历史快照没有分区状态/校验和：以 CSV 实际完整记录数为准。
            verify = export_files.verify_partition_file(
                {**part, "row_count": int(part.get("row_count", 0)), "checksum": ""}
            )
            if not verify["complete"]:
                excluded.append({**part, "note": "；".join(verify["reasons"]) or "文件未闭合"})
                continue
            valid.append({**part, "row_count": verify["rows"]})
            continue
        if part.get("status") != PARTITION_CONFIRMED:
            excluded.append(part)
            continue
        verify = export_files.verify_partition_file(part)
        if not verify["ok"]:
            excluded.append({**part, "note": "；".join(verify["reasons"])})
            continue
        valid.append(part)
    return valid, excluded


def _resolve_manifest(
    *, export_id: int | None = None, request_id: str | None = None
) -> dict[str, Any] | None:
    """定位要发布的导出清单。

    必须由 export_id 或 request_id 指定；两者都没有时返回 None，
    让集市按历史快照的原有口径读取，避免误把别的申请发布出去。
    """
    if export_id is not None:
        manifest = export_files.load_manifest(export_id)
        if manifest is not None:
            return manifest
    if request_id:
        for row in store.rows("data_exporter"):
            if str(row.get("任务编号")) != request_id:
                continue
            manifest = export_files.load_manifest(int(row.get("id", 0)))
            if manifest is not None and manifest.get("request_id") == request_id:
                return manifest
        manifests = _manifests_for_request(request_id)
        return manifests[0] if manifests else None
    return None


def _manifests_for_request(request_id: str) -> list[dict[str, Any]]:
    manifests: list[dict[str, Any]] = []
    for path in export_files.export_root().glob("manifest_export_*.json"):
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if manifest.get("request_id") == request_id and manifest not in manifests:
            manifests.append(manifest)
    return manifests


# ---------------------------------------------------------------------------
# 集市 / 审计 / 契约
# ---------------------------------------------------------------------------

def _legacy_batch_export(
    mart: dict[str, Any], filter_criteria: dict[str, str] | None
) -> dict[str, Any]:
    """没有导出清单的历史快照：沿用原有 result_cache 行数口径。"""
    cached = _get_all_valid_cache()
    exported = 0
    for cache_entry in cached:
        data = cache_entry.get("result_data", cache_entry.get("cached_data", []))
        if not data:
            continue
        if filter_criteria:
            data = _filter_data(data, filter_criteria)
        exported += len(data)

    mart["行数统计"] = str(exported)
    mart["最后刷新"] = export_files.now_text()
    mart["status"] = "可用"
    mart["pending"] = False
    mart["行数口径"] = export_files.LEGACY_ROW_COUNT_SEMANTICS
    mart["有效分区"] = ""
    mart["覆盖说明"] = "历史快照：无分区清单，按缓存全量行数统计。"
    _verify_contract(int(mart.get("id", 0)), exported > 0)
    return {
        "ok": True,
        "mart_id": mart.get("id"),
        "exported_rows": exported,
        "cached_sources": len(cached),
        "row_count_semantics": export_files.LEGACY_ROW_COUNT_SEMANTICS,
    }


def _write_mart_entry(
    export_id: int,
    fmt: str,
    row_count: int,
    file_size: int,
    *,
    request_id: str | None = None,
    semantics: str | None = None,
) -> None:
    rows = store.rows("result_mart")
    existing = next(
        (
            row
            for row in rows
            if row.get("导出任务") == export_id
            and row.get("集市名称") == f"导出任务{export_id}_{fmt}"
        ),
        None,
    )
    if existing is not None:
        existing["行数统计"] = str(row_count)
        existing["刷新周期"] = "manual"
        existing["最后刷新"] = export_files.now_text()
        existing["行数口径"] = semantics or export_files.ROW_COUNT_SEMANTICS
        existing["申请编号"] = request_id or existing.get("申请编号", "")
        return

    mart_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
    rows.append(
        {
            "id": mart_id,
            "集市编号": f"RESU-X{export_id:04d}",
            "集市名称": f"导出任务{export_id}_{fmt}",
            "数据来源": f"data_exporter:{export_id}",
            "主题域": fmt,
            "行数统计": str(row_count),
            "刷新周期": "manual",
            "下游消费": "external",
            "最后刷新": export_files.now_text(),
            "status": "可用" if row_count > 0 else "刷新中",
            "pending": row_count == 0,
            "abnormal": False,
            "导出任务": export_id,
            "申请编号": request_id or "",
            "行数口径": semantics or export_files.ROW_COUNT_SEMANTICS,
        }
    )


def _audit_export(
    export_id: int,
    fmt: str,
    elapsed_ms: float,
    row_count: int,
    file_size: int,
    *,
    statement: str | None = None,
    status: str = "正常",
    abnormal: bool = False,
) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    rows.append(
        {
            "id": audit_id,
            "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
            "查询语句": statement or f"EXPORT id={export_id} fmt={fmt}",
            "执行耗时": f"{elapsed_ms:.0f}ms",
            "扫描行数": str(row_count),
            "返回行数": str(row_count),
            "数据来源": "data_exporter",
            "操作用户": "exporter",
            "查询时间": export_files.now_text(),
            "status": status,
            "pending": False,
            "abnormal": abnormal,
        }
    )


def _audit_mart_claim(mart_id: int, claim: dict[str, Any]) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    rows.append(
        {
            "id": audit_id,
            "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
            "查询语句": (
                f"MART_CLAIM mart={mart_id} export={claim.get('export_id')} "
                f"claim={claim.get('receipt_id')} rows={claim.get('claimed_rows')}"
            ),
            "执行耗时": "0ms",
            "扫描行数": str(claim.get("claimed_rows", 0)),
            "返回行数": str(claim.get("claimed_rows", 0)),
            "数据来源": "result_mart",
            "操作用户": "mart_consumer",
            "查询时间": export_files.now_text(),
            "status": "正常",
            "pending": False,
            "abnormal": False,
        }
    )


def _get_all_valid_cache() -> list[dict[str, Any]]:
    return [c for c in store.rows("result_cache") if c.get("status") == "有效"]


def _filter_data(data: list[dict[str, Any]], criteria: dict[str, str]) -> list[dict[str, Any]]:
    result = data
    for key, value in criteria.items():
        result = [r for r in result if str(r.get(key, "")).find(value) >= 0]
    return result


def _verify_contract(mart_id: int, has_data: bool) -> None:
    contracts = store.rows("data_contract")
    for c in contracts:
        if c.get("status") == "已承诺" and str(mart_id) in str(c.get("数据集", "")):
            if not has_data:
                c["status"] = "已违约"
