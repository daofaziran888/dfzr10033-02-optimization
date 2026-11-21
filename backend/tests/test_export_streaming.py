"""分析导出 → 结果集市的分段导出、回执与核账测试。"""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services import export_files
from app.services.data_exporter import DataExporterService
from app.services.export_files import (
    PARTITION_CONFIRMED,
    PARTITION_PARTIAL,
    scan_csv_file,
)
from app.services.result_mart import ResultMartService
from app.store import store

NORTH = "北区"
SOUTH = "南区"
REQUEST = "REQ-202611"
FIELDS = ["请求编号", "区域", "备注", "金额"]
SOUTH_TRICKY_NOTE = '第一行\n第二行,带引号"x"'

client = TestClient(app)


@pytest.fixture(autouse=True)
def isolated_exports(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_EXPORT_DIR", str(tmp_path / "exports"))
    store.rows("result_cache").clear()
    store.rows("result_cache").extend(
        [
            {
                "id": 101,
                "条目编号": "CACHE-N",
                "查询摘要": "north",
                "结果哈希": "hash-n",
                "status": "有效",
                "result_data": [
                    {"请求编号": REQUEST, "区域": NORTH, "备注": "北-1", "金额": "10"},
                    {"请求编号": REQUEST, "区域": NORTH, "备注": "北-2", "金额": "11"},
                ],
            },
            {
                "id": 102,
                "条目编号": "CACHE-S",
                "查询摘要": "south",
                "结果哈希": "hash-s",
                "status": "有效",
                "result_data": [
                    {
                        "请求编号": REQUEST,
                        "区域": SOUTH,
                        "备注": SOUTH_TRICKY_NOTE,
                        "金额": "20",
                    },
                    {"请求编号": REQUEST, "区域": SOUTH, "备注": "南-2", "金额": "21"},
                ],
            },
        ]
    )
    yield
    store.rows("result_cache").clear()


def _create_export(field_spec: str = ",".join(FIELDS)) -> dict:
    entry, missing = DataExporterService().create_entry(
        {
            "任务编号": REQUEST,
            "数据来源": "result_cache",
            "输出格式": "csv",
            "字段列表": field_spec,
        }
    )
    assert entry is not None, missing
    return entry


def _create_mart() -> dict:
    mart, missing = ResultMartService().create_entry(
        {"集市编号": "MART-001", "集市名称": "月度合账", "数据来源": "result_cache"}
    )
    assert mart is not None, missing
    return mart


def _part(manifest: dict, name: str) -> dict:
    return next(part for part in manifest["partitions"] if part["name"] == name)


class TestStreamingPartitionExport:
    def test_field_order_follows_request_and_csv_escapes(self):
        entry = _create_export(field_spec="金额,区域,备注,请求编号")
        result = DataExporterService().export_query_results(
            entry["id"], partition_by="区域"
        )
        assert result["ok"]
        rows = list(csv.reader(open(_part(result, NORTH)["file"], encoding="utf-8")))
        assert rows[0] == ["金额", "区域", "备注", "请求编号"]
        assert rows[1][2] == "北-1"

        south_rows = list(csv.reader(open(_part(result, SOUTH)["file"], encoding="utf-8")))
        # 带换行与引号的记录被完整转义为一条 CSV 记录。
        assert south_rows[1][2] == SOUTH_TRICKY_NOTE
        assert south_rows[1][0] == "20"
        assert len(south_rows) == 3

    def test_interrupt_leaves_unclosed_partition_without_receipt(self):
        entry = _create_export()
        result = DataExporterService().export_query_results(
            entry["id"],
            partition_by="区域",
            interrupt={"partition": SOUTH, "after_rows": 0},
        )
        assert result["ok"] is False
        north = _part(result, NORTH)
        south = _part(result, SOUTH)
        assert north["status"] == PARTITION_CONFIRMED and north["receipt_id"]
        assert south["status"] == PARTITION_PARTIAL and not south["receipt_id"]
        assert result["published_row_count"] == 2

        on_disk = Path(south["file"])
        scan = scan_csv_file(on_disk)
        assert scan["trailing_incomplete"] is True
        assert export_files.load_receipt(north["receipt_id"]) is not None

    def test_empty_partition_gets_receipt(self):
        entry = _create_export()
        result = DataExporterService().export_query_results(
            entry["id"], partition_by="区域", partitions=["空区"]
        )
        empty = _part(result, "空区")
        assert empty["status"] == PARTITION_CONFIRMED
        assert empty["row_count"] == 0
        receipt = export_files.load_receipt(empty["receipt_id"])
        assert receipt["row_count"] == 0
        assert receipt["complete"] is True

    def test_resume_reuses_confirmed_receipt(self):
        entry = _create_export()
        svc = DataExporterService()
        first = svc.export_query_results(
            entry["id"],
            partition_by="区域",
            interrupt={"partition": SOUTH, "after_rows": 0},
        )
        north_receipt = _part(first, NORTH)["receipt_id"]

        resumed = svc.resume_export(entry["id"])
        assert resumed["ok"] is True
        assert _part(resumed, NORTH)["receipt_id"] == north_receipt
        assert resumed["published_row_count"] == 4

    def test_row_count_list_matches_manifest(self):
        entry = _create_export()
        DataExporterService().export_query_results(entry["id"], partition_by="区域")
        listing = export_files.read_row_count_list(entry["id"])
        names = {row["分区"] for row in listing}
        assert names == {NORTH, SOUTH}
        assert all(row["已发布"] == "1" for row in listing)
        assert all(row["回执编号"] for row in listing)

    def test_truncated_confirmed_file_is_excluded_on_publication(self):
        entry = _create_export()
        mart = _create_mart()
        svc = DataExporterService()
        first = svc.export_query_results(entry["id"], partition_by="区域")
        assert first["ok"]

        # 磁盘上的南区文件被截断，即使 manifest 仍标记 confirmed 也不能发布。
        south_path = Path(_part(first, SOUTH)["file"])
        data = south_path.read_bytes()
        south_path.write_bytes(data[: len(data) // 2])

        published = svc.batch_export_to_mart(mart["id"], request_id=REQUEST)
        assert published["valid_partitions"] == [NORTH]
        assert SOUTH in published["excluded_partitions"]
        assert published["exported_rows"] == 2

    def test_row_count_list_check_detects_tampering(self):
        entry = _create_export()
        DataExporterService().export_query_results(entry["id"], partition_by="区域")
        listing_path = export_files.rowcount_path(entry["id"])
        text = listing_path.read_text(encoding="utf-8")
        listing_path.write_text(
            text.replace(f"{NORTH},confirmed,2", f"{NORTH},confirmed,99"), encoding="utf-8"
        )
        manifest = DataExporterService().manifest_for_export(entry["id"])
        assert manifest["row_count_list_check"]["ok"] is False
        assert any(NORTH in item for item in manifest["row_count_list_check"]["mismatches"])


class TestMartPublicationAndReconcile:
    def test_claim_before_and_after_recovery(self):
        entry = _create_export()
        mart = _create_mart()
        svc = DataExporterService()
        svc.export_query_results(
            entry["id"],
            partition_by="区域",
            interrupt={"partition": SOUTH, "after_rows": 0},
        )

        before = svc.batch_export_to_mart(mart["id"], request_id=REQUEST)
        assert before["exported_rows"] == 2
        assert before["valid_partitions"] == [NORTH]
        assert before["excluded_partitions"] == [SOUTH]
        assert "北区" in before["coverage"] and "南区" in before["coverage"]
        assert mart["行数统计"] == "2"
        assert mart["行数口径"] == export_files.ROW_COUNT_SEMANTICS

        receipt = svc.claim_receipt(before["claim_id"])
        assert receipt is not None
        assert [item["partition"] for item in receipt["files"]] == [NORTH]
        assert Path(receipt["files"][0]["snapshot"]).is_file()

        svc.resume_export(entry["id"])
        after = svc.batch_export_to_mart(mart["id"], request_id=REQUEST)
        assert after["exported_rows"] == 4
        assert set(after["valid_partitions"]) == {NORTH, SOUTH}
        assert after["excluded_partitions"] == []

        reconciled = svc.reconcile_request(REQUEST)
        assert reconciled["published_row_count"] == 4
        assert reconciled["csv_reconciled_row_count"] == 4
        assert reconciled["csv_matches_receipts"] is True
        # 北区两次领取都出现，但只计一份。
        north = next(
            item for item in reconciled["partitions"] if item["partition"] == NORTH
        )
        assert north["row_count"] == 2
        assert len(north["claims"]) == 2
        assert northern_total(reconciled) == 2

    def test_claim_api_links_csv_and_coverage(self):
        entry = _create_export()
        mart = _create_mart()
        svc = DataExporterService()
        svc.export_query_results(
            entry["id"],
            partition_by="区域",
            interrupt={"partition": SOUTH, "after_rows": 0},
        )

        claim = client.post(
            f"/api/marts/{mart['id']}/claim", params={"request_id": REQUEST}
        ).json()
        receipt = client.get(f"/api/marts/receipts/{claim['claim_id']}").json()
        assert receipt["request_id"] == REQUEST
        assert "本次可领取分区" in receipt["coverage_text"]
        assert receipt["claimed_rows"] == 2

        north_file = client.get(f"/api/exports/{entry['id']}/files/{NORTH}")
        assert north_file.status_code == 200
        assert north_file.content.decode("utf-8").startswith("请求编号,区域,备注,金额")

        # 未闭合的南区此刻不可领取。
        assert client.get(f"/api/exports/{entry['id']}/files/{SOUTH}").status_code == 404

        client.post(f"/api/exports/{entry['id']}/resume")
        client.post(f"/api/marts/{mart['id']}/claim", params={"request_id": REQUEST})
        reconciled = client.get(
            f"/api/exports/requests/{REQUEST}/reconcile"
        ).json()
        assert reconciled["published_row_count"] == 4
        assert reconciled["csv_matches_receipts"] is True

    def test_historical_snapshot_uses_legacy_row_count(self):
        mart = _create_mart()
        result = DataExporterService().batch_export_to_mart(mart["id"])
        assert result["row_count_semantics"] == export_files.LEGACY_ROW_COUNT_SEMANTICS
        # 没有导出清单时按缓存有效行数统计（北区 2 + 南区 2）。
        assert result["exported_rows"] == 4
        assert mart["行数口径"] == export_files.LEGACY_ROW_COUNT_SEMANTICS

    def test_publication_without_selector_stays_legacy_even_with_manifest(self):
        entry = _create_export()
        mart = _create_mart()
        svc = DataExporterService()
        svc.export_query_results(
            entry["id"],
            partition_by="区域",
            interrupt={"partition": SOUTH, "after_rows": 0},
        )
        # 未指定 export_id / request_id 时不应误用某个清单，按历史快照口径读。
        result = svc.batch_export_to_mart(mart["id"])
        assert result["row_count_semantics"] == export_files.LEGACY_ROW_COUNT_SEMANTICS
        assert result["exported_rows"] == 4

    def test_manifest_endpoint_reports_partition_state(self):
        entry = _create_export()
        DataExporterService().export_query_results(
            entry["id"],
            partition_by="区域",
            interrupt={"partition": SOUTH, "after_rows": 0},
        )
        manifest = client.get(f"/api/exports/{entry['id']}/manifest").json()
        assert manifest["status"] == "partial"
        assert manifest["published_partitions"] == [NORTH]
        assert manifest["row_count_list_check"]["ok"] is True
        assert manifest["row_count_semantics"] == export_files.ROW_COUNT_SEMANTICS
        assert manifest["source_snapshot"][0]["cache_id"] == 101


def northern_total(reconciled: dict) -> int:
    return next(
        item["row_count"]
        for item in reconciled["partitions"]
        if item["partition"] == NORTH
    )
