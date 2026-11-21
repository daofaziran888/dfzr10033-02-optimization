"""导出文件与行数清单：分段 CSV 转换、分区回执、清单持久化与核对。

这个模块只处理文件，不依赖业务服务，方便 data_exporter 与 result_mart 共用：

* 分段写入：查询明细按块流式序列化到各分区临时文件，不拼整份字符串；
* 分区回执：只有分区最后一段落盘且文件闭合后才确认并写回执，空分区也有回执；
* 行数清单：manifest JSON 之外再生成一份人可读的 rowcount CSV，方便核账；
* 核对工具：校验和、CSV 记录数、文件是否以完整换行收尾。
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable

MANIFEST_SCHEMA_VERSION = 2
ROW_COUNT_SEMANTICS = "confirmed_closed_partitions_v2"
LEGACY_ROW_COUNT_SEMANTICS = "legacy_all_rows"

PARTITION_CONFIRMED = "confirmed"
PARTITION_PARTIAL = "partial"
PARTITION_PENDING = "pending"

_SAFE_NAME = re.compile(r"[^0-9A-Za-z_.\-一-鿿]+")


class ExportInterrupted(RuntimeError):
    """模拟交付中断：分区文件只落了一部分，尚未闭合。"""

    def __init__(self, partition: str, reason: str = "simulated_interruption") -> None:
        super().__init__(f"export interrupted in partition {partition}: {reason}")
        self.partition = partition
        self.reason = reason


# ---------------------------------------------------------------------------
# 路径
# ---------------------------------------------------------------------------

def export_root() -> Path:
    """导出根目录。测试/部署可用 DATA_EXPORT_DIR 或 EXPORT_DIR 覆盖。"""
    raw = os.environ.get("DATA_EXPORT_DIR") or os.environ.get("EXPORT_DIR")
    root = Path(raw) if raw else Path(tempfile.gettempdir()) / "lakehouse_exports"
    root.mkdir(parents=True, exist_ok=True)
    return root


def manifest_path(export_id: int) -> Path:
    return export_root() / f"manifest_export_{export_id}.json"


def rowcount_path(export_id: int) -> Path:
    return export_root() / f"rowcount_export_{export_id}.csv"


def partition_path(export_id: int, partition: str, fmt: str) -> Path:
    return export_root() / f"export_{export_id}_{safe_name(partition)}.{fmt}"


def receipt_dir() -> Path:
    path = export_root() / "receipts"
    path.mkdir(parents=True, exist_ok=True)
    return path


def receipt_path(receipt_id: str) -> Path:
    return receipt_dir() / f"receipt_{safe_name(receipt_id)}.json"


def claim_dir() -> Path:
    path = export_root() / "claims"
    path.mkdir(parents=True, exist_ok=True)
    return path


def claim_path(receipt_id: str) -> Path:
    return claim_dir() / f"claim_{safe_name(receipt_id)}.json"


def claim_snapshot_dir(receipt_id: str) -> Path:
    path = claim_dir() / safe_name(receipt_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def snapshot_claim_file(claim_id: str, partition: dict[str, Any]) -> str:
    """领取时复制一份 CSV 快照，后续续跑改写原文件也能按回执核对。"""
    source = Path(str(partition.get("file", "")))
    if not source.is_file():
        return ""
    target = claim_snapshot_dir(claim_id) / source.name
    atomic_copy_file(source, target)
    return str(target)


def safe_name(value: str) -> str:
    cleaned = _SAFE_NAME.sub("_", str(value)).strip("_")
    return cleaned or "partition"


def now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# 字段顺序与分段转换
# ---------------------------------------------------------------------------

def field_order(field_spec: str | None, sample: dict[str, Any] | None = None) -> list[str]:
    """字段顺序沿用申请值；只有 * / 空时才回退到首行字段。"""
    if field_spec:
        text = str(field_spec).strip()
        if text and text != "*":
            return [part.strip() for part in text.split(",") if part.strip()]
    return list(sample.keys()) if sample else []


def serialise_row(values: Iterable[Any]) -> str:
    """单行 CSV 序列化（不含行终止符），引号与换行交给 csv 模块转义。"""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="")
    writer.writerow(list(values))
    return buf.getvalue()


class _HashingWriter:
    """边写边算大小和摘要，校验和不用再读一遍整份文件。"""

    def __init__(self, handle: Any) -> None:
        self.handle = handle
        self.digest = hashlib.md5()
        self.bytes = 0

    def write(self, text: str) -> None:
        data = text.encode("utf-8")
        self.handle.write(data)
        self.digest.update(data)
        self.bytes += len(data)

    def checksum(self) -> str:
        return self.digest.hexdigest()


class PartitionWriter:
    """把流式行数据分段写进各分区文件。

    调用方先给出期望行数（expected），每写满一个分区就立即收尾并返回该分区
    的落盘指标；这样“最后一段落完”即确认，不需要先把全部明细留在内存里。
    """

    def __init__(
        self,
        export_id: int,
        fields: list[str],
        *,
        fmt: str = "csv",
        chunk_size: int = 500,
    ) -> None:
        self.export_id = export_id
        self.fields = fields
        self.fmt = "csv" if fmt == "csv" else "jsonl"
        self.chunk_size = max(int(chunk_size or 0), 0)
        self._parts: dict[str, dict[str, Any]] = {}
        self._order: list[str] = []
        self._interrupt_partition: str | None = None
        self._interrupt_after = 0
        self.abort_info: dict[str, Any] | None = None

    # -- 生命周期 --
    def set_interrupt(self, partition: str | None, after_rows: int = 0) -> None:
        self._interrupt_partition = str(partition) if partition else None
        self._interrupt_after = int(after_rows)

    def add(self, partition: str, row: dict[str, Any]) -> None:
        name = str(partition)
        part = self._open(name)
        if self._interrupt_partition == name and part["rows"] == self._interrupt_after:
            self._break_mid_record(name, part, row)
        if self.fmt == "csv":
            part["writer"].write(
                serialise_row([row.get(field, "") for field in self.fields]) + "\n"
            )
        else:
            part["writer"].write(json.dumps(row, ensure_ascii=False) + "\n")
        part["rows"] += 1
        if self.chunk_size and part["rows"] % self.chunk_size == 0:
            part["handle"].flush()

    def rows_written(self, partition: str) -> int:
        part = self._parts.get(str(partition))
        return int(part["rows"]) if part else 0

    def finish(self, partition: str) -> dict[str, Any]:
        """收尾一个分区：flush → fsync → 原子替换 → 核对完整记录数。"""
        name = str(partition)
        part = self._open(name)
        part["handle"].flush()
        os.fsync(part["handle"].fileno())
        part["handle"].close()
        os.replace(part["tmp"], part["path"])
        scan = scan_partition_file(part["path"], self.fmt)
        metrics = {
            "name": name,
            "format": self.fmt,
            "file": str(part["path"]),
            "row_count": scan["data_rows"] if scan["complete"] else 0,
            "written_rows": int(part["rows"]),
            "record_count": scan["records"],
            "bytes": scan["bytes"],
            "checksum": part["writer"].checksum(),
            "complete": bool(scan["complete"]) and scan["data_rows"] == part["rows"],
        }
        part["finished"] = True
        self._parts.pop(name, None)
        return metrics

    def abort(self) -> dict[str, dict[str, Any]]:
        """中断收尾：未完成分区落盘但不确认，返回其现场指标。"""
        leftovers: dict[str, dict[str, Any]] = {}
        for name, part in list(self._parts.items()):
            if part["handle"] and not part["handle"].closed:
                try:
                    part["handle"].flush()
                    os.fsync(part["handle"].fileno())
                finally:
                    part["handle"].close()
            if part["tmp"].exists():
                os.replace(part["tmp"], part["path"])
            scan = scan_partition_file(part["path"], self.fmt)
            leftovers[name] = {
                "name": name,
                "format": self.fmt,
                "file": str(part["path"]),
                "row_count": 0,
                "written_rows": int(part["rows"]),
                "record_count": scan["records"],
                "bytes": scan["bytes"],
                "checksum": part["writer"].checksum(),
                "complete": False,
                "trailing_incomplete": scan["trailing_incomplete"],
            }
            self._parts.pop(name, None)
        return leftovers

    # -- 内部 --
    def _open(self, name: str) -> dict[str, Any]:
        if name in self._parts:
            return self._parts[name]
        path = partition_path(self.export_id, name, self.fmt)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        handle = open(tmp, "wb")
        writer = _HashingWriter(handle)
        if self.fmt == "csv":
            writer.write(serialise_row(self.fields) + "\n")
        part = {
            "name": name,
            "path": path,
            "tmp": tmp,
            "handle": handle,
            "writer": writer,
            "rows": 0,
        }
        self._parts[name] = part
        self._order.append(name)
        return part

    def _break_mid_record(
        self, name: str, part: dict[str, Any], row: dict[str, Any]
    ) -> None:
        """写到跨行记录中途，flush 后保留未闭合文件，抛出中断。"""
        if self.fmt == "csv":
            partial = serialise_row([row.get(field, "") for field in self.fields])
            cut = max(1, len(partial) // 2)
            part["writer"].write(partial[:cut])
        part["handle"].flush()
        os.fsync(part["handle"].fileno())
        part["handle"].close()
        os.replace(part["tmp"], part["path"])
        # 中断现场：保留未闭合文件，供续跑识别“读到一半”的分区。
        self.abort_info = {
            "name": name,
            "format": self.fmt,
            "file": str(part["path"]),
            "row_count": 0,
            "written_rows": int(part["rows"]),
            "bytes": part["path"].stat().st_size,
            "checksum": part["writer"].checksum(),
            "complete": False,
            "trailing_incomplete": True,
        }
        self._parts.pop(name, None)
        raise ExportInterrupted(name)


# ---------------------------------------------------------------------------
# 文件完整性与记录数
# ---------------------------------------------------------------------------

def scan_csv_file(path: Path) -> dict[str, Any]:
    """流式扫描 CSV 的完整记录数。

    writer 每条记录都以换行结尾，因此只有“引号外换行”才算闭合记录。
    EOF 时仍在引号内或还有未换行内容，判定为未闭合，不计入 data_rows。
    """
    quote = 0x22
    lf = 0x0A
    cr = 0x0D
    spaces = {0x20, 0x09}
    records = 0
    in_quotes = False
    quote_pending = False
    record_has_content = False
    size = 0

    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            size += len(chunk)
            index = 0
            while index < len(chunk):
                byte = chunk[index]
                if in_quotes:
                    if quote_pending:
                        quote_pending = False
                        if byte == quote:
                            index += 1
                            continue
                        in_quotes = False
                        continue  # 按引号外规则重新处理当前字节
                    if byte == quote:
                        quote_pending = True
                    else:
                        if byte == lf:
                            record_has_content = True
                    index += 1
                    continue
                if byte == quote:
                    in_quotes = True
                    record_has_content = True
                elif byte == lf:
                    records += 1
                    record_has_content = False
                elif byte != cr and byte not in spaces:
                    record_has_content = True
                index += 1

    if quote_pending:
        # 最后一个是闭合引号，只是文件在换行前被截断。
        in_quotes = False
    complete = not in_quotes and not record_has_content
    return {
        "records": records,
        "data_rows": max(records - 1, 0),
        "trailing_incomplete": not complete,
        "bytes": size,
        "complete": complete and records > 0,
    }


def scan_jsonl_file(path: Path) -> dict[str, Any]:
    records = 0
    size = 0
    trailing_incomplete = False
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            size += len(chunk)
            records += chunk.count(b"\n")
            trailing_incomplete = not chunk.endswith(b"\n")
    return {
        "records": records,
        "data_rows": records,
        "trailing_incomplete": trailing_incomplete,
        "bytes": size,
        "complete": records > 0 and not trailing_incomplete,
    }


def scan_partition_file(path: Path, fmt: str = "csv") -> dict[str, Any]:
    if fmt == "csv":
        return scan_csv_file(path)
    return scan_jsonl_file(path)


def file_digest(path: Path) -> str:
    digest = hashlib.md5()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def verify_partition_file(partition: dict[str, Any]) -> dict[str, Any]:
    """核对分区文件：存在、摘要一致、完整记录数与清单一致且已闭合。"""
    path = Path(str(partition.get("file", "")))
    if not str(path) or not path.is_file():
        return {"ok": False, "reasons": ["文件不存在"], "rows": 0, "complete": False}
    fmt = str(partition.get("format") or "csv")
    scan = scan_partition_file(path, fmt)
    digest = file_digest(path)
    reasons: list[str] = []
    if scan["trailing_incomplete"]:
        reasons.append("文件末尾存在未闭合记录")
    if str(partition.get("checksum") or "") and digest != str(partition.get("checksum")):
        reasons.append("校验和与清单不一致")
    if int(partition.get("row_count", -1)) != scan["data_rows"]:
        reasons.append(
            f"行数与清单不一致（清单 {partition.get('row_count')}，文件 {scan['data_rows']}）"
        )
    return {
        "ok": not reasons,
        "reasons": reasons,
        "rows": scan["data_rows"],
        "complete": not scan["trailing_incomplete"],
        "checksum": digest,
        "bytes": scan["bytes"],
    }


# ---------------------------------------------------------------------------
# manifest / 行数清单 / 回执
# ---------------------------------------------------------------------------

def new_manifest(
    export_id: int,
    *,
    request_id: str,
    format: str,
    fields: list[str],
    partition_field: str,
) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "row_count_semantics": ROW_COUNT_SEMANTICS,
        "export_id": export_id,
        "request_id": request_id,
        "format": format,
        "fields": fields,
        "partition_field": partition_field,
        "status": "running",
        "published_row_count": 0,
        "published_partitions": [],
        "partitions": [],
        "source_snapshot": [],
        "claims": [],
        "created_at": now_text(),
        "updated_at": now_text(),
    }


def load_manifest(export_id: int) -> dict[str, Any] | None:
    path = manifest_path(export_id)
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding=encoding, newline="") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def atomic_copy_file(source: Path, target: Path) -> None:
    """按块复制文件，避免把整份 CSV 读进内存。"""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    with open(source, "rb") as src, open(tmp, "wb") as dst:
        while True:
            chunk = src.read(65536)
            if not chunk:
                break
            dst.write(chunk)
        dst.flush()
        os.fsync(dst.fileno())
    os.replace(tmp, target)


def read_file_chunks(path: Path, chunk_size: int = 65536):
    """按块读取文件，供 StreamingResponse 使用。"""
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            yield chunk


def save_manifest(manifest: dict[str, Any]) -> None:
    """写 manifest JSON + 人可读的行数清单 CSV。"""
    manifest["updated_at"] = now_text()
    partitions = manifest.get("partitions", [])
    manifest["published_row_count"] = sum(
        int(part.get("row_count", 0))
        for part in partitions
        if part.get("status") == PARTITION_CONFIRMED
    )
    manifest["published_partitions"] = [
        part.get("name")
        for part in partitions
        if part.get("status") == PARTITION_CONFIRMED
    ]
    manifest["status"] = (
        "completed"
        if partitions and all(part.get("status") == PARTITION_CONFIRMED for part in partitions)
        else "partial"
    )
    atomic_write_text(
        manifest_path(int(manifest["export_id"])),
        json.dumps(manifest, ensure_ascii=False, indent=2),
    )
    save_row_count_list(manifest)


def save_row_count_list(manifest: dict[str, Any]) -> None:
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(
        [
            "申请编号",
            "导出任务",
            "分区",
            "状态",
            "行数",
            "已发布",
            "文件",
            "校验和",
            "字节数",
            "完整记录数",
            "回执编号",
            "确认时间",
            "说明",
        ]
    )
    for part in manifest.get("partitions", []):
        writer.writerow(
            [
                manifest.get("request_id", ""),
                manifest.get("export_id", ""),
                part.get("name", ""),
                part.get("status", ""),
                part.get("row_count", 0),
                1 if part.get("status") == PARTITION_CONFIRMED else 0,
                part.get("file", ""),
                part.get("checksum", ""),
                part.get("bytes", 0),
                part.get("record_count", part.get("row_count", 0)),
                part.get("receipt_id", ""),
                part.get("confirmed_at", ""),
                part.get("note", ""),
            ]
        )
    atomic_write_text(rowcount_path(int(manifest["export_id"])), output.getvalue())


def read_row_count_list(export_id: int) -> list[dict[str, str]]:
    path = rowcount_path(export_id)
    if not path.is_file():
        return []
    with open(path, "r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def save_receipt(receipt: dict[str, Any]) -> None:
    atomic_write_text(
        receipt_path(str(receipt["receipt_id"])),
        json.dumps(receipt, ensure_ascii=False, indent=2),
    )


def load_receipt(receipt_id: str) -> dict[str, Any] | None:
    path = receipt_path(receipt_id)
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def save_claim_receipt(receipt: dict[str, Any]) -> None:
    atomic_write_text(
        claim_path(str(receipt["receipt_id"])),
        json.dumps(receipt, ensure_ascii=False, indent=2),
    )


def load_claim_receipt(receipt_id: str) -> dict[str, Any] | None:
    path = claim_path(receipt_id)
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def load_claim_receipts(request_id: str | None = None) -> list[dict[str, Any]]:
    receipts: list[dict[str, Any]] = []
    for path in sorted(claim_dir().glob("claim_*.json")):
        try:
            receipt = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if request_id is None or receipt.get("request_id") == request_id:
            receipts.append(receipt)
    receipts.sort(key=lambda item: (str(item.get("claimed_at", "")), str(item.get("receipt_id"))))
    return receipts


def make_partition_receipt(
    manifest: dict[str, Any],
    partition: str,
    *,
    row_count: int,
    checksum: str,
    path: Path,
    fields: list[str],
) -> dict[str, Any]:
    digest = hashlib.md5(
        f"{manifest['export_id']}::{partition}::{checksum}".encode("utf-8")
    ).hexdigest()[:10]
    return {
        "receipt_id": f"RCPT-{manifest['export_id']}-{safe_name(partition)}-{digest}",
        "type": "partition_confirmation",
        "export_id": manifest["export_id"],
        "request_id": manifest.get("request_id"),
        "partition": partition,
        "status": PARTITION_CONFIRMED,
        "row_count": row_count,
        "file": str(path),
        "checksum": checksum,
        "fields": fields,
        "row_count_semantics": ROW_COUNT_SEMANTICS,
        "confirmed_at": now_text(),
        "complete": True,
    }


def coverage_text(parts: list[dict[str, Any]], excluded: list[dict[str, Any]]) -> str:
    covered = "、".join(
        f"{part.get('name')}（{part.get('row_count', 0)} 行）" for part in parts
    ) or "无"
    if not excluded:
        return f"本次可领取分区：{covered}；全部已闭合。"
    notes = "、".join(
        f"{part.get('name')}（{part.get('status')}，不计入）" for part in excluded
    )
    return f"本次可领取分区：{covered}；未闭合：{notes}。"
