"""消息载荷存储领域服务：存储消息的原始载荷副本，支持事后审计和回放，管理压缩和过期策略。

数据链路位置：数据归档层，保存原始消息载荷作为全链数据审计和回放的事实来源。

数据落地（data_sink）与 API 连接器（api_connector）两个入口都需要向同一张载荷表
写入原始载荷明细。早期两处各自用 ``max(id)+1`` 计算行号，并发入库时两个回执可能指向
同一个号。这里把「写入原始载荷明细」收拢成唯一的共用流程：

- 行号（id）在同一把锁内分配并落行，跨入口并发也不会重号，回执号与实际行严格一致；
- 批次名、消息 ID、原文均由调用方按既有规则传入，本服务不改变入口各自的命名行为；
- 同一入口同一提交编号（commit_no）的重复/更正请求在此统一核对，先成功交付的原文
  不可被后续不同金额改动，未采纳版本的原因与已入库金额均可通过回执查询；
- 行具备准备态：未准备完整的行（挂起中的落地请求、被拒版本）不会进入回放结果。
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Callable

from app.services.base import BaseModuleService
from app.store import store


def _extract_amount(raw_text: str) -> str | None:
    """从原样文本里提取金额，用于提交编号核对；提取不到返回 None。"""
    for marker in ("'金额': '", "'金额': ", "'amount': '", "'amount': "):
        if marker in raw_text:
            value = raw_text.split(marker, 1)[1]
            value = value.split("'", 1)[0] if marker.endswith("'") else value.split(",", 1)[0]
            value = value.split("}", 1)[0].strip().strip("'")
            if value:
                return value
    return None


class PayloadStoreService(BaseModuleService):
    MODULE = "payload_store"
    ENTITY = "原始载荷"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["采集批次", "消息ID", "原始载荷", "存储时间", "压缩后大小"]
    REQUIRED_FIELDS = ["采集批次", "消息ID", "原始载荷"]
    STATUS_ORDER = ["已存储", "已压缩", "已过期"]
    ACTION_RULES = {"批量压缩": "已存储", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        # 所有载荷行的分配与提交编号核对都在这把锁内完成，保证跨入口并发的原子性。
        self._lock = threading.RLock()
        # 已成功交付的提交编号：(入口, 提交编号) -> 回执事实
        self._accepted: dict[tuple[str, str], dict[str, Any]] = {}
        # 同一提交编号下的每次尝试（含未采纳），供核对入库金额与未入库原因。
        self._attempts: dict[tuple[str, str], list[dict[str, Any]]] = {}
        # 挂起在载荷转换阶段的落地请求，待放开后完成或被冲突拒绝。
        self._staging: dict[str, dict[str, Any]] = {}

    # ---- 共用流程：原始载荷明细写入 ----

    @staticmethod
    def _resolve_message_id(message_id: str | Callable[[int], str], payload_id: int) -> str:
        return message_id(payload_id) if callable(message_id) else message_id

    def write_raw_payload(
        self,
        *,
        batch_name: str,
        message_id: str | Callable[[int], str],
        raw_text: str,
        ready: bool,
        source: str,
        commit_no: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """两个入口写入原始载荷明细的唯一共用流程。

        行号在锁内分配并立即落行，避免并发时两个回执指向同一个号。批次命名、消息 ID
        与原文完全沿用调用方传入的既有内容；``ready=False`` 表示行尚未准备完整，
        不会出现在回放取数中。
        """
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            rows = store.rows(self.MODULE)
            payload_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
            row = {
                "id": payload_id,
                "采集批次": batch_name,
                "消息ID": self._resolve_message_id(message_id, payload_id),
                "原始载荷": raw_text,
                "存储时间": now,
                "压缩后大小": str(len(raw_text)),
                "status": "已存储",
                "pending": True,
                "abnormal": False,
                "准备状态": "完整" if ready else "未就绪",
                "入口": source,
                "提交编号": commit_no or "",
            }
            if extra:
                row.update(extra)
            rows.append(row)
            return {
                "payload_id": payload_id,
                "采集批次": batch_name,
                "消息ID": row["消息ID"],
                "原始载荷": raw_text,
                "存储时间": now,
                "准备状态": row["准备状态"],
            }

    def register_attempt(self, entry: str, commit_no: str, attempt: dict[str, Any]) -> None:
        """登记一次提交尝试（无论最终是否采纳），保证未入库原因可查。"""
        with self._lock:
            self._attempts.setdefault((entry, commit_no), []).append(
                {"记录时间": datetime.now(timezone.utc).isoformat(), **attempt})

    def lookup_accepted(self, entry: str, commit_no: str) -> dict[str, Any] | None:
        with self._lock:
            accepted = self._accepted.get((entry, commit_no))
            return dict(accepted) if accepted is not None else None

    def submit_payload(
        self,
        *,
        entry: str,
        commit_no: str | None,
        batch_name: str,
        message_id: str | Callable[[int], str],
        raw_text: str,
        source: str,
    ) -> dict[str, Any]:
        """带提交编号核对的原子入库：核对与行写入在同一把锁内完成。

        - 首次提交：写入完整行并发出成功回执，原文就此定稿；
        - 同编号同原文重发：幂等返回已有回执，原批次不新增行；
        - 同编号不同内容（如金额更正）：拒绝写入，保留已交付原文，原因可查。
        """
        with self._lock:
            amount = _extract_amount(raw_text)
            if commit_no:
                accepted = self._accepted.get((entry, commit_no))
                if accepted is not None:
                    if accepted["原始载荷"] == raw_text:
                        reason = "重复提交：原文与已交付回执一致，幂等处理，未新增载荷行"
                        self.register_attempt(entry, commit_no, {
                            "提交编号": commit_no, "金额": amount, "入口": source,
                            "批次": batch_name, "结果": "重复忽略", "原因": reason,
                        })
                        return {"ok": True, "duplicate": True, "reason": reason,
                                "receipt": dict(accepted)}
                    reason = (
                        f"提交编号 {commit_no} 已成功交付，已交付原文不得修改："
                        f"已入库金额={accepted.get('金额')}，本次金额={amount}，本次提交未入库"
                    )
                    self.register_attempt(entry, commit_no, {
                        "提交编号": commit_no, "金额": amount, "入口": source,
                        "批次": batch_name, "结果": "冲突拒绝", "原因": reason,
                    })
                    return {"ok": False, "duplicate": False, "conflict": True,
                            "reason": reason, "receipt": dict(accepted)}
            written = self.write_raw_payload(
                batch_name=batch_name, message_id=message_id, raw_text=raw_text,
                ready=True, source=source, commit_no=commit_no,
            )
            receipt = {
                "payload_id": written["payload_id"],
                "采集批次": written["采集批次"],
                "消息ID": written["消息ID"],
                "原始载荷": raw_text,
                "金额": amount,
                "入口": source,
                "提交编号": commit_no,
                "存储时间": written["存储时间"],
            }
            if commit_no:
                self._accepted[(entry, commit_no)] = receipt
                self.register_attempt(entry, commit_no, {
                    "提交编号": commit_no, "金额": amount, "入口": source,
                    "批次": batch_name, "结果": "已入库",
                    "payload_id": written["payload_id"], "原因": "",
                })
            return {"ok": True, "duplicate": False, "receipt": receipt}

    # ---- 挂起/放开：落地请求卡在载荷转换阶段 ----

    def stage_pending(
        self,
        *,
        entry: str,
        commit_no: str,
        batch_name: str,
        message_id: str | Callable[[int], str],
        raw_text: str,
        source: str,
        partition: str,
    ) -> dict[str, Any]:
        """落地入口在载荷转换前挂起请求：先占一个未就绪行，回放取不到该行。"""
        with self._lock:
            written = self.write_raw_payload(
                batch_name=batch_name, message_id=message_id, raw_text=raw_text,
                ready=False, source=source, commit_no=commit_no,
                extra={"分区": partition},
            )
            self._staging[commit_no] = {
                "entry": entry, "commit_no": commit_no, "payload_id": written["payload_id"],
                "batch_name": batch_name, "raw_text": raw_text, "source": source,
                "partition": partition, "金额": _extract_amount(raw_text),
                "挂起时间": datetime.now(timezone.utc).isoformat(),
            }
            return {"ok": True, "staged": True, "commit_no": commit_no,
                    "payload_id": written["payload_id"], "采集批次": batch_name}

    def resume_pending(self, *, entry: str, commit_no: str) -> dict[str, Any]:
        """放开挂起的落地请求：无冲突则定稿入库；已被同编号更正抢先则拒绝并记录原因。"""
        with self._lock:
            staged = self._staging.get(commit_no)
            if staged is None or staged["entry"] != entry:
                return {"ok": False, "reason": f"提交编号 {commit_no} 没有挂起中的落地请求"}
            raw_text = staged["raw_text"]
            amount = staged["金额"]
            accepted = self._accepted.get((entry, commit_no))
            row = store.find(self.MODULE, staged["payload_id"])
            if accepted is not None:
                reason = (
                    f"提交编号 {commit_no} 的金额更正已先完成转换并入库（已入库金额="
                    f"{accepted.get('金额')}），挂起版本金额={amount} 与已交付原文冲突，未入库"
                )
                if row is not None:
                    row["准备状态"] = "未采纳"
                    row["未采纳原因"] = reason
                self.register_attempt(entry, commit_no, {
                    "提交编号": commit_no, "金额": amount, "入口": staged["source"],
                    "批次": staged["batch_name"], "结果": "冲突拒绝", "原因": reason,
                })
                self._staging.pop(commit_no, None)
                return {"ok": False, "conflict": True, "reason": reason,
                        "receipt": dict(accepted), "payload_id": staged["payload_id"],
                        "partition": staged["partition"], "batch_name": staged["batch_name"]}
            if row is not None:
                row["准备状态"] = "完整"
                row.pop("未采纳原因", None)
            receipt = {
                "payload_id": staged["payload_id"],
                "采集批次": staged["batch_name"],
                "消息ID": row["消息ID"] if row is not None else "",
                "原始载荷": raw_text,
                "金额": amount,
                "入口": staged["source"],
                "提交编号": commit_no,
                "存储时间": row["存储时间"] if row is not None else "",
            }
            self._accepted[(entry, commit_no)] = receipt
            self.register_attempt(entry, commit_no, {
                "提交编号": commit_no, "金额": amount, "入口": staged["source"],
                "批次": staged["batch_name"], "结果": "已入库",
                "payload_id": staged["payload_id"], "原因": "挂起放开后完成转换",
            })
            self._staging.pop(commit_no, None)
            return {"ok": True, "duplicate": False, "receipt": receipt,
                    "partition": staged["partition"], "batch_name": staged["batch_name"]}

    def reconcile_commit(self, entry: str, commit_no: str) -> dict[str, Any]:
        """按提交编号核对同一入口的请求：实际入库金额与每份未入库尝试的原因都可查。"""
        with self._lock:
            accepted = self._accepted.get((entry, commit_no))
            attempts = [dict(a) for a in self._attempts.get((entry, commit_no), [])]
            return {
                "entry": entry,
                "commit_no": commit_no,
                "accepted": dict(accepted) if accepted is not None else None,
                "stored_amount": accepted.get("金额") if accepted is not None else None,
                "attempts": attempts,
                "rejected": [a for a in attempts if a.get("结果") in ("冲突拒绝",)],
            }

    # ---- 业务扩展：载荷管理与归档 ----

    def batch_compress_payloads(self, batch_name: str | None = None) -> dict[str, Any]:
        """批量压缩指定批次的载荷。"""
        rows = store.rows(self.MODULE)
        if batch_name:
            rows = [r for r in rows if batch_name in str(r.get("采集批次", ""))]
        compressed = 0
        for row in rows:
            if row.get("status") != "已存储":
                continue
            payload = str(row.get("原始载荷", ""))
            if len(payload) > 100:
                row["原始载荷"] = payload[:100] + "...[compressed]"
                row["压缩后大小"] = str(len(row["原始载荷"]))
            row["status"] = "已压缩"
            compressed += 1
        return {"ok": True, "compressed": compressed}

    def get_replay_source(self, topic: str, limit: int = 100) -> list[dict[str, Any]]:
        """从载荷存储提取回放数据源。

        回放只读取已准备完整的行：挂起中（未就绪）和未采纳的行不会出现，保证批量消息
        回放读到的数据与有效入库回执一致。
        """
        rows = store.rows(self.MODULE)
        matched = [
            r for r in rows
            if topic in str(r.get("采集批次", ""))
            and r.get("status") in ("已存储", "已压缩")
            and r.get("准备状态", "完整") == "完整"
        ]
        return matched[:limit]

    def validate_batch_integrity(self, batch_name: str) -> dict[str, Any]:
        """校验采集批次的存储完整性。"""
        rows = store.rows(self.MODULE)
        batch_rows = [r for r in rows if batch_name in str(r.get("采集批次", ""))]
        total = len(batch_rows)
        stored = len([r for r in batch_rows if r.get("status") == "已存储"])
        compressed = len([r for r in batch_rows if r.get("status") == "已压缩"])
        expired = len([r for r in batch_rows if r.get("status") == "已过期"])
        ready = len([r for r in batch_rows if r.get("准备状态", "完整") == "完整"])
        return {"batch": batch_name, "total": total, "ready": ready,
                "stored": stored, "compressed": compressed, "expired": expired,
                "intact": total > 0 and expired == 0}

    def route_to_watermark(self, batch_name: str) -> str | None:
        """载荷批次路由到水位追踪。"""
        batch_rows = [r for r in store.rows(self.MODULE) if batch_name in str(r.get("采集批次", ""))]
        if not batch_rows:
            return None
        now = datetime.now(timezone.utc).isoformat()
        watermark_rows = store.rows("watermark_tracker")
        wid = max((int(r.get("id", 0)) for r in watermark_rows), default=0) + 1
        watermark_rows.append({
            "id": wid, "数据源": batch_name,
            "水位字段": "载荷计数", "当前水位": str(len(batch_rows)),
            "上次推进时间": now, "延迟分钟数": "0",
            "status": "正常水位", "pending": True, "abnormal": False,
        })
        return now

    def export_snapshot(self, batch_name: str) -> dict[str, Any]:
        """导出载荷快照用于离线分析。"""
        payloads = self.get_replay_source(batch_name, limit=500)
        export_rows = store.rows("stream_export")
        eid = max((int(r.get("id", 0)) for r in export_rows), default=0) + 1
        now = datetime.now(timezone.utc).isoformat()
        export_rows.append({
            "id": eid, "任务名称": f"载荷快照-{batch_name}",
            "数据源": batch_name, "导出格式": "json",
            "时间范围": now, "文件大小": str(len(str(payloads))),
            "status": "排队中", "pending": True, "abnormal": False,
        })
        return {"ok": True, "export_id": eid, "payload_count": len(payloads)}
