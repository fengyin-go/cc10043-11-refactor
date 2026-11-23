"""数据落地配置领域服务：配置处理后的数据写入目标存储的路径、分区和保留策略。

数据链路位置：数据落位层，将处理完成的结构化数据按配置写入目标存储，是数据链路最后一个写入环节。

原始载荷明细的行号分配、提交编号核对与准备态统一交给消息载荷存储的共用流程，本服务
只保留落地入口自己的批次命名（``sink-{entry_id}-{partition}``）、消息 ID
（``sink-msg-{行号}``）与原样文本（``str(data)``）。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.payload_store import PayloadStoreService
from app.store import store


class DataSinkService(BaseModuleService):
    MODULE = "data_sink"
    ENTITY = "落地配置"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["配置名称", "目标存储类型", "写入模式", "分区策略", "保留天数"]
    REQUIRED_FIELDS = ["配置名称", "目标存储类型", "写入模式"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用配置": "已启用", "停用配置": "已停用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    ENTRY = "data_sink"

    def _payload_service(self) -> PayloadStoreService:
        from app.services import service_for
        return service_for("payload_store")  # type: ignore[return-value]

    def _batch_name(self, entry_id: int, partition: str) -> str:
        # 沿用既有落地批次命名行为。
        return f"sink-{entry_id}-{partition}"

    # ---- 业务扩展：数据落位与分区管理 ----

    def prepare_sink(
        self, entry_id: int, data: dict[str, Any], commit_no: str,
        partition_key: str | None = None,
    ) -> dict[str, Any]:
        """落地请求在载荷转换前挂起：先占未就绪行，回放读不到，等待放开。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已启用":
            return {"ok": False, "message": "落地配置不可用"}
        partition = partition_key or entry.get("分区策略", "default")
        batch_name = self._batch_name(entry_id, partition)
        raw_text = str(data)
        result = self._payload_service().stage_pending(
            entry=self.ENTRY, commit_no=commit_no, batch_name=batch_name,
            message_id=lambda pid: f"sink-msg-{pid}", raw_text=raw_text,
            source=f"{self.ENTRY}:{entry_id}", partition=partition,
        )
        result["message"] = "落地请求已挂起，等待载荷转换"
        result["partition"] = partition
        return result

    def commit_sink(self, entry_id: int, commit_no: str) -> dict[str, Any]:
        """放开挂起的落地请求，按提交编号核对后定稿或拒绝。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已启用":
            return {"ok": False, "message": "落地配置不可用"}
        outcome = self._payload_service().resume_pending(
            entry=self.ENTRY, commit_no=commit_no)
        if not outcome.get("ok"):
            return {"ok": False, "message": outcome.get("reason", "落地提交未采纳"),
                    **{k: v for k, v in outcome.items() if k != "reason"}}
        receipt = outcome["receipt"]
        return {
            "ok": True, "message": f"数据已落位至{entry.get('目标存储类型', '')}",
            "partition": outcome.get("partition"),
            "write_mode": entry.get("写入模式", ""),
            "payload_id": receipt["payload_id"],
            "commit_no": commit_no,
            "receipt": receipt,
        }

    def sink_data(self, entry_id: int, data: dict[str, Any], partition_key: str | None = None,
                  commit_no: str | None = None) -> dict[str, Any]:
        """将数据按配置写入目标存储。

        原始载荷明细经由载荷存储的共用流程写入，行号不再由本服务自行计算；批次名、
        消息 ID、原文与返回字段保持既有行为。带提交编号时走原子核对，防止同编号的
        金额更正覆盖已经交付的原文。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已启用":
            return {"ok": False, "message": "落地配置不可用"}
        storage_type = entry.get("目标存储类型", "")
        write_mode = entry.get("写入模式", "")
        partition = partition_key or entry.get("分区策略", "default")
        batch_name = self._batch_name(entry_id, partition)
        raw_text = str(data)
        payload_service = self._payload_service()
        if commit_no:
            outcome = payload_service.submit_payload(
                entry=self.ENTRY, commit_no=commit_no, batch_name=batch_name,
                message_id=lambda pid: f"sink-msg-{pid}", raw_text=raw_text,
                source=f"{self.ENTRY}:{entry_id}",
            )
            if not outcome.get("ok"):
                return {"ok": False, "message": outcome["reason"],
                        "conflict": True, "receipt": outcome.get("receipt")}
            if outcome.get("duplicate"):
                receipt = outcome["receipt"]
                return {"ok": True, "message": outcome["reason"], "duplicate": True,
                        "partition": partition, "write_mode": write_mode,
                        "payload_id": receipt["payload_id"], "commit_no": commit_no}
            receipt = outcome["receipt"]
            return {"ok": True, "message": f"数据已落位至{storage_type}",
                    "partition": partition, "write_mode": write_mode,
                    "payload_id": receipt["payload_id"], "commit_no": commit_no}
        written = payload_service.write_raw_payload(
            batch_name=batch_name, message_id=lambda pid: f"sink-msg-{pid}",
            raw_text=raw_text, ready=True, source=f"{self.ENTRY}:{entry_id}",
            extra={"分区": partition},
        )
        return {"ok": True, "message": f"数据已落位至{storage_type}", "partition": partition,
                "write_mode": write_mode, "payload_id": written["payload_id"]}

    def validate_partition_strategy(self, entry_id: int) -> list[str]:
        """校验分区策略与数据源的兼容性。"""
        errors = []
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return ["落地配置不存在"]
        strategy = entry.get("分区策略", "")
        valid_strategies = ["按日期", "按小时", "按数据源", "按业务键", "无分区"]
        if strategy not in valid_strategies:
            errors.append(f"不支持的分区策略: {strategy}，可选: {', '.join(valid_strategies)}")
        try:
            retention = int(str(entry.get("保留天数", "0")))
        except ValueError:
            retention = 0
        if retention <= 0:
            errors.append("保留天数必须大于0")
        return errors

    def route_source_data(self, source_name: str) -> list[str]:
        """根据数据源名称查找匹配的落地配置。"""
        configs = store.rows(self.MODULE)
        return [c.get("配置名称", "") for c in configs if c.get("status") == "已启用" and source_name in str(c.get("配置名称", ""))]

    def cleanup_expired(self) -> dict[str, Any]:
        """清理过期的数据落地产物。"""
        rows = store.rows(self.MODULE)
        now = datetime.now(timezone.utc)
        cleaned = 0
        for row in rows:
            if row.get("status") != "已启用":
                continue
            try:
                retention = int(str(row.get("保留天数", "0")))
            except ValueError:
                continue
            payloads = store.rows("payload_store")
            config_name = row.get("配置名称", "")
            for p in payloads[:]:
                if config_name in str(p.get("采集批次", "")):
                    time_str = str(p.get("存储时间", ""))
                    try:
                        store_time = datetime.fromisoformat(time_str)
                        if (now - store_time).days > retention:
                            p["status"] = "已过期"
                            p["pending"] = False
                            cleaned += 1
                    except ValueError:
                        continue
        return {"ok": True, "expired": cleaned}

    def check_downstream_consumers(self, entry_id: int) -> list[dict[str, Any]]:
        """检查数据落位结果的下游消费者（审计、导出、回放）。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        config_name = entry.get("配置名称", "")
        consumers = []
        audit = store.rows("stream_audit")
        for a in audit:
            if config_name in str(a.get("数据源", "")):
                consumers.append({"type": "审计", "id": a.get("id"), "detail": a.get("处理时间")})
                break
        export = store.rows("stream_export")
        for e in export:
            if config_name in str(e.get("数据源", "")):
                consumers.append({"type": "导出", "id": e.get("id"), "detail": e.get("任务名称")})
                break
        replay = store.rows("batch_replay")
        for r in replay:
            if config_name in str(r.get("目标Topic", "")):
                consumers.append({"type": "回放", "id": r.get("id"), "detail": r.get("回放范围")})
                break
        return consumers
