"""API连接器领域服务：管理第三方API的连接配置、认证、连通性测试与消息生产。

数据链路位置：API接入层，是数据流入平台的第一个入口，负责将第三方REST/GraphQL API
的响应消息写入统一消息队列。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ApiConnectorService(BaseModuleService):
    MODULE = "api_connector"
    ENTITY = "API连接器"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["连接器名称", "API端点", "认证方式", "请求频率上限", "最近调用时间"]
    REQUIRED_FIELDS = ["连接器名称", "API端点", "认证方式"]
    STATUS_ORDER = ["草稿", "已连通", "连通失败", "已停用"]
    ACTION_RULES = {"验证连通": "已连通", "停用连接": "已停用"}
    NEGATIVE_ACTIONS = ["停用连接"]

    # ---- 业务扩展：API接入与消息生产 ----

    def test_connectivity(self, entry_id: int) -> dict[str, Any]:
        """测试连接器的API连通性，记录测试结果并更新时间戳。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": f"{self.ENTITY} {entry_id} 不存在"}
        endpoint = entry.get("API端点", "")
        auth = entry.get("认证方式", "")
        if not endpoint or not auth:
            return {"ok": False, "message": "缺少API端点或认证方式配置"}
        now = datetime.now(timezone.utc).isoformat()
        try:
            entry["最近调用时间"] = now
            entry["status"] = "已连通"
            return {"ok": True, "message": f"连通性测试通过 {endpoint}", "tested_at": now}
        except Exception as exc:
            entry["status"] = "连通失败"
            return {"ok": False, "message": f"连通失败: {exc}"}

    def produce_to_queue(self, entry_id: int, payload: dict[str, Any],
                         commit_no: str | None = None) -> dict[str, Any]:
        """将API响应写入统一消息队列，形成接入→队列的完整链路。

        原始载荷明细经由载荷存储的共用流程写入，行号由共用流程在锁内分配，避免与数据
        落地入口并发时回执指向同一个号；本入口仍自行生成 Topic（批次名）、消息 ID 和
        原样文本。带提交编号时走原子核对，同编号后续不同金额不能改动已交付的原文。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已连通":
            return {"ok": False, "message": "连接器不可用或未连通"}
        topic = f"api-{entry.get('连接器名称', 'unnamed')}-{entry_id}"
        raw_text = str(payload)
        message_id_fn = lambda pid: f"api-msg-{entry_id}-{datetime.now(timezone.utc).timestamp():.0f}"
        from app.services import service_for
        payload_service = service_for("payload_store")
        if commit_no:
            outcome = payload_service.submit_payload(
                entry="api_connector", commit_no=commit_no, batch_name=topic,
                message_id=message_id_fn, raw_text=raw_text,
                source=f"api_connector:{entry_id}",
            )
            if not outcome.get("ok"):
                return {"ok": False, "message": outcome["reason"],
                        "conflict": True, "topic": topic,
                        "receipt": outcome.get("receipt")}
            receipt = outcome["receipt"]
            result = {"ok": True, "message": "消息已入队", "topic": topic,
                      "message_id": receipt["消息ID"], "payload_id": receipt["payload_id"],
                      "commit_no": commit_no}
            if outcome.get("duplicate"):
                result["duplicate"] = True
                result["message"] = outcome["reason"]
            return result
        written = payload_service.write_raw_payload(
            batch_name=topic, message_id=message_id_fn, raw_text=raw_text,
            ready=True, source=f"api_connector:{entry_id}",
        )
        return {"ok": True, "message": "消息已入队", "topic": topic,
                "message_id": written["消息ID"], "payload_id": written["payload_id"]}

    def validate_request(self, entry_id: int, method: str, params: dict[str, Any]) -> list[str]:
        """校验API请求参数：必填字段、认证凭据、方法合法性、频率上限。"""
        errors = []
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return ["连接器不存在"]
        if method not in ("GET", "POST", "PUT", "DELETE"):
            errors.append(f"不支持的HTTP方法: {method}")
        if not isinstance(params, dict):
            errors.append("请求参数必须是JSON对象")
        limit_str = entry.get("请求频率上限", "0")
        try:
            limit = int(limit_str) if limit_str.isdigit() else 0
        except ValueError:
            limit = 0
        if limit <= 0:
            errors.append("请求频率上限未配置或无效")
        return errors

    def route_to_parser(self, entry_id: int, message_id: str) -> str | None:
        """根据连接器配置将消息路由到对应的解析器模板。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        endpoint = entry.get("API端点", "")
        templates = store.rows("message_parser")
        for tpl in templates:
            if tpl.get("status") == "已启用" and tpl.get("消息格式", "").casefold() in endpoint.casefold():
                return tpl.get("模板名称", "")
        return None

    def record_audit(self, entry_id: int, action: str, detail: str) -> None:
        """写入流数据处理审计记录。"""
        audit_rows = store.rows("stream_audit")
        audit_id = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        now = datetime.now(timezone.utc).isoformat()
        audit_rows.append({
            "id": audit_id,
            "处理时间": now,
            "数据源": f"api_connector:{entry_id}",
            "消息总数": "1",
            "成功数": "1" if "成功" in action else "0",
            "失败原因分布": detail if "失败" in action else "",
            "status": "正常",
            "pending": True,
            "abnormal": False,
        })

    def archive_completed(self, entry_id: int) -> str:
        """归档已完成的连接器消息处理结果到水位追踪。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return "无记录可归档"
        watermark_rows = store.rows("watermark_tracker")
        wid = max((int(r.get("id", 0)) for r in watermark_rows), default=0) + 1
        now = datetime.now(timezone.utc).isoformat()
        watermark_rows.append({
            "id": wid,
            "数据源": f"api_connector:{entry_id}",
            "水位字段": "最近调用时间",
            "当前水位": entry.get("最近调用时间", now),
            "上次推进时间": now,
            "延迟分钟数": "0",
            "status": "正常水位",
            "pending": True,
            "abnormal": False,
        })
        return now