"""载荷共用写入流程与提交编号核对的端到端验证脚本。

运行方式（在仓库根目录）：
    python3 -I verification/verify_payload_flow.py

覆盖：
1. 重构前后两入口的分区/Topic（批次名）、消息 ID 形态与原文行为比对；
2. 两入口并发写入原始载荷明细，行号不重号，回执号与实际行一致；
3. 落地 100 元订单挂起在载荷转换阶段，同提交编号的 120 元更正先完成转换，
   放开后 100 元被拒、金额与未入库原因可按提交编号核对；
4. API 连接器独立订单成功入库后，批量消息回放只读到准备完整、与回执一致的行；
5. 重发未被采纳的 100 元版本，原批次不多出行；成功回执后不同金额不得改动原文。
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from app.services import service_for  # noqa: E402
from app.store import store  # noqa: E402

FAILURES: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {label}" + (f" -> {detail}" if detail else ""))
    if not condition:
        FAILURES.append(f"{label} {detail}")


def reset_tables(*, keep_configs: bool = False) -> None:
    modules = ["payload_store", "batch_replay"]
    if not keep_configs:
        modules += ["data_sink", "api_connector"]
    for module in modules:
        store.rows(module).clear()
    payload = service_for("payload_store")
    payload._accepted.clear()
    payload._attempts.clear()
    payload._staging.clear()


def seed_configs() -> tuple[int, int]:
    sink_rows = store.rows("data_sink")
    sink_id = max((int(r["id"]) for r in sink_rows), default=0) + 1
    sink_rows.append({
        "id": sink_id, "status": "已启用", "pending": True, "abnormal": False,
        "配置名称": "订单落地", "目标存储类型": "数仓", "写入模式": "追加",
        "分区策略": "order-zone", "保留天数": "30",
    })
    conn_rows = store.rows("api_connector")
    conn_id = max((int(r["id"]) for r in conn_rows), default=0) + 1
    conn_rows.append({
        "id": conn_id, "status": "已连通", "pending": True, "abnormal": False,
        "连接器名称": "订单API", "API端点": "https://example.test/orders",
        "认证方式": "Bearer", "请求频率上限": "100", "最近调用时间": "",
    })
    return sink_id, conn_id


def payload_ids() -> list[int]:
    return sorted(int(r["id"]) for r in store.rows("payload_store"))


# ---- 步骤 1：重构前后命名/分区/原文行为比对 ----

def step1_behavior_parity(sink_id: int, conn_id: int) -> None:
    print("\n=== 步骤1：重构前后 分区/Topic(批次名)/消息ID/原文 行为比对 ===")
    sink = service_for("data_sink")
    connector = service_for("api_connector")
    payload = service_for("payload_store")

    data = {"order": "A1", "金额": "88"}
    # 重构前 data_sink 的既有规则
    old_batch = f"sink-{sink_id}-order-zone"
    old_raw = str(data)
    r1 = sink.sink_data(sink_id, data, partition_key="order-zone")
    row1 = store.find("payload_store", r1["payload_id"])
    check("落地批次名与重构前一致", row1["采集批次"] == old_batch,
          f"{row1['采集批次']} == {old_batch}")
    check("落地消息ID沿用 sink-msg-{行号}", row1["消息ID"] == f"sink-msg-{r1['payload_id']}",
          row1["消息ID"])
    check("落地原文沿用 str(data)", row1["原始载荷"] == old_raw)
    check("落地返回字段保持既有",
          set(["ok", "message", "partition", "write_mode", "payload_id"]).issubset(r1),
          str(sorted(r1)))
    check("落地分区字段保持既有", r1["partition"] == "order-zone")

    body = {"order": "B2", "amount": "66"}
    r2 = connector.produce_to_queue(conn_id, body)
    row2 = store.find("payload_store", r2["payload_id"])
    old_topic = f"api-订单API-{conn_id}"
    check("API Topic(批次名)与重构前一致", row2["采集批次"] == old_topic,
          f"{row2['采集批次']} == {old_topic}")
    check("API消息ID沿用 api-msg 前缀", row2["消息ID"].startswith(f"api-msg-{conn_id}-"),
          row2["消息ID"])
    check("API原文沿用 str(payload)", row2["原始载荷"] == str(body))
    check("API返回字段保持既有",
          set(["ok", "message", "topic", "message_id"]).issubset(r2), str(sorted(r2)))
    check("新写入行默认准备完整可回放", row2.get("准备状态") == "完整")

    ids = payload_ids()
    check("两入口行号互不重复", len(ids) == len(set(ids)), str(ids))
    check("回执 payload_id 与实际行一致",
          r1["payload_id"] == row1["id"] and r2["payload_id"] == row2["id"])


# ---- 步骤 2：并发写入不重号 ----

def step2_concurrent_ids(sink_id: int, conn_id: int) -> None:
    print("\n=== 步骤2：落地与API并发入库，行号不重号 ===")
    sink = service_for("data_sink")
    connector = service_for("api_connector")
    receipts: list[dict] = []
    errors: list[Exception] = []

    def worker(is_sink: bool) -> None:
        try:
            for i in range(20):
                if is_sink:
                    r = sink.sink_data(sink_id, {"seq": i, "金额": str(10 + i)})
                else:
                    r = connector.produce_to_queue(conn_id, {"seq": i, "amount": str(10 + i)})
                receipts.append(r)
        except Exception as exc:  # pragma: no cover - 仅用于暴露并发异常
            errors.append(exc)

    t1 = threading.Thread(target=worker, args=(True,))
    t2 = threading.Thread(target=worker, args=(False,))
    t1.start(); t2.start(); t1.join(); t2.join()
    check("并发过程无异常", not errors, str(errors))
    ids = [r["payload_id"] for r in receipts if r.get("ok")]
    check("40 条并发回执全部成功", len(receipts) == 40, f"receipts={len(receipts)}")
    check("并发回执行号全局唯一", len(ids) == len(set(ids)) == 40, f"unique={len(set(ids))}")
    actual = payload_ids()
    check("回执号都能在读回的载荷明细中找到", set(ids).issubset(set(actual)))
    check("载荷表无重复行号", len(actual) == len(set(actual)))


# ---- 步骤 3-5：提交编号冲突、回放一致性、重发幂等 ----

def step3_conflict_and_replay(sink_id: int, conn_id: int) -> None:
    print("\n=== 步骤3：100元挂起 → 120元更正先入库 → 放开后冲突拒绝 ===")
    sink = service_for("data_sink")
    connector = service_for("api_connector")
    replay_svc = service_for("batch_replay")
    payload = service_for("payload_store")
    commit = "ORD-1001"
    sink_batch = f"sink-{sink_id}-order-zone"
    api_topic = f"api-订单API-{conn_id}"

    # 3.1 暂停落地的 100 元订单（卡在载荷转换）
    paused = sink.prepare_sink(sink_id, {"order": commit, "金额": "100"}, commit)
    check("100元订单已挂起", paused["ok"] and paused["staged"], str(paused))
    paused_row = store.find("payload_store", paused["payload_id"])
    check("挂起行处于未就绪态", paused_row["准备状态"] == "未就绪")
    check("挂起批次仍是落地批次", paused_row["采集批次"] == sink_batch)

    # 3.2 挂起期间回放读不到未准备完整的行
    replay_rows_during = payload.get_replay_source(sink_batch)
    check("挂起中未就绪行不进入回放",
          all(r["id"] != paused["payload_id"] for r in replay_rows_during),
          f"回放行数={len(replay_rows_during)}")

    # 3.3 同提交编号的 120 元更正先完成转换并入库
    corrected = sink.sink_data(sink_id, {"order": commit, "金额": "120"},
                               partition_key="order-zone", commit_no=commit)
    check("120元更正成功入库", corrected["ok"] and not corrected.get("conflict"),
          str(corrected))
    corr_row = store.find("payload_store", corrected["payload_id"])
    check("120元更正行准备完整", corr_row["准备状态"] == "完整")
    check("120元更正原文可读回", "'金额': '120'" in corr_row["原始载荷"], corr_row["原始载荷"])

    # 3.4 API 连接器同时交付一笔独立订单（不同提交编号、不同批次）
    api_r = connector.produce_to_queue(
        conn_id, {"order": "ORD-2002", "amount": "55"}, commit_no="ORD-2002")
    check("API独立订单成功入库", api_r["ok"], str(api_r))
    api_row = store.find("payload_store", api_r["payload_id"])
    check("API独立订单金额可读回", "'amount': '55'" in api_row["原始载荷"])

    # 3.5 放开 100 元挂起请求 -> 冲突拒绝，不改动已交付原文
    resumed = sink.commit_sink(sink_id, commit)
    check("放开100元后被冲突拒绝", not resumed["ok"] and resumed.get("conflict"), str(resumed))
    check("拒绝原因可查且指明已入库120", "120" in resumed["message"] and "100" in resumed["message"],
          resumed["message"])
    paused_row_after = store.find("payload_store", paused["payload_id"])
    check("被拒100元行标记为未采纳", paused_row_after["准备状态"] == "未采纳")
    check("被拒行带有未采纳原因", bool(paused_row_after.get("未采纳原因")))
    corr_row_after = store.find("payload_store", corrected["payload_id"])
    check("已交付的120元原文未被改动", corr_row_after["原始载荷"] == corr_row["原始载荷"])

    # 3.6 提交编号核对：入库金额 + 未入库原因都可查
    recon = payload.reconcile_commit("data_sink", commit)
    check("核对得到实际入库金额120", recon["stored_amount"] == "120", str(recon["stored_amount"]))
    rejected = recon["rejected"]
    check("未采纳尝试及其原因可查",
          len(rejected) >= 1 and any("100" in a.get("原因", "") for a in rejected),
          str([(a["结果"], a.get("金额"), a.get("原因")) for a in recon["attempts"]]))
    check("已入库回执金额为120", recon["accepted"] and recon["accepted"]["金额"] == "120")

    # ---- 步骤4：回放与有效入库回执一致 ----
    print("\n=== 步骤4：批量消息回放按批次取数，与有效回执一致 ===")
    replay_rows = payload.get_replay_source(sink_batch)
    ids_in_replay = [r["id"] for r in replay_rows]
    check("回放包含120元更正行", corrected["payload_id"] in ids_in_replay)
    check("回放排除未就绪/未采纳的100元行", paused["payload_id"] not in ids_in_replay)
    check("回放仅含准备完整行", all(r.get("准备状态", "完整") == "完整" for r in replay_rows))

    # API 批次回放：只含 API 独立订单，读回内容与回执一致
    api_replay = payload.get_replay_source(api_topic)
    check("API批次回放含独立订单", api_r["payload_id"] in [r["id"] for r in api_replay])
    check("API回放原文与回执一致",
          any(r["id"] == api_r["payload_id"] and r["原始载荷"] == api_row["原始载荷"]
              for r in api_replay))

    # 经回放任务实体跑一遍 start_replay（目标 Topic 设为落地批次）
    br_rows = store.rows("batch_replay")
    br_id = max((int(r["id"]) for r in br_rows), default=0) + 1
    br_rows.append({
        "id": br_id, "status": "待回放", "pending": True, "abnormal": False,
        "回放范围": "批次回放", "开始时间": "2026-11-23", "结束时间": "2026-11-23",
        "目标Topic": sink_batch, "回放速率": "10",
    })
    before = len(store.rows("payload_store"))
    rr = replay_svc.start_replay(br_id)
    check("回放任务成功且只回放完整行",
          rr["ok"] and rr["replayed"] == len(replay_rows), str(rr))
    replay_copies = [r for r in store.rows("payload_store")
                     if r["采集批次"] == f"replay-{sink_batch}"]
    check("回放副本数等于完整行数", len(replay_copies) == len(replay_rows))
    check("回放副本均为完整态", all(r["准备状态"] == "完整" for r in replay_copies))
    check("回放副本行号唯一不重号",
          len({r["id"] for r in replay_copies}) == len(replay_copies))
    check("回放后载荷表仍无重复行号",
          (after := payload_ids()) and len(after) == len(set(after)))
    check("回放新增行数等于副本数", len(store.rows("payload_store")) - before == len(replay_copies))

    # ---- 步骤5：重发未采纳版本 & 成功回执后不同金额不可改 ----
    print("\n=== 步骤5：重发100元版本不新增行；不同金额不能改动已交付原文 ===")
    batch_rows_before = [r["id"] for r in store.rows("payload_store")
                         if r["采集批次"] == sink_batch]
    resent = sink.sink_data(sink_id, {"order": commit, "金额": "100"},
                            partition_key="order-zone", commit_no=commit)
    batch_rows_after = [r["id"] for r in store.rows("payload_store")
                        if r["采集批次"] == sink_batch]
    check("重发100元被拒绝", not resent["ok"] and resent.get("conflict"), str(resent["message"]))
    check("原批次未多出一行", batch_rows_before == batch_rows_after,
          f"before={len(batch_rows_before)} after={len(batch_rows_after)}")
    check("重发拒绝原因可查", "已成功交付" in resent["message"] or "冲突" in resent["message"])

    # 再来一版不同金额（130），同样不得改动原文
    third = sink.sink_data(sink_id, {"order": commit, "金额": "130"},
                           partition_key="order-zone", commit_no=commit)
    check("130元后续版本同样被拒", not third["ok"])
    final_row = store.find("payload_store", corrected["payload_id"])
    check("已交付原文始终为120元版本",
          final_row["原始载荷"] == str({"order": commit, "金额": "120"}),
          final_row["原始载荷"])

    # 同原文幂等重发：不新增行，返回已有回执
    idem = sink.sink_data(sink_id, {"order": commit, "金额": "120"},
                          partition_key="order-zone", commit_no=commit)
    batch_rows_idem = [r["id"] for r in store.rows("payload_store")
                       if r["采集批次"] == sink_batch]
    check("同原文重发幂等成功", idem["ok"] and idem.get("duplicate"))
    check("幂等重发指向原回执行", idem["payload_id"] == corrected["payload_id"])
    check("幂等重发不新增批次行", batch_rows_idem == batch_rows_after)

    # API 侧同样验证一次同编号防篡改
    api_again = connector.produce_to_queue(
        conn_id, {"order": "ORD-2002", "amount": "99"}, commit_no="ORD-2002")
    check("API同编号不同金额被拒", not api_again["ok"] and api_again.get("conflict"))
    api_final = store.find("payload_store", api_r["payload_id"])
    check("API已交付原文保持55元", "'amount': '55'" in api_final["原始载荷"],
          api_final["原始载荷"])
    recon_api = payload.reconcile_commit("api_connector", "ORD-2002")
    check("API提交编号核对可查入库金额与拒绝原因",
          recon_api["stored_amount"] == "55"
          and any(a["结果"] == "冲突拒绝" for a in recon_api["attempts"]))


def main() -> int:
    reset_tables()
    sink_id, conn_id = seed_configs()
    step1_behavior_parity(sink_id, conn_id)
    # 各阶段独立计数，重置载荷事实但保留配置实体。
    reset_tables(keep_configs=True)
    step2_concurrent_ids(sink_id, conn_id)
    reset_tables(keep_configs=True)
    step3_conflict_and_replay(sink_id, conn_id)
    print("\n=== 汇总 ===")
    if FAILURES:
        print(f"失败 {len(FAILURES)} 项：")
        for item in FAILURES:
            print(" -", item)
        return 1
    print("全部检查通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
