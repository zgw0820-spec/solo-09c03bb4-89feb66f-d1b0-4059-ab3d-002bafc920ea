"""Gateway-handover (采集网关接替) end-to-end self-tests.

Runs in-process with ASGITransport exactly like tests/selftest.py, reusing its
injectable sender and worker helpers.

Covered acceptance points:
  K. 正常接替全链路：首报走队列、同事务派单+切网关+按首报刷新设备状态
     （即使原网关已有更大序号工单）
  L. 切换后迟到上报：已入队的旧网关消息与未注明网关的消息被终端拦截留痕，
     新网关消息照常派单
  M. 隔离重放的旧网关消息同样被拦截结束投递
  N. 接替首报重投/回执丢失：同一首报只生成一张工单、网关不重复切换
  O. 首报受理前原网关照常派单（停止工作器制造投递窗口）
  P. 每设备仅允许接替一次：网页 409、接收端 HANDOVER_LIMIT
  Q. 服务重启后网关归属与留痕仍可查实
  R. 未注明网关归原网关；同网关接替/缺新网关编号被拒
"""
from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
import tempfile
import threading
import time
from pathlib import Path

import httpx
import uvicorn

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_TMP = tempfile.mkdtemp(prefix="maint-handover-")
os.environ.setdefault("RECEIVER_DB", os.path.join(_TMP, "receiver.db"))
os.environ.setdefault("WEB_DB", os.path.join(_TMP, "web.db"))

from fastapi.testclient import TestClient  # noqa: E402

from receiver.app import create_app as create_receiver  # noqa: E402
from web.app import create_app as create_web  # noqa: E402
from web import db as webdb  # noqa: E402
from worker.worker import Worker  # noqa: E402
from tests.selftest import AsgiSender, wait_for  # noqa: E402

PASS = 0
FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS
    if cond:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAILURES.append(f"{name} {detail}")
        print(f"  [FAIL] {name} {detail}")


def main() -> int:
    tmp = _TMP
    web_db = os.environ["WEB_DB"]
    recv_db = os.environ["RECEIVER_DB"]

    receiver_app = create_receiver(db_path=recv_db)
    web_app = create_web(db_path=web_db, receiver_url="http://receiver")
    web_client = TestClient(web_app)
    sender = AsgiSender(receiver_app)
    worker = Worker(db_path=web_db, receiver_url="http://receiver",
                    sender=sender)

    bg = threading.Thread(target=worker.run_forever, daemon=True)
    bg.start()

    def status(key: str) -> str:
        return web_client.get(f"/api/events/{key}").json()["status"]

    def recv_post(payload: dict) -> httpx.Response:
        async def _post() -> httpx.Response:
            transport = httpx.ASGITransport(app=receiver_app)
            async with httpx.AsyncClient(
                    transport=transport, base_url="http://r") as c:
                return await c.post("/api/dispatch", json=payload)
        return asyncio.new_event_loop().run_until_complete(_post())

    def recv_get(path: str) -> dict:
        async def _get() -> dict:
            transport = httpx.ASGITransport(app=receiver_app)
            async with httpx.AsyncClient(
                    transport=transport, base_url="http://r") as c:
                return (await c.get(path)).json()
        return asyncio.new_event_loop().run_until_complete(_get())

    def gateway_of(device: str) -> dict | None:
        rows = recv_get("/api/gateways")["gateways"]
        return next((g for g in rows if g["device_id"] == device), None)

    def audit_for(event_key: str) -> list[dict]:
        return [a for a in recv_get("/api/gateway-audit")["gateway_audit"]
                if a["event_key"] == event_key]

    # ============================================ K. 正常接替全链路
    print("\n[K] 网关接替：首报派单+切网关+按首报刷新状态（越过更大序号）")
    # K.1 设备先在原网关 GW-OLD 上产生两条事件（seq 1、2）
    r = web_client.post("/api/events", json={
        "device_id": "DEV-H1", "event_type": "fault", "team_no": "T1",
        "gateway_no": "GW-OLD"})
    r.raise_for_status()
    h1_f1 = r.json()["event_key"]
    r = web_client.post("/api/events", json={
        "device_id": "DEV-H1", "event_type": "fault", "team_no": "T1",
        "gateway_no": "GW-OLD"})
    r.raise_for_status()
    h1_f2 = r.json()["event_key"]
    assert wait_for(lambda: status(h1_f2) == "accepted")
    gw = gateway_of("DEV-H1")
    check("K1 首次上报登记原/当前网关均为 GW-OLD",
          gw is not None and gw["original_gateway"] == "GW-OLD"
          and gw["current_gateway"] == "GW-OLD"
          and gw["handover_count"] == 0, json.dumps(gw, ensure_ascii=False))
    state = recv_get("/api/device-state")["device_state"]
    st = next(d for d in state if d["device_id"] == "DEV-H1")
    check("K2 接替前设备最新序号为 2/fault",
          st["latest_seq"] == 2 and st["latest_type"] == "fault", str(st))

    # K.2 以一条 recovery（seq 3）作为接替首报，新网关 GW-NEW
    r = web_client.post("/api/events", json={
        "device_id": "DEV-H1", "event_type": "recovery", "team_no": "T1",
        "gateway_no": "GW-NEW", "is_handover": True,
        "description": "更换采集网关"})
    r.raise_for_status()
    handover_key = r.json()["event_key"]
    check("K3 接替首报序号递增为 3 且标记 is_handover",
          handover_key == "DEV-H1:3" and r.json()["is_handover"] is True)
    assert wait_for(lambda: status(handover_key) == "accepted")

    ev = web_client.get(f"/api/events/{handover_key}").json()
    check("K4 接替首报走现有队列并被接收端受理",
          ev["status"] == "accepted" and ev["payload"]["is_handover"] is True
          and ev["payload"]["gateway_no"] == "GW-NEW")
    gw = gateway_of("DEV-H1")
    check("K5 当前网关已切换为 GW-NEW，原网关保留，接替次数=1",
          gw["current_gateway"] == "GW-NEW"
          and gw["original_gateway"] == "GW-OLD"
          and gw["handover_count"] == 1
          and gw["handover_event_key"] == handover_key,
          json.dumps(gw, ensure_ascii=False))
    state = recv_get("/api/device-state")["device_state"]
    st = next(d for d in state if d["device_id"] == "DEV-H1")
    check("K6 设备状态按首报更新为 seq=3/recovery",
          st["latest_seq"] == 3 and st["latest_type"] == "recovery"
          and st["latest_event_key"] == handover_key, str(st))
    wo = recv_get("/api/work-orders")["work_orders"]
    wo_h = next(w for w in wo if w["event_key"] == handover_key)
    check("K7 接替首报生成一张工单且带网关/接替标记",
          wo_h["is_handover"] == 1 and wo_h["gateway_no"] == "GW-NEW"
          and sum(1 for w in wo if w["event_key"] == handover_key) == 1)
    au = audit_for(handover_key)
    check("K8 网关留痕记录 handover 动作（GW-OLD -> GW-NEW）",
          len(au) == 1 and au[0]["action"] == "handover"
          and "GW-NEW" in (au[0]["reason"] or ""),
          json.dumps(au, ensure_ascii=False))

    # ============================================ K2. 低序号接替首报
    print("\n[K2] 接替首报序号更小：仍按首报刷新设备状态（越过更大序号工单）")
    # 直接构造：原网关已有 seq=5 的工单，接替首报 seq=2（迟到链路场景）
    d1 = recv_post({
        "event_key": "DEV-LOW:5", "device_id": "DEV-LOW", "seq": 5,
        "event_type": "fault", "team_no": "T1", "gateway_no": "GW-OLD"})
    check("K2.1 原网关高序号事件先派单",
          d1.status_code == 200 and d1.json()["duplicate"] is False)
    d2 = recv_post({
        "event_key": "DEV-LOW:2", "device_id": "DEV-LOW", "seq": 2,
        "event_type": "recovery", "team_no": "T1", "gateway_no": "GW-NEW",
        "is_handover": True})
    check("K2.2 低序号接替首报受理并切换网关",
          d2.status_code == 200 and d2.json()["handover"] is True
          and d2.json()["state_updated"] is True
          and d2.json()["current_gateway"] == "GW-NEW", str(d2.json()))
    st = next(d for d in recv_get("/api/device-state")["device_state"]
              if d["device_id"] == "DEV-LOW")
    check("K2.3 设备状态按低序号首报刷新（recovery/2），不被高序号旧工单阻挡",
          st["latest_seq"] == 2 and st["latest_type"] == "recovery"
          and st["latest_event_key"] == "DEV-LOW:2", str(st))
    wo = recv_get("/api/work-orders")["work_orders"]
    check("K2.4 两张工单各自独立存在（接替首报仍生成自己的工单）",
          sum(1 for w in wo if w["device_id"] == "DEV-LOW") == 2)

    # ============================================ L. 迟到上报/已入队拦截
    print("\n[L] 切换后旧网关迟到上报（含已入队消息）：留痕并结束投递")
    worker.stop()
    bg.join(timeout=3)

    # 三条消息在切换生效后才被工作器领取
    r = web_client.post("/api/events", json={
        "device_id": "DEV-H1", "event_type": "fault", "team_no": "T1",
        "gateway_no": "GW-OLD"})  # 旧网关迟到消息
    r.raise_for_status()
    late_old = r.json()["event_key"]
    r = web_client.post("/api/events", json={
        "device_id": "DEV-H1", "event_type": "fault", "team_no": "T1"})
    # 未注明 -> 归原网关 GW-OLD -> 同样拦截
    r.raise_for_status()
    late_unmarked = r.json()["event_key"]
    r = web_client.post("/api/events", json={
        "device_id": "DEV-H1", "event_type": "recovery", "team_no": "T1",
        "gateway_no": "GW-NEW"})  # 新网关 -> 正常派单
    r.raise_for_status()
    new_gw_event = r.json()["event_key"]

    # 确认它们已入队（pending）但尚未投递
    assert all(status(k) == "pending"
               for k in (late_old, late_unmarked, new_gw_event))

    manual = Worker(db_path=web_db, receiver_url="http://receiver",
                    sender=AsgiSender(receiver_app))
    manual.run_once()

    check("L1 已入队的旧网关消息被终端拦截（suppressed，不重试不隔离）",
          status(late_old) == "suppressed", status(late_old))
    check("L2 未注明网关的消息归原网关后同样被拦截",
          status(late_unmarked) == "suppressed", status(late_unmarked))
    check("L3 新网关普通上报照常派单",
          status(new_gw_event) == "accepted", status(new_gw_event))

    for key in (late_old, late_unmarked):
        ev = web_client.get(f"/api/events/{key}").json()
        check(f"L4 {key} 接收端响应为 GATEWAY_NOT_CURRENT 且标记 terminal",
              (ev["last_response"] or {}).get("detail", {})
              .get("reason_code") == "GATEWAY_NOT_CURRENT"
              and ev["last_response"]["detail"].get("terminal") is True,
              json.dumps(ev.get("last_response"), ensure_ascii=False))
        au = audit_for(key)
        check(f"L5 {key} 网关留痕存在 suppressed 记录",
              len(au) == 1 and au[0]["action"] == "suppressed"
              and au[0]["reason_code"] == "GATEWAY_NOT_CURRENT",
              json.dumps(au, ensure_ascii=False))

    # 被抑制的消息不会再次被领取（队列已排空，再跑一次无效果）
    again = manual.run_once()
    check("L6 suppressed 消息不再参与投递（二次排空为 0）", again == 0)
    wo = recv_get("/api/work-orders")["work_orders"]
    check("L7 被拦截消息没有产生工单",
          all(w["event_key"] not in (late_old, late_unmarked) for w in wo))
    rej = recv_get("/api/rejections")["rejections"]
    check("L8 接收端拒收记录中留存两条网关拦截",
          sum(1 for x in rej
              if x["reason_code"] == "GATEWAY_NOT_CURRENT") == 2,
          f"count={sum(1 for x in rej if x['reason_code'] == 'GATEWAY_NOT_CURRENT')}")

    # ======================================== M. 隔离消息重放后被网关拦截
    print("\n[M] 旧网关隔离故障补全班组号重放：仍被拦截结束投递")
    r = web_client.post("/api/events", json={
        "device_id": "DEV-H1", "event_type": "fault",
        "gateway_no": "GW-OLD"})  # 缺班组号
    r.raise_for_status()
    q_key = r.json()["event_key"]
    m_worker = Worker(db_path=web_db, receiver_url="http://receiver",
                      sender=AsgiSender(receiver_app))
    m_worker.run_once()
    time.sleep(1.2)
    m_worker.run_once()
    check("M1 旧网关缺班组号故障先按原规则两次拒收进入隔离",
          status(q_key) == "quarantined", status(q_key))

    # 补全班组号重放（经隔离重放路径重新入队）
    r = web_client.post(f"/api/quarantine/{q_key}/replay",
                        json={"team_no": "T1"})
    r.raise_for_status()
    time.sleep(0.2)
    m_worker.run_once()
    check("M2 隔离重放的旧网关消息被网关规则终端拦截（suppressed）",
          status(q_key) == "suppressed", status(q_key))
    ev = web_client.get(f"/api/events/{q_key}").json()
    check("M3 重放拦截原因码为 GATEWAY_NOT_CURRENT",
          (ev["last_response"] or {}).get("detail", {})
          .get("reason_code") == "GATEWAY_NOT_CURRENT")
    q = web_client.get("/api/quarantine").json()["quarantine"]
    qrow = next(x for x in q if x["event_key"] == q_key)
    check("M4 隔离记录保留并标记已重放（消息不丢失、全链路可查）",
          qrow["replay_status"] == "replayed" and qrow["replayed_at"])

    # ================================ N. 接替首报重投：一张工单/一次切换
    print("\n[N] 回执丢失后同一接替首报重投：幂等返回、不重复派单/切换")
    payload = {
        "event_key": "DEV-H2:1", "device_id": "DEV-H2", "seq": 1,
        "event_type": "fault", "team_no": "T9", "gateway_no": "GW-X",
        "is_handover": True}
    first = recv_post(payload)
    second = recv_post(payload)  # 模拟回执丢失后的重投
    check("N1 首报首次受理成功",
          first.status_code == 200 and first.json()["duplicate"] is False
          and first.json()["handover"] is True, str(first.json()))
    check("N2 重投幂等返回 duplicate=true 且同工单号",
          second.status_code == 200 and second.json()["duplicate"] is True
          and second.json()["work_order_no"]
          == first.json()["work_order_no"], str(second.json()))
    gw = gateway_of("DEV-H2")
    check("N3 重投没有重复切换（接替次数仍为 1，当前网关 GW-X）",
          gw["handover_count"] == 1 and gw["current_gateway"] == "GW-X",
          json.dumps(gw, ensure_ascii=False))
    wo = recv_get("/api/work-orders")["work_orders"]
    check("N4 同一首报只有一张工单",
          sum(1 for w in wo if w["event_key"] == "DEV-H2:1") == 1)

    # 工作器层也验证一次：强制已 accepted 的接替首报再走一遍队列
    r = web_client.post("/api/events", json={
        "device_id": "DEV-H3", "event_type": "fault", "team_no": "T9",
        "gateway_no": "GW-Y", "is_handover": True})
    r.raise_for_status()
    h3 = r.json()["event_key"]
    n_worker = Worker(db_path=web_db, receiver_url="http://receiver",
                      sender=AsgiSender(receiver_app))
    n_worker.run_once()
    check("N5 队列内接替首报正常受理", status(h3) == "accepted")
    conn = webdb.connect(web_db)
    with webdb.transaction(conn):
        conn.execute(
            "UPDATE events SET status='pending', next_attempt_at=?, "
            "claimed_at=NULL WHERE event_key=?", (webdb.utcnow(), h3))
    conn.close()
    n_worker.run_once()
    wo = recv_get("/api/work-orders")["work_orders"]
    check("N6 工作器重投后仍只有一张工单、事件保持 accepted",
          sum(1 for w in wo if w["event_key"] == h3) == 1
          and status(h3) == "accepted")

    # ================================ O. 首报受理前原网关照常派单
    print("\n[O] 接替首报未受理前，原网关普通上报照常派单")
    # 先让设备在 GW-A 上有历史
    r = web_client.post("/api/events", json={
        "device_id": "DEV-H4", "event_type": "fault", "team_no": "T1",
        "gateway_no": "GW-A"})
    r.raise_for_status()
    h4_base = r.json()["event_key"]
    o_worker = Worker(db_path=web_db, receiver_url="http://receiver",
                      sender=AsgiSender(receiver_app))
    o_worker.run_once()
    assert status(h4_base) == "accepted"

    # 接替首报与一条原网关普通上报同时停在队列里（不启动投递）
    r = web_client.post("/api/events", json={
        "device_id": "DEV-H4", "event_type": "fault", "team_no": "T1",
        "gateway_no": "GW-B", "is_handover": True})
    r.raise_for_status()
    h4_handover = r.json()["event_key"]
    r = web_client.post("/api/events", json={
        "device_id": "DEV-H4", "event_type": "recovery", "team_no": "T1",
        "gateway_no": "GW-A"})
    r.raise_for_status()
    h4_old = r.json()["event_key"]
    check("O1 接替首报受理前当前网关仍是 GW-A",
          gateway_of("DEV-H4")["current_gateway"] == "GW-A")
    # FIFO：先投递接替首报；这里单独验证“受理前”窗口——把首报卡住，
    # 先直接让原网关消息投递：模拟首报仍在途中时旧网关消息到达接收端。
    direct = recv_post({
        "event_key": "DEV-H4:99", "device_id": "DEV-H4", "seq": 99,
        "event_type": "recovery", "team_no": "T1", "gateway_no": "GW-A"})
    check("O2 首报受理前原网关消息接收端正常派单（200 非抑制）",
          direct.status_code == 200 and direct.json()["duplicate"] is False,
          str(direct.status_code))
    # 现在排空队列：接替生效后，排队的 GW-A 旧消息应被拦截
    o_worker.run_once()
    check("O3 接替首报受理后切换为 GW-B",
          gateway_of("DEV-H4")["current_gateway"] == "GW-B"
          and status(h4_handover) == "accepted")
    check("O4 切换后才轮到的原网关排队消息被抑制",
          status(h4_old) == "suppressed", status(h4_old))

    # ================================ P. 每设备仅允许接替一次
    print("\n[P] 二次接替：网页前置 409、接收端权威 HANDOVER_LIMIT")
    r = web_client.post("/api/events", json={
        "device_id": "DEV-H1", "event_type": "fault", "team_no": "T1",
        "gateway_no": "GW-THIRD", "is_handover": True})
    check("P1 网页拒绝同一设备二次接替（409）",
          r.status_code == 409 and "接替" in r.json()["detail"],
          str(r.status_code))
    # 绕过网页直接打接收端：权威兜底
    direct = recv_post({
        "event_key": "DEV-H1:90", "device_id": "DEV-H1", "seq": 90,
        "event_type": "fault", "team_no": "T1", "gateway_no": "GW-THIRD",
        "is_handover": True})
    check("P2 接收端对二次接替终端拒收 HANDOVER_LIMIT（422）",
          direct.status_code == 422
          and direct.json()["detail"]["reason_code"] == "HANDOVER_LIMIT"
          and direct.json()["detail"]["terminal"] is True,
          str(direct.json()))
    check("P3 被拒二次接替没有改变当前网关",
          gateway_of("DEV-H1")["current_gateway"] == "GW-NEW")

    # 接替首报缺新网关编号
    r = web_client.post("/api/events", json={
        "device_id": "DEV-NEWX", "event_type": "fault", "team_no": "T1",
        "is_handover": True})
    check("P4 网页对缺新网关编号的接替首报返回 400", r.status_code == 400)
    direct = recv_post({
        "event_key": "DEV-NEWX:1", "device_id": "DEV-NEWX", "seq": 1,
        "event_type": "fault", "team_no": "T1", "is_handover": True})
    check("P5 接收端对缺网关接替首报终端拒收 HANDOVER_GATEWAY_REQUIRED",
          direct.status_code == 422
          and direct.json()["detail"]["reason_code"]
          == "HANDOVER_GATEWAY_REQUIRED")
    # 新网关与当前网关相同（用未占用的 event_key，避免幂等分支先命中）
    direct = recv_post({
        "event_key": "DEV-H2:7", "device_id": "DEV-H2", "seq": 7,
        "event_type": "fault", "team_no": "T1", "gateway_no": "GW-X",
        "is_handover": True})
    check("P6 同网关接替被拒 HANDOVER_SAME_GATEWAY",
          direct.status_code == 422
          and direct.json()["detail"]["reason_code"]
          == "HANDOVER_SAME_GATEWAY")

    # ================================ Q. 重启后可查实
    print("\n[Q] 服务重启：网关归属、接替结果与留痕持久可查")
    # 用真实 uvicorn 端口承载重启后的接收端，网页代理走真实 HTTP。
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        free_port = s.getsockname()[1]
    recv_app2 = create_receiver(db_path=recv_db)
    config = uvicorn.Config(recv_app2, host="127.0.0.1", port=free_port,
                            log_level="warning")
    server = uvicorn.Server(config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    assert wait_for(lambda: server.started, timeout=10.0)

    try:
        async def _restart_query():
            async with httpx.AsyncClient(
                    base_url=f"http://127.0.0.1:{free_port}") as c:
                g = (await c.get("/api/gateways")).json()["gateways"]
                a = (await c.get("/api/gateway-audit")).json()["gateway_audit"]
                return g, a

        g2, a2 = asyncio.new_event_loop().run_until_complete(_restart_query())
        h1 = next(g for g in g2 if g["device_id"] == "DEV-H1")
        check("Q1 重启后 DEV-H1 当前网关仍为 GW-NEW、接替次数 1",
              h1["current_gateway"] == "GW-NEW"
              and h1["original_gateway"] == "GW-OLD"
              and h1["handover_count"] == 1, json.dumps(h1, ensure_ascii=False))
        check("Q2 重启后网关留痕完整（含 handover 与多条 suppressed）",
              any(x["action"] == "handover" for x in a2)
              and sum(1 for x in a2 if x["action"] == "suppressed") >= 4,
              f"suppressed={sum(1 for x in a2 if x['action'] == 'suppressed')}")

        web_app2 = create_web(
            db_path=os.path.join(tmp, "web_restart_proxy.db"),
            receiver_url=f"http://127.0.0.1:{free_port}")
        wc2 = TestClient(web_app2)
        proxy = wc2.get("/api/receiver/gateways")
        check("Q3 网页代理可查询接收端当前网关与接替结果（200）",
              proxy.status_code == 200
              and any(g["device_id"] == "DEV-H1"
                      for g in proxy.json()["gateways"]))
        proxy_audit = wc2.get("/api/receiver/gateway-audit")
        check("Q4 网页代理可查询网关留痕（200）",
              proxy_audit.status_code == 200
              and len(proxy_audit.json()["gateway_audit"]) == len(a2))
    finally:
        server.should_exit = True
        server_thread.join(timeout=10)

    # 重启后 suppressed 事件仍是终态、不会被新工作器重新投递
    recv_app2 = create_receiver(db_path=recv_db)
    restarted_worker = Worker(db_path=web_db, receiver_url="http://receiver",
                              sender=AsgiSender(recv_app2))
    processed = restarted_worker.run_once()
    check("Q5 重启后 suppressed/accepted 终态事件不被重新投递",
          all(status(k) in ("suppressed", "accepted")
              for k in (late_old, late_unmarked, q_key, h4_old)))
    check("Q6 重启排空仅处理真正 pending 的事件（终态不动）",
          processed == 0, f"processed={processed}")

    # ================================ R. 未注明网关归原网关
    print("\n[R] 未注明网关归原网关；接替后未注明消息按原网关拦截")
    r = web_client.post("/api/events", json={
        "device_id": "DEV-H5", "event_type": "fault", "team_no": "T1"})
    r.raise_for_status()
    h5_1 = r.json()["event_key"]
    r_worker = Worker(db_path=web_db, receiver_url="http://receiver",
                      sender=AsgiSender(receiver_app))
    r_worker.run_once()
    gw = gateway_of("DEV-H5")
    check("R1 首报未注明网关：原/当前网关登记为 NULL（未注明）",
          gw is not None and gw["original_gateway"] is None
          and gw["current_gateway"] is None, json.dumps(gw, ensure_ascii=False))
    check("R2 未注明网关的普通上报正常派单", status(h5_1) == "accepted")
    # 接替到 GW-Z
    r = web_client.post("/api/events", json={
        "device_id": "DEV-H5", "event_type": "recovery", "team_no": "T1",
        "gateway_no": "GW-Z", "is_handover": True})
    r.raise_for_status()
    h5_h = r.json()["event_key"]
    r_worker.run_once()
    check("R3 接替后当前网关为 GW-Z",
          gateway_of("DEV-H5")["current_gateway"] == "GW-Z"
          and status(h5_h) == "accepted")
    # 再发一条未注明的 -> 归原网关（NULL）!= GW-Z -> 拦截
    r = web_client.post("/api/events", json={
        "device_id": "DEV-H5", "event_type": "fault", "team_no": "T1"})
    r.raise_for_status()
    h5_late = r.json()["event_key"]
    r_worker.run_once()
    check("R4 接替后未注明消息归原网关并被拦截",
          status(h5_late) == "suppressed", status(h5_late))

    # ============================================================== 汇总
    total = PASS + len(FAILURES)
    print("\n" + "=" * 60)
    print(f"网关接替自测结果：{PASS}/{total} 通过")
    if FAILURES:
        print("失败项：")
        for f in FAILURES:
            print("  -", f)
        return 1
    print("全部网关接替自测通过。")
    print(f"临时数据库目录（可手动检查）：{tmp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
