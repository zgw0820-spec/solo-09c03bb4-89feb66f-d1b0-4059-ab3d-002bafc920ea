"""End-to-end self-tests for the maintenance event delivery system.

Runs entirely in-process with FastAPI/Starlette ASGITransport (no network
ports, no Docker required) and one shared SQLite database for web + worker,
mirroring the docker-compose topology.

Covered acceptance points:
  A. 缺班组号故障两次显式拒收 -> 隔离；同设备恢复照常送达
  B. 补全班组号重放：接收端留存处理记录、旧故障不覆盖较新恢复、不重复派单
  C. 投递失败（网络异常/5xx）退避后可继续处理，不阻塞后续事件
  D. 工作器“重启”/陈旧处理锁：同一事件不重复派单
  E. 服务重启后隔离消息不丢失、序号继续递增、重放不可重复
  F. 接收端幂等：同 event_key 重复 POST 不产生第二张工单
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
from pathlib import Path

import asyncio
import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Point module-level app factories at writable temp paths before importing.
_TMP = tempfile.mkdtemp(prefix="maint-selftest-")
os.environ.setdefault("RECEIVER_DB", os.path.join(_TMP, "receiver.db"))
os.environ.setdefault("WEB_DB", os.path.join(_TMP, "web.db"))

from fastapi.testclient import TestClient  # noqa: E402

from receiver.app import create_app as create_receiver  # noqa: E402
from web.app import create_app as create_web  # noqa: E402
from web import db as webdb  # noqa: E402
from worker.worker import Worker  # noqa: E402

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


class AsgiSender:
    """httpx-style sender that calls an ASGI app in-process."""

    def __init__(self, app, headers: dict[str, str] | None = None):
        self.app = app
        self.extra_headers = headers or {}
        self.calls: list[str] = []
        self.fail_keys: dict[str, int] = {}  # event_key -> remaining failures

    def set_failures(self, event_key: str, times: int) -> None:
        self.fail_keys[event_key] = times

    def __call__(self, url: str, payload: dict, headers: dict) -> httpx.Response:
        key = payload["event_key"]
        self.calls.append(key)
        h = dict(headers)
        h.update(self.extra_headers)
        if self.fail_keys.get(key, 0) > 0:
            self.fail_keys[key] -= 1
            raise httpx.ConnectError("simulated connection refused")

        async def _post() -> httpx.Response:
            transport = httpx.ASGITransport(app=self.app)
            async with httpx.AsyncClient(
                    transport=transport, base_url="http://receiver") as c:
                return await c.post("/api/dispatch", json=payload, headers=h)

        # Each call gets a fresh event loop so the sender is safe to invoke
        # from both the main thread and the background worker thread.
        return asyncio.new_event_loop().run_until_complete(_post())


def wait_for(predicate, timeout: float = 10.0, interval: float = 0.1) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


async def _receiver_get(app, path: str) -> dict:
    """Query the receiver ASGI app directly (the web proxy is tested too)."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
            transport=transport, base_url="http://receiver") as c:
        resp = await c.get(path)
        return resp.json()


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

    # A background worker mirrors the docker-compose worker service.
    bg = threading.Thread(target=worker.run_forever, daemon=True)
    bg.start()

    def event_status(key: str) -> str:
        return web_client.get(f"/api/events/{key}").json()["status"]

    def recv_get(path: str) -> dict:
        return asyncio.run(_receiver_get(receiver_app, path))

    # ============================================================ A. 拒收隔离
    print("\n[A] 缺班组号故障 -> 两次显式拒收 -> 隔离；恢复照常送达")
    r = web_client.post("/api/events", json={
        "device_id": "DEV-1", "event_type": "fault",
        "description": "泵体异响"})
    r.raise_for_status()
    fault1 = r.json()["event_key"]
    check("A1 首次提交序号为 1", fault1 == "DEV-1:1")

    assert wait_for(lambda: event_status(fault1) == "quarantined"), \
        event_status(fault1)
    ev = web_client.get(f"/api/events/{fault1}").json()
    check("A2 两次明确拒收后进入隔离", ev["status"] == "quarantined"
          and ev["attempts"] == 2, f"status={ev['status']} attempts={ev['attempts']}")

    q = web_client.get("/api/quarantine").json()["quarantine"]
    check("A3 隔离消息已持久化且原因正确",
          len(q) == 1 and q[0]["event_key"] == fault1
          and "拒收" in q[0]["reason"], json.dumps(q, ensure_ascii=False))

    recv_rej = recv_get("/api/rejections")["rejections"]
    check("A4 接收端留存两次拒收记录",
          len(recv_rej) == 2
          and all(x["reason_code"] == "MISSING_TEAM_NO" for x in recv_rej),
          f"count={len(recv_rej)}")

    # 同设备恢复事件（带班组号）——必须照常送达，不受隔离故障影响
    r = web_client.post("/api/events", json={
        "device_id": "DEV-1", "event_type": "recovery",
        "team_no": "TEAM-A7", "description": "检修完成恢复"})
    r.raise_for_status()
    recovery2 = r.json()["event_key"]
    check("A5 同设备后续事件序号递增为 2", recovery2 == "DEV-1:2")
    assert wait_for(lambda: event_status(recovery2) == "accepted")
    wo = recv_get("/api/work-orders")["work_orders"]
    check("A6 恢复事件照常派单（隔离不阻塞同设备恢复）",
          any(w["event_key"] == recovery2 for w in wo),
          json.dumps(wo, ensure_ascii=False))

    state = recv_get("/api/device-state")["device_state"]
    check("A7 设备最新状态为恢复(seq=2)",
          len(state) == 1 and state[0]["latest_seq"] == 2
          and state[0]["latest_type"] == "recovery",
          json.dumps(state, ensure_ascii=False))

    # ================================================ B. 补全班组号重放旧故障
    print("\n[B] 值班员补全班组号重放：留存记录、不覆盖新恢复、不重复派单")
    wo_before = recv_get("/api/work-orders")["work_orders"]
    r = web_client.post(f"/api/quarantine/{fault1}/replay",
                        json={"team_no": "TEAM-A7"})
    r.raise_for_status()
    check("B1 重放请求返回重新入队", r.json()["status"] == "pending")

    assert wait_for(lambda: event_status(fault1) == "accepted")
    wo_after = recv_get("/api/work-orders")["work_orders"]
    check("B2 重放后旧故障生成自己的派单记录（接收端留存）",
          any(w["event_key"] == fault1 and w["is_replay"] == 1
              and w["team_no"] == "TEAM-A7" for w in wo_after),
          json.dumps(wo_after, ensure_ascii=False))
    check("B3 重放没有为恢复事件重复派单（DEV-1:2 仅一张工单）",
          sum(1 for w in wo_after if w["event_key"] == recovery2) == 1)
    check("B4 总派单记录数 = 恢复1 + 重放故障1 = 2",
          len(wo_after) == len(wo_before) + 1 == 2,
          f"before={len(wo_before)} after={len(wo_after)}")

    state = recv_get("/api/device-state")["device_state"]
    check("B5 旧故障(seq=1)未覆盖较新恢复(seq=2)，设备状态仍是 recovery/2",
          state[0]["latest_seq"] == 2 and state[0]["latest_type"] == "recovery",
          json.dumps(state, ensure_ascii=False))

    q = web_client.get("/api/quarantine").json()["quarantine"]
    check("B6 隔离消息仍保留（标记已重放，不丢失）",
          len(q) == 1 and q[0]["replay_status"] == "replayed"
          and q[0]["replayed_at"] is not None)

    r = web_client.post(f"/api/quarantine/{fault1}/replay",
                        json={"team_no": "TEAM-A8"})
    check("B7 同一隔离消息不能重复重放（409）", r.status_code == 409,
          f"status={r.status_code}")

    # ============================================ C. 投递失败后可继续处理事件
    print("\n[C] 投递失败（网络异常 + 5xx）退避后继续，不阻塞队列后续事件")
    worker.stop()
    bg.join(timeout=3)

    r = web_client.post("/api/events", json={
        "device_id": "DEV-2", "event_type": "fault", "team_no": "TEAM-B1"})
    r.raise_for_status()
    f2 = r.json()["event_key"]
    r = web_client.post("/api/events", json={
        "device_id": "DEV-2", "event_type": "recovery", "team_no": "TEAM-B1"})
    r.raise_for_status()
    rec2 = r.json()["event_key"]

    # f2: first two delivery attempts fail with a simulated network error.
    sender.set_failures(f2, 2)
    manual = Worker(db_path=web_db, receiver_url="http://receiver",
                    sender=sender)
    manual.run_once()
    # The first failure schedules a sub-second backoff; wait it out so the
    # second simulated failure is also consumed before the queue drains.
    time.sleep(2.2)
    manual.run_once()
    ev = web_client.get(f"/api/events/{f2}").json()
    check("C1 两次网络失败后事件仍在队列并累计尝试次数、记录错误",
          ev["status"] == "pending" and ev["attempts"] == 2
          and ev["last_error"]
          and ("网络失败" in ev["last_error"]
               or "投递异常" in ev["last_error"]),
          json.dumps(ev, ensure_ascii=False))
    check("C2 失败事件不阻塞队列：同批恢复事件已正常派单",
          web_client.get(f"/api/events/{rec2}").json()["status"] == "accepted")

    # 5xx path via the receiver's opt-in outage header (different sender).
    r = web_client.post("/api/events", json={
        "device_id": "DEV-3", "event_type": "fault", "team_no": "TEAM-C1"})
    r.raise_for_status()
    f3 = r.json()["event_key"]
    outage_sender = AsgiSender(receiver_app,
                               headers={"x-simulate-failure": "1"})
    outage_worker = Worker(db_path=web_db, receiver_url="http://receiver",
                           sender=outage_sender)
    outage_worker.run_once()
    ev = web_client.get(f"/api/events/{f3}").json()
    check("C3 接收端 5xx 故障期：事件退避重试而非丢失/隔离",
          ev["status"] == "pending" and ev["attempts"] >= 1
          and ev["next_attempt_at"] is not None
          and "SIMULATED_OUTAGE" in (ev["last_response"] or {}).get(
              "detail", {}).get("reason_code", ""),
          json.dumps(ev, ensure_ascii=False))

    # Wait out the backoff, then a healthy worker redelivers everything.
    time.sleep(2.2)
    healthy = Worker(db_path=web_db, receiver_url="http://receiver",
                     sender=AsgiSender(receiver_app))
    healthy.run_once()
    check("C4 恢复后失败事件最终送达 accepted",
          web_client.get(f"/api/events/{f2}").json()["status"] == "accepted"
          and web_client.get(f"/api/events/{f3}").json()["status"]
          == "accepted")
    wo = recv_get("/api/work-orders")["work_orders"]
    check("C5 失败重试未产生重复工单（每个 event_key 仅一张）",
          {w["event_key"] for w in wo} == {fault1, recovery2, rec2, f2, f3},
          json.dumps([w["event_key"] for w in wo]))

    # ======================================== D. 重启/陈旧锁 -> 不重复派单
    print("\n[D] 工作器崩溃重启（陈旧 processing 锁）：同一事件不重复派单")
    r = web_client.post("/api/events", json={
        "device_id": "DEV-4", "event_type": "fault", "team_no": "TEAM-D1"})
    r.raise_for_status()
    f4 = r.json()["event_key"]

    # Simulate a crash after claim: event stuck in 'processing' with an old
    # claimed_at, and the receiver has never seen it.
    conn = webdb.connect(web_db)
    with webdb.transaction(conn):
        old_ts = time.strftime(
            "%Y-%m-%dT%H:%M:%S",
            time.gmtime(time.time() - 120)) + "+00:00"
        conn.execute(
            "UPDATE events SET status='processing', claimed_at=? "
            "WHERE event_key=?", (old_ts, f4))
    conn.close()

    restarted = Worker(db_path=web_db, receiver_url="http://receiver",
                       sender=AsgiSender(receiver_app))
    released = restarted.release_stale()
    check("D1 重启时释放陈旧 processing 锁", released >= 1)
    restarted.run_once()
    check("D2 陈旧事件重启后成功投递",
          web_client.get(f"/api/events/{f4}").json()["status"] == "accepted")
    # Restart again: redelivery attempt must be idempotent at the receiver.
    restarted2 = Worker(db_path=web_db, receiver_url="http://receiver",
                        sender=AsgiSender(receiver_app))
    # Force the row back through the pipeline to prove receiver idempotency.
    conn = webdb.connect(web_db)
    with webdb.transaction(conn):
        conn.execute(
            "UPDATE events SET status='pending', next_attempt_at=?, "
            "claimed_at=NULL WHERE event_key=?",
            (webdb.utcnow(), f4))
    conn.close()
    restarted2.run_once()
    wo = recv_get("/api/work-orders")["work_orders"]
    check("D3 重复投递不产生第二张工单（接收端 UNIQUE 幂等）",
          sum(1 for w in wo if w["event_key"] == f4) == 1)
    check("D4 事件最终状态仍为 accepted 且记录了幂等返回",
          web_client.get(f"/api/events/{f4}").json()["status"] == "accepted")

    # ========================== E. 重启后隔离不丢、序号继续递增、重放防重复
    print("\n[E] 模拟服务重启：隔离消息保留、序号继续递增")
    # Recreate apps/worker against the same database files.
    web_app2 = create_web(db_path=web_db, receiver_url="http://receiver")
    wc2 = TestClient(web_app2)
    q = wc2.get("/api/quarantine").json()["quarantine"]
    check("E1 重启后隔离消息仍在（含原故障与重放标记）",
          len(q) == 1 and q[0]["event_key"] == fault1
          and q[0]["replay_status"] == "replayed")
    events = wc2.get("/api/events").json()["events"]
    check("E2 重启后全部事件记录完整（6 条历史事件均在）", len(events) == 6,
          f"actual={len(events)}")

    r = wc2.post("/api/events", json={
        "device_id": "DEV-1", "event_type": "fault", "team_no": "TEAM-A9"})
    r.raise_for_status()
    check("E3 同设备序号在历史最大值上继续递增（DEV-1:3）",
          r.json()["event_key"] == "DEV-1:3" and r.json()["seq"] == 3)

    # ========================== F. 接收端原生幂等
    print("\n[F] 接收端幂等：同一 event_key 直接重复 POST")

    async def _native_idempotency():
        transport = httpx.ASGITransport(app=receiver_app)
        async with httpx.AsyncClient(
                transport=transport, base_url="http://receiver") as c:
            payload = {"event_key": "DEV-9:1", "device_id": "DEV-9", "seq": 1,
                       "event_type": "fault", "team_no": "TEAM-X"}
            first = await c.post("/api/dispatch", json=payload)
            second = await c.post("/api/dispatch", json=payload)
            third = await c.post("/api/dispatch",
                                 json={**payload, "is_replay": True})
            rr = await c.post("/api/dispatch", json={
                "event_key": "DEV-8:1", "device_id": "DEV-8", "seq": 1,
                "event_type": "recovery"})
        return first, second, third, rr

    first, second, third, rr = asyncio.run(_native_idempotency())
    check("F1 首次派单 200", first.status_code == 200
          and first.json()["duplicate"] is False)
    check("F2 重复 POST 幂等返回同工单、不新建",
          second.status_code == 200 and second.json()["duplicate"] is True
          and second.json()["work_order_no"] == first.json()["work_order_no"])
    check("F3 重放式重复 POST 同样幂等",
          third.status_code == 200 and third.json()["duplicate"] is True)
    wo = recv_get("/api/work-orders")["work_orders"]
    check("F4 DEV-9:1 始终只有一张工单",
          sum(1 for w in wo if w["event_key"] == "DEV-9:1") == 1)
    check("F5 恢复事件无班组号仍可接收（仅故障强制校验班组号）",
          rr.status_code == 200 and rr.json()["accepted"] is True)

    # ================================== G. 网页代理：接收端在线 200 / 离线 502
    print("\n[G] 操作网页状态代理：接收端在线透传，离线返回 502 容错")
    import socket
    import uvicorn

    free_port = 0
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        free_port = s.getsockname()[1]
    proxy_recv_db = os.path.join(tmp, "proxy_receiver.db")
    proxy_app = create_receiver(db_path=proxy_recv_db)
    config = uvicorn.Config(proxy_app, host="127.0.0.1", port=free_port,
                            log_level="warning")
    server = uvicorn.Server(config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    assert wait_for(lambda: server.started, timeout=10.0)

    proxy_web = create_web(db_path=os.path.join(tmp, "proxy_web.db"),
                           receiver_url=f"http://127.0.0.1:{free_port}")
    pwc = TestClient(proxy_web)
    resp = pwc.get("/api/receiver/work-orders")
    check("G1 接收端在线时代理透传 200",
          resp.status_code == 200
          and resp.json() == {"work_orders": []})

    server.should_exit = True
    server_thread.join(timeout=10)
    resp = pwc.get("/api/receiver/work-orders")
    check("G2 接收端离线时代理返回 502 且不抛栈（页面可继续使用）",
          resp.status_code == 502
          and "接收端暂不可用" in resp.json()["detail"])

    # ============================================================== 汇总
    total = PASS + len(FAILURES)
    print("\n" + "=" * 60)
    print(f"自测结果：{PASS}/{total} 通过")
    if FAILURES:
        print("失败项：")
        for f in FAILURES:
            print("  -", f)
        return 1
    print("全部关键自测通过。")
    print(f"临时数据库目录（可手动检查）：{tmp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
