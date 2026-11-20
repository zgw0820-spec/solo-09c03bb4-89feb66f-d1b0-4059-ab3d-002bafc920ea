"""Concurrency checks: parallel workers and parallel submissions.

* N worker threads share one SQLite database and one receiver; each event must
  be dispatched exactly once (no duplicate work orders, no lost events).
* Many threads submit events for the same device concurrently: per-device seq
  must be a gap-free 1..N unique sequence.
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_TMP = tempfile.mkdtemp(prefix="maint-concurrency-")
os.environ.setdefault("RECEIVER_DB", os.path.join(_TMP, "receiver.db"))
os.environ.setdefault("WEB_DB", os.path.join(_TMP, "web.db"))

from fastapi.testclient import TestClient  # noqa: E402

from receiver.app import create_app as create_receiver  # noqa: E402
from web.app import create_app as create_web  # noqa: E402
from worker.worker import Worker  # noqa: E402
from tests.selftest import AsgiSender  # noqa: E402

receiver_app = create_receiver(db_path=os.environ["RECEIVER_DB"])
web_app = create_web(db_path=os.environ["WEB_DB"],
                     receiver_url="http://receiver")
web_client = TestClient(web_app)

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (" " + detail if not cond else ""))
    if not cond:
        failures.append(name)


# ---------------------------------------------------------- parallel submit
print("[H] 同设备并发提交：序号唯一且连续")
DEVICE = "DEV-CONC"
N_SUBMIT = 20


def submit(_i: int) -> str:
    r = web_client.post("/api/events", json={
        "device_id": DEVICE, "event_type": "fault", "team_no": "TEAM-Z"})
    r.raise_for_status()
    return r.json()["event_key"]


with ThreadPoolExecutor(max_workers=8) as pool:
    keys = list(pool.map(submit, range(N_SUBMIT)))

seqs = sorted(int(k.split(":")[1]) for k in keys)
check("H1 并发提交无重复序号", len(set(keys)) == N_SUBMIT)
check("H2 序号连续为 1..N", seqs == list(range(1, N_SUBMIT + 1)),
      f"got={seqs}")

# --------------------------------------------------------- parallel workers
print("[I] 多工作器并发投递：不重复派单、不丢事件")
N_WORKERS = 4
stop = threading.Event()


def run_worker() -> None:
    w = Worker(db_path=os.environ["WEB_DB"],
               receiver_url="http://receiver",
               sender=AsgiSender(receiver_app))
    while not stop.is_set():
        if w.run_once() == 0:
            stop.wait(0.05)


threads = [threading.Thread(target=run_worker, daemon=True)
           for _ in range(N_WORKERS)]
for t in threads:
    t.start()

deadline = 30
import time
end = time.time() + deadline
while time.time() < end:
    pending = web_client.get("/api/events").json()["events"]
    if all(e["status"] == "accepted" for e in pending):
        break
    time.sleep(0.2)
stop.set()
for t in threads:
    t.join(timeout=5)

events = web_client.get("/api/events").json()["events"]
check("I1 所有事件最终 accepted",
      all(e["status"] == "accepted" for e in events),
      str([(e["event_key"], e["status"]) for e in events if e["status"] != "accepted"]))


async def get_orders():
    transport = httpx.ASGITransport(app=receiver_app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://receiver") as c:
        return (await c.get("/api/work-orders")).json()["work_orders"]


orders = asyncio.run(get_orders())
order_keys = [o["event_key"] for o in orders]
check("I2 接收端工单数与事件数一致（无丢失）",
      len(orders) == len(events),
      f"orders={len(orders)} events={len(events)}")
check("I3 无重复工单（同一事件未被重复派单）",
      len(set(order_keys)) == len(order_keys)
      and set(order_keys) == {e["event_key"] for e in events})
check("I4 全部工单 is_replay=0（均为首次投递）",
      all(o["is_replay"] == 0 for o in orders))

print("\n并发检查结果：",
      "全部通过" if not failures else f"{len(failures)} 项失败 {failures}")
raise SystemExit(1 if failures else 0)
