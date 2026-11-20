"""Real-process HTTP integration test (Docker Compose topology, without Docker).

Spawns three independent processes against temp SQLite files:
  * uvicorn receiver.app   (port 8101)
  * uvicorn web.app        (port 8100, talks HTTP to the receiver)
  * python -m worker.worker (shares the web SQLite file)
Then drives the full scenario over real HTTP sockets.
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
TMP = Path(tempfile.mkdtemp(prefix="maint-http-"))
WEB_DB = TMP / "web.db"
RECV_DB = TMP / "receiver.db"
USERSITE = subprocess.run(
    [sys.executable, "-c", "import site;print(site.getusersitepackages())"],
    capture_output=True, text=True).stdout.strip()
ENV = {**os.environ, "PYTHONPATH": f"{ROOT}:{USERSITE}"}


def wait_health(url: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=2).status_code == 200:
                return
        except httpx.HTTPError:
            time.sleep(0.3)
    raise RuntimeError(f"service not healthy: {url}")


def main() -> int:
    procs: list[subprocess.Popen] = []
    try:
        receiver = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "receiver.app:app",
             "--host", "127.0.0.1", "--port", "8101"],
            cwd=ROOT, env={**ENV, "RECEIVER_DB": str(RECV_DB)},
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        procs.append(receiver)
        wait_health("http://127.0.0.1:8101/health")

        web = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "web.app:app",
             "--host", "127.0.0.1", "--port", "8100"],
            cwd=ROOT,
            env={**ENV, "WEB_DB": str(WEB_DB),
                 "RECEIVER_URL": "http://127.0.0.1:8101"},
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        procs.append(web)
        wait_health("http://127.0.0.1:8100/health")

        worker = subprocess.Popen(
            [sys.executable, "-m", "worker.worker"],
            cwd=ROOT,
            env={**ENV, "WEB_DB": str(WEB_DB),
                 "RECEIVER_URL": "http://127.0.0.1:8101",
                 "WORKER_POLL_INTERVAL": "0.3"},
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        procs.append(worker)

        webc = httpx.Client(base_url="http://127.0.0.1:8100", timeout=10)

        # 1. fault without team_no -> quarantine after two explicit rejects
        r = webc.post("/api/events", json={
            "device_id": "PUMP-7", "event_type": "fault",
            "description": "温度异常"})
        r.raise_for_status()
        fault_key = r.json()["event_key"]
        assert fault_key == "PUMP-7:1", fault_key

        # 2. recovery (with team) for same device -> still delivered
        r = webc.post("/api/events", json={
            "device_id": "PUMP-7", "event_type": "recovery",
            "team_no": "TEAM-9"})
        r.raise_for_status()
        rec_key = r.json()["event_key"]
        assert rec_key == "PUMP-7:2", rec_key

        deadline = time.time() + 20
        final: dict = {}
        while time.time() < deadline:
            final = {e["event_key"]: e["status"]
                     for e in webc.get("/api/events").json()["events"]}
            if final.get(fault_key) == "quarantined" \
                    and final.get(rec_key) == "accepted":
                break
            time.sleep(0.4)
        assert final.get(fault_key) == "quarantined", final
        assert final.get(rec_key) == "accepted", final
        print("[1] 缺班组号故障已隔离，同设备恢复已送达:", final)

        q = webc.get("/api/quarantine").json()["quarantine"]
        assert len(q) == 1 and q[0]["event_key"] == fault_key
        print("[2] 隔离消息已持久化:", q[0]["reason"])

        # 3. restart worker mid-way (kill + relaunch); quarantine must survive
        worker.send_signal(signal.SIGTERM)
        worker.wait(timeout=10)
        procs.remove(worker)
        worker2 = subprocess.Popen(
            [sys.executable, "-m", "worker.worker"],
            cwd=ROOT,
            env={**ENV, "WEB_DB": str(WEB_DB),
                 "RECEIVER_URL": "http://127.0.0.1:8101",
                 "WORKER_POLL_INTERVAL": "0.3"},
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        procs.append(worker2)
        time.sleep(1)
        q2 = webc.get("/api/quarantine").json()["quarantine"]
        assert len(q2) == 1 and q2[0]["replay_status"] == "quarantined"
        print("[3] worker 重启后隔离消息仍在")

        # 4. replay with team_no -> accepted, old fault must not overwrite
        #    the newer recovery in receiver device state
        r = webc.post(f"/api/quarantine/{fault_key}/replay",
                      json={"team_no": "TEAM-9"})
        r.raise_for_status()
        deadline = time.time() + 20
        while time.time() < deadline:
            st = webc.get(f"/api/events/{fault_key}").json()["status"]
            if st == "accepted":
                break
            time.sleep(0.4)
        assert st == "accepted", st
        print("[4] 补全班组号重放后旧故障投递成功")

        state = httpx.get(
            "http://127.0.0.1:8101/api/device-state", timeout=10).json()
        dev = next(d for d in state["device_state"]
                   if d["device_id"] == "PUMP-7")
        assert dev["latest_seq"] == 2 and dev["latest_type"] == "recovery", dev
        print("[5] 旧故障未覆盖较新恢复，设备最新状态:", dev)

        orders = httpx.get(
            "http://127.0.0.1:8101/api/work-orders", timeout=10).json()
        keys = sorted(o["event_key"] for o in orders["work_orders"])
        assert keys == ["PUMP-7:1", "PUMP-7:2"], keys
        assert sum(1 for o in orders["work_orders"]
                   if o["event_key"] == rec_key) == 1
        print("[6] 接收端工单（无重复派单）:", keys)

        # 5. receiver reject audit retained
        rejections = httpx.get(
            "http://127.0.0.1:8101/api/rejections", timeout=10).json()
        assert len(rejections["rejections"]) == 2
        print("[7] 接收端留存 2 条拒收记录")

        # 6. static page is served
        page = webc.get("/")
        assert page.status_code == 200 and "检修事件投递操作台" in page.text
        print("[8] 操作网页静态页面可访问")

        print("\nHTTP 端到端集成测试全部通过。")
        return 0
    finally:
        for p in procs:
            p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        print(f"(临时目录: {TMP})")


if __name__ == "__main__":
    raise SystemExit(main())
