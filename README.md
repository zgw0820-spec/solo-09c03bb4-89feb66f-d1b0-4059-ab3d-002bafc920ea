# 检修事件投递系统（FastAPI + SQLite + 消费工作器）

模拟"检修系统 → 接收端"的可靠事件投递链路：操作网页按设备号提交带递增序号的
故障/恢复事件，消费工作器负责投递；班组号缺失的故障会被接收端**两次明确拒收后
进入隔离**，值班员补全班组号后可重放；全过程保证不重复派单、隔离消息不丢失。

## 组件

| 组件 | 代码 | 端口 | 说明 |
| --- | --- | --- | --- |
| 检修接收端 | `receiver/` | 8001→8000 | 班组号校验、422 显式拒收、`UNIQUE(event_key)` 幂等派单、设备状态仅按更大序号推进、拒收/派单记录留存 |
| 操作网页 | `web/` | 8000 | 事件提交（按设备号递增 seq）、事件/隔离/日志查询、补全班组号重放、接收端状态代理；单文件前端 `web/static/index.html` |
| 消费工作器 | `worker/worker.py` | — | 轮询共享 SQLite，条件 UPDATE 行级领取，退避重试，两次显式拒收隔离，崩溃陈旧锁恢复 |
| 存储 | SQLite (WAL) | 卷 | `web-data` 由 web/worker 共享；`receiver-data` 接收端独占 |

## 启动（Docker Compose）

```bash
docker compose up --build
# 操作网页:   http://localhost:8000
# 接收端 API: http://localhost:8001  （/docs 为 Swagger 文档）
```

无 Docker 时可直接本地运行（`pip install -r requirements.txt`）：

```bash
RECEIVER_DB=/tmp/receiver.db uvicorn receiver.app:app --port 8101
WEB_DB=/tmp/web.db RECEIVER_URL=http://127.0.0.1:8101 \
  uvicorn web.app:app --port 8100
WEB_DB=/tmp/web.db RECEIVER_URL=http://127.0.0.1:8101 \
  python -m worker.worker
```

## 关键行为与设计

- **显式拒收 → 隔离**：接收端对缺班组号的 `fault` 返回 `422 MISSING_TEAM_NO`
  并写入 `reject_records`；工作器累计到 2 次显式拒收后把事件写入
  `quarantine` 表（永久保留），不再投递。`recovery` 不强制班组号，同设备后续
  恢复事件是独立行，照常送达。
- **重放**：隔离区补全班组号后，事件以同一 `event_key`、`is_replay=true`
  重新入队。接收端保留全部处理记录；设备状态只接受**严格更大 seq** 的事件，
  因此旧故障重放不会覆盖较新的恢复。同一隔离消息禁止重复重放（409）。
- **不重复派单（双保险）**：
  1. 工作器用 `UPDATE ... WHERE status='pending'` 原子领取，多 worker/重启
     不会领走同一行；崩溃遗留的 `processing` 行在启动时及超时后释放重投。
  2. 接收端 `dispatch_records.event_key UNIQUE`，重复投递幂等返回原工单。
- **失败可继续**：网络异常/5xx/非预期错误按指数退避（0.5s 起，上限 30s）回到
  `pending`，队列中其他事件不受阻塞；写锁竞争自动重试，接收端兜底 503。
- **重启不丢数据**：事件先落库再投递（store-and-forward），隔离表独立持久化，
  序号基于历史 `MAX(seq)` 继续递增。

## 状态机

```
pending ──领取──▶ processing ──2xx──▶ accepted
                     ├── 422 显式拒收(第1次) ──▶ rejected ──到期──▶ pending
                     ├── 422 显式拒收(第2次) ──▶ quarantined ──重放──▶ pending
                     └── 网络错误/5xx ──退避──▶ pending
```

## 自测

```bash
python tests/selftest.py          # 33 项进程内端到端断言（A~G 全场景）
python tests/concurrency.py       # 并发提交 + 4 工作器并发投递
python tests/http_integration.py  # 三进程真实 HTTP 集成（等同 compose 拓扑）
```
