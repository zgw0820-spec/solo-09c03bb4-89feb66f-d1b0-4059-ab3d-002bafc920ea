# 检修事件投递系统（FastAPI + SQLite + 消费工作器）

模拟"检修系统 → 接收端"的可靠事件投递链路：操作网页按设备号提交带递增序号的
故障/恢复事件，消费工作器负责投递；班组号缺失的故障会被接收端**两次明确拒收后
进入隔离**，值班员补全班组号后可重放；全过程保证不重复派单、隔离消息不丢失。

支持**采集网关接替**：值班员在操作台为设备填写新网关编号，用一条当前故障或恢复
上报作为接替首报；接收端受理首报时在同一事务内派单、切换当前网关并按首报更新
设备状态；切换后旧网关的迟到/重放消息一律留痕并结束投递。

## 组件

| 组件 | 代码 | 端口 | 说明 |
| --- | --- | --- | --- |
| 检修接收端 | `receiver/` | 8001→8000 | 班组号校验、422 显式拒收、`UNIQUE(event_key)` 幂等派单、设备状态仅按更大序号推进、拒收/派单记录留存；网关登记/接替/拦截的权威记录方（`device_gateways`、`gateway_audit`） |
| 操作网页 | `web/` | 8000 | 事件提交（按设备号递增 seq，可注明网关编号/标记接替首报）、事件/隔离/日志/网关接替/网关留痕查询、补全班组号重放、接收端状态代理；单文件前端 `web/static/index.html` |
| 消费工作器 | `worker/worker.py` | — | 轮询共享 SQLite，条件 UPDATE 行级领取，退避重试，两次显式拒收隔离，网关终端拒收进入 `suppressed` 终态，崩溃陈旧锁恢复 |
| 存储 | SQLite (WAL) | 卷 | `web-data` 由 web/worker 共享；`receiver-data` 接收端独占；含旧卷自动加列迁移 |

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
                     ├── 422 网关终端拒收 ──▶ suppressed（留痕，结束投递）
                     └── 网络错误/5xx ──退避──▶ pending
```

## 采集网关接替规则

- **发起方式**：操作台提交上报时勾选"以此条上报发起网关接替首报"并填写新网关
  编号（故障/恢复均可）。首报是一条普通队列事件，沿用现有 store-and-forward
  链路，不另起通道。每台设备仅允许接替一次，网页前置 409、接收端权威兜底
  （`HANDOVER_LIMIT`）。
- **网关归属**：接收端在 `device_gateways` 记录每台设备的原网关、当前网关与
  接替次数。上报可注明 `gateway_no`，**未注明的上报归原网关**；设备首条消息即
  接替首报时原网关留空。
- **受理原子性**：接替首报受理在**同一数据库事务**内完成派单、当前网关切换、
  设备状态按首报刷新——即使原网关已有序号更大的工单，首报仍更新设备状态。
- **受理前不影响旧网关**：首报仍在队列中（未受理）时，接收端当前网关不变，
  原网关普通上报照常派单。
- **普通上报只允许当前网关派单**：切换后，归属网关（注明值或回退后的原网关）
  与当前网关不一致的消息被终端拒收 `GATEWAY_NOT_CURRENT`。这覆盖**已经入队但
  尚未派单的旧网关消息**以及**经隔离重放的旧网关消息**：接收端写 `reject_records`
  + `gateway_audit` 留痕，工作器把事件置为 `suppressed` 终态，不重试、不隔离、
  不派单。
- **幂等与可查实**：接替首报回执丢失后重投，命中 `UNIQUE(event_key)` 幂等返回
  原工单，只生成一张工单、网关不重复切换。网关归属与全部留痕持久化在接收端
  SQLite，服务重启后网页"网关接替/网关留痕"页仍可查实。

### 网关相关接口（接收端，网页均有代理 `/api/receiver/...`）

| 接口 | 说明 |
| --- | --- |
| `GET /api/gateways` | 每台设备的原网关、当前网关、接替次数、接替首报与时间 |
| `GET /api/gateway-audit` | 登记 `register` / 接替 `handover` / 拦截 `suppressed` 全过程留痕 |
| `POST /api/dispatch` | 新增可选字段 `gateway_no`、`is_handover`；终端拒收响应带 `terminal: true` |

## 自测

```bash
python tests/selftest.py            # 33 项进程内端到端断言（A~G 基线场景）
python tests/gateway_handover.py    # 52 项网关接替专项断言（K~R：接替/迟到/重放/幂等/重启）
python tests/concurrency.py         # 并发提交 + 4 工作器并发投递
python tests/http_integration.py    # 三进程真实 HTTP 集成（含网关接替与接收端重启）
```
