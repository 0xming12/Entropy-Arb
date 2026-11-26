# NVDA 运维和回放

| 文件 | 用途 |
|---|---|
| `ops.py`、`risk_snapshot.py`、`archive_logs.py` | 执行日报，持仓风险与告警，旧市场数据归档和磁盘巡检 |
| `live-cutover.sh`、`cutover-through-friday.sh`、`deadline.py` | 空仓且订单已清时的切换、延续检查和截止结果 |
| `weekend_restart.py`、`weekend_watch.py`、`check_orders.py` | 周末有仓重启、恢复状态核对和真实订单号验证 |
| `friday_audit.py`、`io_report.py`、`observe-iostat.sh` | 提前收盘审计和逐秒磁盘指标 |
| `capture_inputs.py`、`transfer.py`、`fees.py`、`replay.py` | 在独立环境准备公开行情和资金费数据，串行运行吃单和 ALO 回放。见 [回放说明](REPLAY.md)。 |
| `server.py`、`saved_results.py` | 通过 SSM 查询服务器已经写好的日报、风险快照和审计结果 |

## 连接与服务器设置

从控制机读取 EC2 时，使用已有权限的 AWS CLI 会话及 Session Manager Plugin。通过环境变量提供 `ENTROPY_NVDA_INSTANCE`。需要选择 AWS 配置时设置 `AWS_PROFILE`、`AWS_REGION`，也可用 `AWS_CLI` 指定可执行文件。服务器端服务用实例角色发 SNS，`/etc/entropy-nvda-ops.conf` 的字段见 [示例](ops.conf.example)。两边账户标识在运行时由服务器状态读取。实际交易配置、凭据、交易状态和行情数据由部署环境单独保管。

涉及起停服务的脚本对应 2026 年 11 月的具体维护窗口。应用前核对日期、部署路径、运行版本及实例角色。复现历史回放只使用独立目录里的公开行情和隔离配置。下面的检查只运行本地模拟和语法检查。

```sh
python3 deployment/nvda_ops/test_ops.py
nice -n 19 python3 deployment/nvda_ops/test_replay.py
bash -n deployment/nvda_ops/live-cutover.sh
bash -n deployment/nvda_ops/cutover-through-friday.sh
```
