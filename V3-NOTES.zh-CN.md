# Entropy Arb v3 实盘修复说明（基于 v2-order-guards）

本版本在 `entropy-arb-v2-order-guards` 的订单安全框架基础上，修复了阻碍小资金实盘测试的 4 个 P0 级高危缺陷与 5 组 P1 级执行/接口问题。

## 一、P0 级修复

1. **拆分 `reduce_only` 信号触发门槛与最差滑点底线，彻底消除误强平**
   - 原因：v2 在 `_plan()` 中对 `reduce_only=True` 直接使用 `reduce_only_min_worst_bps (-50 bps)`，导致开机首次风控轮询未返回、休市切换、短暂费率/余额查询超时或触发熔断时，立刻以最高 `-50 bps` 亏本市价砸平健康对冲仓位。
   - 修复：新增 `_plan_threshold_bps()` 与可选配置 `execution.reduce_only_exit_threshold_bps`（默认 `null`，即沿用正常 `_eff_threshold()` 回归平仓，豁免休市缓冲与不利资金费率偏移）；`reduce_only_min_worst_bps` 仅用于成交滑点最差底线复核。
   - 启动时先完成首轮费率与可用保证金查询，再启动策略循环。

2. **非零中枢（`midline_bps != 0`）下的熔断盈亏守恒修复**
   - 原因：v2 直接把绝对价差 `fill_edge` 传给 `circuit_breaker.record_trade_pnl()`，在 `midline_bps != 0` 时，正常顺上轨/下轨分批建仓会连续记为负收益，极易误触 `max_consecutive_losses` 与 `max_daily_loss_usd`。
   - 修复：引入 `_midline_adjusted_edge_usd()` 按相对当前中枢计算半程净边际（`midline=0` 时与 `fill_edge` 完全等价；开仓 + 平仓之和严格等于实际闭环现金盈亏），同时继续由 `daily_mtm_pnl()` 兜底真实基差偏移亏损。

3. **修正 Hyperliquid HIP-3 实时费率计算公式并覆盖 `tradexyz` 对冲腿**
   - 按 Hyperliquid 官方 HIP-3 费率规则修正 `HLVenue.fetch_taker_fee_bps()`：`scale_if_hip3 = deployerFeeScale + 1`（`<1`）或 `deployerFeeScale * 2`（`>=1`），再乘 `growthMode`（`0.1` 或 `1.0`）与 `(1 - activeReferralDiscount)`。
   - `meta` 请求失败或标的缺失时严格 fail-closed 返回 `None`，不再吞异常返回未缩放费率。
   - 当对冲腿为 `--hedge tradexyz`（HL HIP-3）时，`_fee_monitor_loop` 同步监控双腿实时费率并纳入 `effective_fee_bps`。

4. **消除双腿发单之间的同步 `fsync` 抖动，并优化 HL 成交费率查询**
   - `_execute_pair()` 与 `_hedge_attempts()` 在下单前通过**一次**原子紧凑 JSON 写入落盘双腿 `attempted` 状态，随后 `asyncio.gather` 并发发单，两腿之间零磁盘 `fsync` 阻塞。
   - `HLVenue.fetch_fill_fee()` 改为 `userFillsByTime`（默认查最近 5 分钟，大幅降低 API weight），并校验 `feeToken` 为美元稳定币；若发生单腿成交不平衡，费率核验自动异步后置，绝不拖延 `_maybe_hedge()` 对冲。

## 二、P1 级修复与二次审查加固

1. **Hyperliquid `HL_ACCOUNT_ADDRESS` 强制必填 + 启动期链上 Agent 授权校验（High）**：
   - `HLCreds.complete` 强制要求有效的 42 位 `0x` `HL_ACCOUNT_ADDRESS`，`HLAccount` 不再静默回退到 Agent 钱包地址。
   - 启动期 `HLVenue.verify_account()` 调用 Hyperliquid `userRole` 与 `extraAgents` 校验签名私钥确实是该 `HL_ACCOUNT_ADDRESS` 的未过期授权 Agent（或主钱包本人），否则拒绝启动，彻底杜绝查空 Agent 账户误平对冲腿的风险。
2. **小额残余敞口全平放行（Medium）**：
   - `_hedge_attempts()` 当 `reduce_only` 补单可一次性全平该交易所残余仓位时，豁免 `min_order_notional`/`min_quote`（Hyperliquid 与 Lighter 均对全平 `reduce_only` 碎单豁免 \$10 门槛），并且当仓位变化进入可平状态时立即解除 `_unhedgeable_until` 冷却。
3. **按交易所账户+品种维度加独占锁（Medium）**：
   - 实盘启动后对每个交易所的 `state_identity()`（如 `io:NVDA` + HL 主账户地址）额外加 `venue_<hash>.lock` 独占文件锁，阻止同品种同时跑 `--hedge lighter` 与 `--hedge lighter-rh` 互相把对方成交当仓位漂移而误对冲。
4. **未决 `not-found` IOC 过期自愈 + 人工备份式恢复工具（Low）**：
   - 新增 `execution.pending_not_found_expire_sec`（默认 `120.0` 秒，`0` = 关闭）：对连续 3 次返回 `unknownOid`/`not-found` 且超过过期窗口的 `known_filled=0` 未决 IOC，在**双边链上持仓严格吻合零成交预期**的前提下安全按零成交结案；若链上仓位不符则继续保持暂停。
   - 升级 `tools/inspect_order_state.py`，新增 `--resolve-zero-fill`、`--set-position`、`--clear-reconcile`（必须带 `--confirm`，修改前自动生成 `.bak.<ts>` 备份并原子写入）。
5. **配置数值范围全量校验（Low）**：补齐 `status_interval_sec > 0`、`reconcile_sec > 0`、`staleness_sec > 0`、`venue_probe_sec > 0`、`max_orders_per_min >= 1`、`max_consecutive_errors >= 1`、`net_tolerance_base >= 0`、`inventory.floor_frac ∈ [0, 1)`、`taker_fee_bps >= 0`、`pending_not_found_expire_sec >= 0` 等边界校验。
6. **释放 `latency_buffer_bps` 作为边界滑点空间**：正常入场的 `_worst_case_check()` / `_order_price_bounds()` 扣除 `latency_buffer_bps`，信号刚过门槛时两腿不再被压成 `0 bps` 贴盘口限价单。
7. **清淡盘口不阻断减仓与补单**：`max_book_age_sec` 仅拦截开新仓并驱动 watchdog 重连，不再拦截 `reduce_only` 平仓与 `_hedge()` 补单；主动重连不再刷屏误报 WS 断线 Telegram 告警；示例配置默认值上调为 `30.0s`。
8. **单腿补单优先级与分片计数优化**：`_evaluate()` 存在超容差裸敞口时优先执行 `_maybe_hedge()`（不受 `cooldown_sec` 阻挡）；`_hedge_attempt_count` 仅累计失败/零成交重试次数，大额敞口分多片正常成交不会误触停机。
9. **Lighter 模糊错误分类与 SDK 兼容**：`create_order` 返回 `502/503/504/timeout/connection` 类错误字符串时一律判为 `unresolved: True` 进入 P0-1 对账；超时取消后尝试通知 `nonce_manager`；`query_order` 遇到 `AttributeError`/`TypeError` 自动降级到 `account_inactive_orders`。
10. **HL Unified Account 可用保证金兼容 & 其他加固**：
    - `HLVenue.fetch_equity()` 自动探测 `userAbstraction`，对 `unifiedAccount` / `portfolioMargin` 从 `spotClearinghouseState` 读取 `total - hold`，避免 HIP-3 `withdrawable=0` 误锁死在 `REDUCE-ONLY`。
    - 双锁按固定键序加锁并防取消泄漏；拿锁后发单前复核交易所就绪、行情新鲜度与极端价差；链上仓位漂移采纳后立即落盘；未决单超过 15 秒且存在裸敞口时推送高优告警。

## 三、小资金实盘测试快速开始

项目新增了小资金实盘测试模板 `config.live-test.example.yaml`（仓位上限 \$30、单笔 \$15、最低可用保证金 \$20）。上线前请按以下步骤验证：

1. **先跑采集与校准**（默认不下单）：
   ```bash
   python3 main.py --symbol NVDA --hedge lighter --config config.live-test.example.yaml --record-only
   ```
   用 `tools/analyze.py` 计算并填入真实 `midline_bps`、`upper_bps`、`lower_bps`。
2. **确认交易所账户前提**：
   - Hyperliquid：确认目标 HIP-3 标的（如 `io:NVDA`）的逐仓/杠杆设置与可用保证金；
   - Lighter：建议在 `requirements-live.txt` 中锁定当前可正常工作的 `lighter-python` commit SHA。
3. **小资金启动实盘**：
   ```bash
   cp config.live-test.example.yaml config.live.yaml
   python3 main.py --live --symbol NVDA --hedge lighter --config config.live.yaml --cn
   ```
   完整走通 **开仓 -> 持仓 -> 正常回归平仓 -> 重启恢复** 闭环并核对两侧真实手续费/资金费后，再逐步放大仓位上限。
