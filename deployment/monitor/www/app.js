"use strict";

const $ = (id) => document.getElementById(id);
const canvas = $("chart");
const buttons = [...document.querySelectorAll("[data-hours]")];
const timeOfDay = new Intl.DateTimeFormat("zh-CN", { hour: "2-digit", minute: "2-digit", hour12: false });
const localTime = new Intl.DateTimeFormat("zh-CN", {
  month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
});
const money = (value) => Number.isFinite(value) ? `${value < 0 ? "−" : ""}$${Math.abs(value).toFixed(4)}` : "—";
const qty = (value) => Number.isFinite(value) ? value.toLocaleString("zh-CN", { maximumFractionDigits: 6 }) : "—";
const pos = (value) => Number.isFinite(value) && value > 0 ? "+" + qty(value) : qty(value);
const quote = (value) => Number.isFinite(value) ? value.toFixed(3) : "—";
const fmtTime = (ts, fmt = localTime) => Number.isFinite(ts) ? fmt.format(ts * 1000) : "—";
let hours = 6;
let requestId = 0;
let series = [];
let scale = null;

function setText(id, value, signal) {
  const el = $(id);
  el.textContent = value;
  el.dataset.sign = signal > 0 ? "positive" : signal < 0 ? "negative" : "";
}

function setMode(text, kind = "") {
  $("mode").querySelector("span").textContent = text;
  $("mode").dataset.kind = kind;
}

function showNotice(text, warning = false) {
  const el = $("notice");
  el.textContent = text;
  el.hidden = !text;
  el.classList.toggle("warning", warning);
}

function drawChart() {
  const rect = canvas.getBoundingClientRect();
  if (!rect.width || !rect.height) return;
  const ratio = window.devicePixelRatio || 1;
  const w = rect.width, h = rect.height;
  canvas.width = Math.floor(w * ratio);
  canvas.height = Math.floor(h * ratio);
  const ctx = canvas.getContext("2d");
  ctx.scale(ratio, ratio);
  ctx.font = '12px system-ui, sans-serif';
  if (series.length < 2) {
    ctx.textAlign = "center";
    ctx.fillStyle = "#a3b0c2";
    ctx.fillText("该时段暂无足够的双边报价", w / 2, h / 2);
    scale = null;
    return;
  }
  const left = 41, right = w - 14, top = 12, bottom = h - 27;
  const values = series.map((p) => p[1]);
  const min = Math.min(0, ...values), max = Math.max(0, ...values);
  const pad = Math.max(1, (max - min) * 0.12);
  const floor = min - pad, ceiling = max + pad;
  const start = series[0][0], end = series.at(-1)[0];
  const x = (t) => left + (t - start) / (end - start || 1) * (right - left);
  const y = (v) => bottom - (v - floor) / (ceiling - floor) * (bottom - top);
  scale = { start, end, left, right };
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (let i = 0; i <= 4; i++) {
    const v = floor + (ceiling - floor) * i / 4;
    const row = y(v);
    ctx.strokeStyle = "#29374a";
    ctx.beginPath(); ctx.moveTo(left, row); ctx.lineTo(right, row); ctx.stroke();
    ctx.fillStyle = "#a3b0c2";
    ctx.fillText(v.toFixed(1), left - 6, row);
  }
  if (floor < 0 && ceiling > 0) {
    ctx.setLineDash([4, 4]); ctx.strokeStyle = "#516278";
    ctx.beginPath(); ctx.moveTo(left, y(0)); ctx.lineTo(right, y(0)); ctx.stroke(); ctx.setLineDash([]);
  }
  ctx.strokeStyle = "#7be4ce";
  ctx.lineWidth = 1.75;
  ctx.lineJoin = "round";
  ctx.beginPath();
  series.forEach(([t, v], i) => {
    if (!i || t - series[i - 1][0] > 125) ctx.moveTo(x(t), y(v));
    else ctx.lineTo(x(t), y(v));
  });
  ctx.stroke();
  ctx.fillStyle = "#7be4ce";
  ctx.beginPath(); ctx.arc(x(end), y(series.at(-1)[1]), 3, 0, Math.PI * 2); ctx.fill();
  ctx.fillStyle = "#a3b0c2";
  ctx.textBaseline = "top";
  const ticks = w < 430 ? 2 : 4;
  for (let i = 0; i <= ticks; i++) {
    const t = start + (end - start) * i / ticks;
    ctx.textAlign = i === 0 ? "left" : i === ticks ? "right" : "center";
    ctx.fillText(fmtTime(t, timeOfDay), x(t), bottom + 8);
  }
}

function renderTrades(rows) {
  $("trades-empty").hidden = rows.length > 0;
  $("trade-wrap").hidden = rows.length === 0;
  const body = $("trade-body");
  body.replaceChildren(...rows.map((row) => {
    const tr = document.createElement("tr");
    for (const [text, details] of [
      [fmtTime(row.ts)], [row.side], [qty(row.qty)],
      [qty(row.buy_fill), `${row.buy} · ${row.buy_status}`],
      [qty(row.sell_fill), `${row.sell} · ${row.sell_status}`], [money(row.edge)],
    ]) {
      const cell = document.createElement("td");
      cell.textContent = text;
      if (details) {
        const subtitle = document.createElement("small");
        subtitle.textContent = details;
        cell.append(subtitle);
      }
      tr.append(cell);
    }
    return tr;
  }));
}

function render(data) {
  const p = data.portfolio, live = data.mode === "active", ticking = p.runtime;
  if (live) setMode(ticking ? "实盘运行中" : "实盘正在启动", ticking ? "active" : "warn");
  else if (data.mode === "failed") setMode("实盘启动失败", "warn");
  else if (data.mode === "activating") setMode("实盘正在启动", "warn");
  else if (data.collector === "active") setMode("仅采集行情");
  else setMode("机器人未运行", "warn");
  const notices = [];
  if (data.mode === "failed") notices.push("实盘服务异常退出，请检查 VPS 服务日志。");
  if (!live && data.mode !== "failed") notices.push("实盘尚未运行。当前展示行情，仓位和成交将在首次实盘后出现。");
  if (ticking?.halted) notices.push("机器人已停机，请在交易所核对仓位。");
  if (p.reconcile) notices.push("有订单需要对账，请核对双边仓位和成交。");
  if (live && !ticking) notices.push("等待实盘服务输出最新状态，暂无法确认下单风控。");
  if (ticking?.stale || ticking?.degraded) notices.push("机器人报告盘口过期或交易所连接异常。");
  if (data.quotes && data.now - data.quotes.ts > 150) notices.push("分钟行情更新已超过 2 分半。");
  showNotice(notices.join(" "), live && (!ticking || ticking.halted || ticking.stale || ticking.degraded) || Boolean(p.reconcile) || data.mode === "failed");
  setText("updated", "页面更新 " + fmtTime(data.now, timeOfDay));
  setText("daily", money(p.daily), p.daily);
  const a = p.positions.entropy, b = p.positions.hedge;
  setText("net", a !== null && b !== null ? pos(a + b) : "—");
  setText("mtm", money(ticking?.mtm), ticking?.mtm);
  setText("pending", qty(p.pending));
  setText("risk-hint", !live ? "等待实盘状态" : p.reconcile ? "暂停开仓，等待对账" : "仅统计待确认订单");
  setText("state-at", p.saved ? (live ? "上次状态落盘 " : "历史状态 · ") + fmtTime(p.saved) : "等待首次实盘状态");
  setText("entropy-pos", pos(a));
  setText("hedge-pos", pos(b));
  const safe = ticking ? (ticking.halted ? "已停机" : p.reconcile ? "等待对账" :
    ticking.reduce_only ? "只减仓" : ticking.stale || ticking.degraded ? "行情 / 连接异常" : "未触发限制") :
    p.loss_breaker ? "上次状态已熔断" : live ? "等待最新状态" : "实盘未运行";
  setText("risk-state", safe);
  setText("streak", qty(p.loss_streak));
  setText("actual", money(p.actual_fee));
  setText("estimate", money(p.estimated_fee));
  setText("funding", money(p.funding), p.funding);
  const q = data.quotes;
  setText("premium", q ? `${q.spread >= 0 ? "+" : ""}${q.spread.toFixed(2)} bps` : "—");
  setText("quote-at", q ? fmtTime(q.ts) + " · " + q.source : "");
  setText("entropy-quote", q ? `${quote(q.quotes[0])} / ${quote(q.quotes[1])}` : "—");
  setText("lighter-quote", q ? `${quote(q.quotes[2])} / ${quote(q.quotes[3])}` : "—");
  series = data.chart.filter(([timestamp]) => timestamp >= data.now - hours * 3600);
  drawChart();
  renderTrades(p.trades);
  $("events-panel").hidden = p.events.length === 0;
  $("events").replaceChildren(...p.events.map((event) => {
    const li = document.createElement("li");
    const at = document.createElement("time");
    at.textContent = event.at;
    li.append(at, document.createTextNode(event.label));
    return li;
  }));
}

async function refresh() {
  const id = ++requestId;
  try {
    const response = await fetch(`/api/overview?hours=${hours}`, { cache: "no-store" });
    if (!response.ok) throw new Error("HTTP " + response.status);
    const data = await response.json();
    if (Date.now() / 1000 - data.now > 35) throw new Error("stale server snapshot");
    if (id === requestId) render(data);
  } catch (error) {
    if (id !== requestId) return;
    setMode("连接中断", "warn");
    showNotice("暂时无法读取服务器。下方可能是上次成功刷新时的数据。", true);
  }
}

buttons.forEach((button) => button.addEventListener("click", () => {
  hours = Number(button.dataset.hours);
  for (const item of buttons) item.setAttribute("aria-pressed", String(item === button));
  refresh();
}));
canvas.addEventListener("pointermove", (event) => {
  if (!scale || !series.length) return;
  const rect = canvas.getBoundingClientRect();
  const pct = Math.min(1, Math.max(0, (event.clientX - rect.left - scale.left) / (scale.right - scale.left)));
  const t = scale.start + (scale.end - scale.start) * pct;
  const nearest = series.reduce((best, point) => Math.abs(point[0] - t) < Math.abs(best[0] - t) ? point : best);
  const marker = $("chart-point");
  marker.textContent = fmtTime(nearest[0]) + " · " + nearest[1].toFixed(2) + " bps";
  marker.hidden = false;
});
canvas.addEventListener("pointerleave", () => { $("chart-point").hidden = true; });
new ResizeObserver(drawChart).observe(canvas.parentElement);
document.addEventListener("visibilitychange", () => { if (!document.hidden) refresh(); });
setInterval(() => { if (!document.hidden) refresh(); }, 8000);
refresh();
