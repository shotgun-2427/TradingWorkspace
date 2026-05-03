import { api } from "../lib/api.js";
import { areaChart, barChart, lineChart } from "../lib/charts.js";
import {
  clear,
  el,
  fmtDollars,
  fmtInt,
  fmtNumber,
  fmtPct,
  kpi,
  makeTable,
  notice,
} from "../lib/dom.js";
import { getState, on, toast } from "../lib/state.js";

let pageNode = null;
let activeTab = "positions";
const compositionState = { history_start: null, history_end: null };
const attributionState = {
  source: "simulated",
  start: null,
  end: null,
  metric: "net_pnl",
  top_n: 10,
};

export async function renderAsset(node) {
  pageNode = node;
  build();
  await renderActiveTab();
}

on("profile", () => {
  if (pageNode && !pageNode.hidden) renderActiveTab();
});

function build() {
  clear(pageNode);
  pageNode.appendChild(
    el(
      "div",
      { class: "page-header" },
      el("h2", {}, "Asset Analysis"),
      el(
        "p",
        {},
        "Live IBKR positions, current portfolio composition, and contributors / detractors.",
      ),
    ),
  );

  const tabs = el("div", { class: "tabs" });
  for (const tab of [
    { id: "positions", label: "Live positions" },
    { id: "composition", label: "Portfolio composition" },
    { id: "attribution", label: "Attribution" },
    { id: "orders", label: "Orders calendar" },
  ]) {
    const btn = el("button", { type: "button", "data-tab": tab.id }, tab.label);
    btn.classList.toggle("active", tab.id === activeTab);
    btn.addEventListener("click", () => {
      activeTab = tab.id;
      tabs.querySelectorAll("button").forEach((b) => b.classList.toggle("active", b === btn));
      renderActiveTab();
    });
    tabs.appendChild(btn);
  }
  pageNode.appendChild(tabs);
  pageNode.appendChild(el("div", { id: "asset-tab-host" }));
}

async function renderActiveTab() {
  const host = document.getElementById("asset-tab-host");
  if (!host) return;
  clear(host);
  if (activeTab === "positions") return renderPositions(host);
  if (activeTab === "composition") return renderComposition(host);
  if (activeTab === "attribution") return renderAttribution(host);
  if (activeTab === "orders") return renderOrdersCalendar(host);
}

// ── Positions ──────────────────────────────────────────────────────────────

async function renderPositions(host) {
  const profile = getState().profile;
  let payload;
  try {
    payload = await api.get("/asset/positions", { profile });
  } catch (err) {
    host.appendChild(notice(`Could not load positions: ${err.message}`, "error"));
    return;
  }

  const refreshBox = el("div", { class: "section" });
  refreshBox.appendChild(
    el(
      "div",
      { class: "controls" },
      el(
        "div",
        { class: "field" },
        el("label", {}, "Profile"),
        el("p", { class: "muted" }, profile.toUpperCase()),
      ),
      buildRefreshButton(profile),
    ),
  );
  if (payload.snapshot_severity === "stale") {
    refreshBox.appendChild(
      notice(
        `Positions snapshot is stale (${payload.snapshot_timestamp}). Click Refresh to pull a fresh one before trusting the numbers.`,
        "error",
      ),
    );
  } else if (payload.snapshot_severity === "warn") {
    refreshBox.appendChild(
      notice(`Positions snapshot from ${payload.snapshot_timestamp} - older than 1 day.`, "warning"),
    );
  } else if (payload.snapshot_severity === "ok") {
    refreshBox.appendChild(
      notice(`Positions snapshot from ${payload.snapshot_timestamp}.`, "info"),
    );
  }
  host.appendChild(refreshBox);

  if (!payload.available) {
    host.appendChild(
      notice(payload.message || "No data.", "info"),
    );
    return;
  }

  const summary = payload.summary || {};
  host.appendChild(
    el(
      "div",
      { class: "section" },
      el(
        "div",
        { class: "kpi-row" },
        kpi("Open positions", fmtInt(summary.open_positions)),
        kpi("Long market value", fmtDollars(summary.long_market_value)),
        kpi("Short market value", fmtDollars(summary.short_market_value)),
        kpi("Net market value", fmtDollars(summary.net_market_value)),
        kpi("Unrealized PnL", fmtDollars(summary.unrealized_pnl_total), null,
          summary.unrealized_pnl_total >= 0 ? "positive" : "negative"),
      ),
      el(
        "p",
        { class: "muted", style: { marginTop: "8px" } },
        `Latest NAV: ${fmtDollars(summary.nav)}  -  Cash: ${fmtDollars(summary.cash)}  -  Gross MV: ${fmtDollars(summary.gross_market_value)}`,
      ),
    ),
  );

  // PnL chart + winners/losers
  const chartsRow = el("div", { class: "row-1-2" });
  const winnersBox = el("div", { class: "section" });
  winnersBox.appendChild(el("h3", {}, "Top / Bottom"));
  const winnersTable = makeTable(payload.winners || [], [
    { key: "symbol", label: "Best" },
    { key: "unrealized_pnl", label: "Unrealized PnL", format: (v) => fmtDollars(v) },
  ]);
  const losersTable = makeTable(payload.losers || [], [
    { key: "symbol", label: "Worst" },
    { key: "unrealized_pnl", label: "Unrealized PnL", format: (v) => fmtDollars(v) },
  ]);
  winnersBox.appendChild(winnersTable);
  winnersBox.appendChild(el("hr", { class: "divider" }));
  winnersBox.appendChild(losersTable);

  const pnlBox = el("div", { class: "section" });
  pnlBox.appendChild(el("h3", {}, "PnL by ETF"));
  const pnlHost = el("div", { class: "chart-host" });
  pnlBox.appendChild(pnlHost);

  chartsRow.appendChild(winnersBox);
  chartsRow.appendChild(pnlBox);
  host.appendChild(chartsRow);

  if (payload.pnl_chart?.length) {
    const labels = payload.pnl_chart.map((r) => r.symbol);
    const values = payload.pnl_chart.map((r) => r.unrealized_pnl);
    barChart(pnlHost, { labels, values, label: "Unrealized PnL" });
  }

  // Positions table
  const tableBox = el("div", { class: "section" });
  tableBox.appendChild(el("h3", {}, "Positions"));
  tableBox.appendChild(
    makeTable(payload.positions || [], [
      { key: "symbol", label: "Symbol" },
      { key: "shares", label: "Shares", format: (v) => fmtInt(v) },
      { key: "avg_price", label: "Avg price", format: (v) => fmtNumber(v, 2) },
      { key: "last_price", label: "Last price", format: (v) => fmtNumber(v, 2) },
      { key: "market_value", label: "Market value", format: (v) => fmtDollars(v) },
      {
        key: "unrealized_pnl",
        label: "Unrealized PnL",
        format: (v) => fmtDollars(v),
        cellClass: (v) => (v > 0 ? "positive" : v < 0 ? "negative" : ""),
      },
      { key: "realized_pnl", label: "Realized PnL", format: (v) => fmtDollars(v) },
      { key: "weight_pct", label: "Weight %", format: (v) => fmtNumber(v, 2) },
    ]),
  );
  tableBox.appendChild(
    el("p", { class: "muted", style: { marginTop: "10px" } }, `Snapshot file: ${payload.snapshot}`),
  );
  host.appendChild(tableBox);
}

function buildRefreshButton(profile) {
  const btn = el("button", { type: "button" }, "Refresh from IBKR");
  btn.style.maxWidth = "240px";
  btn.addEventListener("click", async () => {
    btn.disabled = true;
    btn.textContent = "Refreshing...";
    try {
      const r = await api.post("/asset/positions/refresh", null, { profile });
      if (r.ok) toast(`Refreshed ${r.rows} positions at ${r.timestamp}.`, "success");
      else toast(r.error || "Refresh failed", "error", 7000);
    } catch (err) {
      toast(err.message, "error", 7000);
    } finally {
      btn.disabled = false;
      btn.textContent = "Refresh from IBKR";
      renderActiveTab();
    }
  });
  return el("div", { class: "field" }, el("label", {}, "Live snapshot"), btn);
}

// ── Composition ────────────────────────────────────────────────────────────

async function renderComposition(host) {
  const profile = getState().profile;
  let payload;
  try {
    payload = await api.get("/asset/composition", { profile });
  } catch (err) {
    host.appendChild(notice(`Could not load composition: ${err.message}`, "error"));
    return;
  }

  if (!payload.available) {
    host.appendChild(notice(payload.message || payload.error || "No data.", "info"));
  } else {
    host.appendChild(
      el(
        "div",
        { class: "section" },
        el("h3", {}, "Latest portfolio composition"),
        el("p", { class: "muted" }, `Audit date: ${payload.audit_date || "-"}`),
        makeTable(payload.rows || [], [
          { key: "ticker", label: "Ticker" },
          { key: "full_name", label: "Name" },
          { key: "weight", label: "Weight", format: (v) => fmtPct(v, 1) },
          { key: "price", label: "Price", format: (v) => fmtNumber(v, 2) },
          { key: "target_value", label: "Target value", format: (v) => fmtDollars(v) },
          { key: "target_shares", label: "Target shares", format: (v) => fmtInt(v) },
          {
            key: "delta_shares",
            label: "Delta shares",
            format: (v) => fmtInt(v),
            cellClass: (v) => (v > 0 ? "positive" : v < 0 ? "negative" : ""),
          },
          {
            key: "delta_value",
            label: "Delta value",
            format: (v) => fmtDollars(v),
            cellClass: (v) => (v > 0 ? "positive" : v < 0 ? "negative" : ""),
          },
        ]),
      ),
    );
  }

  // History
  await renderCompositionHistory(host);
}

async function renderCompositionHistory(host) {
  const profile = getState().profile;
  let history;
  try {
    history = await api.get("/asset/composition/history", {
      profile,
      start: compositionState.history_start,
      end: compositionState.history_end,
    });
  } catch (err) {
    host.appendChild(notice(`Could not load history: ${err.message}`, "error"));
    return;
  }

  const box = el("div", { class: "section" });
  box.appendChild(el("h3", {}, "Historical portfolio composition"));

  if (!history.available) {
    box.appendChild(notice(history.message || "No historical data.", "info"));
    host.appendChild(box);
    return;
  }
  if (history.error) box.appendChild(notice(history.error, "error"));

  const startInput = el("input", {
    type: "date",
    value: compositionState.history_start || history.start || history.min_date,
    min: history.min_date || "",
    max: history.max_date || "",
  });
  const endInput = el("input", {
    type: "date",
    value: compositionState.history_end || history.end || history.max_date,
    min: history.min_date || "",
    max: history.max_date || "",
  });
  startInput.addEventListener("change", (e) => {
    compositionState.history_start = e.target.value || null;
    renderActiveTab();
  });
  endInput.addEventListener("change", (e) => {
    compositionState.history_end = e.target.value || null;
    renderActiveTab();
  });

  box.appendChild(
    el(
      "div",
      { class: "controls" },
      el("div", { class: "field" }, el("label", {}, "Start date"), startInput),
      el("div", { class: "field" }, el("label", {}, "End date"), endInput),
    ),
  );

  const chartHost = el("div", { class: "chart-host tall" });
  box.appendChild(chartHost);
  host.appendChild(box);

  const tickers = history.tickers || [];
  const series = tickers.map((t) => ({
    name: t,
    points: history.rows.map((r) => ({ date: r.date, value: r[t] || 0 })),
  }));
  if (series.length) {
    areaChart(chartHost, series, { stacked: true, yLabel: "Weight" });
  }
}

// ── Attribution ────────────────────────────────────────────────────────────

async function renderAttribution(host) {
  const profile = getState().profile;
  let payload;
  try {
    payload = await api.get("/asset/attribution", {
      profile,
      source: attributionState.source,
      start: attributionState.start,
      end: attributionState.end,
      metric: attributionState.metric,
      top_n: attributionState.top_n,
    });
  } catch (err) {
    host.appendChild(notice(`Could not load attribution: ${err.message}`, "error"));
    return;
  }

  // Source toggle + range + metric controls
  const controlsBox = el("div", { class: "section" });
  const sourceToggle = el("div", { class: "segmented" });
  for (const opt of [
    { value: "simulated", label: "Simulated" },
    { value: "actual", label: "Paper portfolio" },
  ]) {
    const btn = el("button", { type: "button", "data-source": opt.value }, opt.label);
    if (attributionState.source === opt.value) btn.classList.add("active");
    btn.addEventListener("click", () => {
      attributionState.source = opt.value;
      renderAttribution(host);
    });
    sourceToggle.appendChild(btn);
  }

  const startInput = el("input", { type: "date", value: attributionState.start || payload.range?.start || "" });
  const endInput = el("input", { type: "date", value: attributionState.end || payload.range?.end || "" });
  startInput.addEventListener("change", (e) => {
    attributionState.start = e.target.value || null;
    renderAttribution(host);
  });
  endInput.addEventListener("change", (e) => {
    attributionState.end = e.target.value || null;
    renderAttribution(host);
  });

  const metricSelect = el(
    "select",
    {},
    [
      { value: "net_pnl", label: "Net PnL ($)" },
      { value: "contribution_bps", label: "Contribution (bps)" },
    ].map(({ value, label }) => {
      const o = el("option", { value }, label);
      if (value === attributionState.metric) o.selected = true;
      return o;
    }),
  );
  metricSelect.addEventListener("change", () => {
    attributionState.metric = metricSelect.value;
    renderAttribution(host);
  });

  const topNInput = el("input", {
    type: "number",
    min: "1",
    max: "25",
    value: String(attributionState.top_n),
  });
  topNInput.addEventListener("change", (e) => {
    const n = parseInt(e.target.value || "10", 10);
    attributionState.top_n = Math.min(25, Math.max(1, n));
    renderAttribution(host);
  });

  controlsBox.appendChild(
    el(
      "div",
      { class: "controls" },
      el("div", { class: "field" }, el("label", {}, "Source"), sourceToggle),
      el("div", { class: "field" }, el("label", {}, "Start date"), startInput),
      el("div", { class: "field" }, el("label", {}, "End date"), endInput),
      el("div", { class: "field" }, el("label", {}, "Metric"), metricSelect),
      el("div", { class: "field" }, el("label", {}, "Top N"), topNInput),
    ),
  );
  host.appendChild(controlsBox);

  if (!payload.available) {
    host.appendChild(notice(payload.message || payload.error || "No data.", "info"));
    return;
  }

  if (payload.trades_missing) {
    host.appendChild(notice("Trade artifacts missing - blotter omitted.", "warning"));
  }

  const k = payload.kpis || {};
  host.appendChild(
    el(
      "div",
      { class: "section" },
      el(
        "div",
        { class: "kpi-row" },
        kpi("Total gross PnL", fmtDollars(k.gross_pnl)),
        kpi("Slippage + commission", fmtDollars(k.slippage_and_commission)),
        kpi("Total net PnL", fmtDollars(k.net_pnl)),
        kpi("Traded ticker count", fmtInt(k.traded_ticker_count)),
      ),
      el(
        "p",
        { class: "muted", style: { marginTop: "8px" } },
        `Audit ${payload.audit_date}; range ${payload.range?.start} -> ${payload.range?.end}`,
      ),
    ),
  );

  const row = el("div", { class: "row-2" });
  const contribBox = el("div", { class: "section" });
  contribBox.appendChild(el("h3", {}, "Top contributors and detractors"));
  const contribHost = el("div", { class: "chart-host tall" });
  contribBox.appendChild(contribHost);

  const cumulBox = el("div", { class: "section" });
  cumulBox.appendChild(el("h3", {}, "Cumulative metric"));
  const cumulHost = el("div", { class: "chart-host tall" });
  cumulBox.appendChild(cumulHost);

  row.appendChild(contribBox);
  row.appendChild(cumulBox);
  host.appendChild(row);

  if (payload.contributors_chart?.length) {
    barChart(contribHost, {
      labels: payload.contributors_chart.map((r) => r.ticker),
      values: payload.contributors_chart.map((r) => r.value),
      horizontal: true,
      label: payload.metric === "contribution_bps" ? "bps" : "$",
    });
  }

  if (payload.cumulative_chart?.length) {
    const groups = new Map();
    for (const r of payload.cumulative_chart) {
      if (!groups.has(r.series)) groups.set(r.series, []);
      groups.get(r.series).push({ date: r.date, value: r.value });
    }
    const series = Array.from(groups.entries()).map(([name, points]) => ({ name, points }));
    lineChart(cumulHost, series, {
      yLabel: payload.metric === "contribution_bps" ? "Cumulative bps" : "Cumulative $",
    });
  }

  // Summary table
  const sumBox = el("div", { class: "section" });
  sumBox.appendChild(el("h3", {}, "Contributors and detractors"));
  sumBox.appendChild(
    makeTable(payload.summary || [], [
      { key: "ticker", label: "Ticker" },
      { key: "gross_pnl", label: "Gross PnL", format: (v) => fmtDollars(v) },
      { key: "slippage_cost", label: "Slippage", format: (v) => fmtDollars(v) },
      { key: "commission_cost", label: "Commissions", format: (v) => fmtDollars(v) },
      { key: "net_pnl", label: "Net PnL", format: (v) => fmtDollars(v) },
      { key: "contribution_bps", label: "Contribution (bps)", format: (v) => fmtNumber(v, 2) },
      { key: "active_day_count", label: "Active days", format: (v) => fmtInt(v) },
      { key: "traded_day_count", label: "Traded days", format: (v) => fmtInt(v) },
    ]),
  );
  host.appendChild(sumBox);

  // Trade blotter
  if (!payload.trades_missing && payload.trade_blotter?.length) {
    const blotter = el("div", { class: "section" });
    blotter.appendChild(el("h3", {}, "Trade blotter"));
    blotter.appendChild(
      makeTable(payload.trade_blotter, [
        { key: "execution_date", label: "Exec date" },
        { key: "event_date", label: "Event date" },
        { key: "ticker", label: "Ticker" },
        { key: "trade_direction", label: "Side" },
        { key: "trade_notional", label: "Notional", format: (v) => fmtDollars(v) },
        { key: "pre_trade_weight", label: "Pre-weight", format: (v) => fmtPct(v, 2) },
        { key: "post_trade_weight", label: "Post-weight", format: (v) => fmtPct(v, 2) },
        { key: "slippage_cost", label: "Slippage", format: (v) => fmtDollars(v, { digits: 2 }) },
        { key: "commission_cost", label: "Commission", format: (v) => fmtDollars(v, { digits: 2 }) },
        { key: "cost_bps", label: "Cost (bps)", format: (v) => fmtNumber(v, 2) },
      ]),
    );
    host.appendChild(blotter);
  }
}

// ── Orders calendar ────────────────────────────────────────────────────────

const ordersState = {
  year: new Date().getFullYear(),
  month: new Date().getMonth() + 1, // 1..12
  selected: null,
};

async function renderOrdersCalendar(host) {
  const profile = getState().profile;
  let payload;
  try {
    payload = await api.get("/asset/orders/calendar", {
      profile,
      year: ordersState.year,
      month: ordersState.month,
    });
  } catch (err) {
    host.appendChild(notice(`Could not load orders calendar: ${err.message}`, "error"));
    return;
  }

  const totals = payload.totals || {};
  const summary = el(
    "div",
    { class: "section" },
    el(
      "div",
      { class: "kpi-row" },
      kpi("Total buys", fmtInt(totals.buy)),
      kpi("Total sells", fmtInt(totals.sell)),
      kpi("Days with orders", fmtInt(totals.days_with_orders)),
      kpi("Submission files", fmtInt(totals.submission_files)),
    ),
    el(
      "p",
      { class: "muted", style: { marginTop: "10px" } },
      payload.earliest && payload.latest
        ? `Submissions on disk span ${payload.earliest} -> ${payload.latest}.`
        : "No submitted-orders files on disk yet.",
    ),
  );
  host.appendChild(summary);

  // Header with prev/next navigation
  const header = el(
    "div",
    { class: "controls", style: { alignItems: "center" } },
    el(
      "div",
      { class: "field" },
      el("label", {}, "Month"),
      el("h3", { style: { margin: 0 } }, `${payload.month_name} ${payload.year}`),
    ),
    navButton("← Previous", () => stepMonth(-1)),
    navButton("Next →", () => stepMonth(1)),
    navButton("Today", () => {
      const now = new Date();
      ordersState.year = now.getFullYear();
      ordersState.month = now.getMonth() + 1;
      ordersState.selected = null;
      renderOrdersCalendar(host.parentNode ? document.getElementById("asset-tab-host") : host);
    }),
  );

  const calendarBox = el("div", { class: "section" });
  calendarBox.appendChild(header);
  calendarBox.appendChild(buildCalendarGrid(payload, host));
  host.appendChild(calendarBox);

  // Detail panel for the selected day
  const detailBox = el("div", { class: "section", id: "orders-day-detail" });
  host.appendChild(detailBox);
  if (ordersState.selected) {
    await renderOrdersDayDetail(detailBox, ordersState.selected);
  } else {
    detailBox.appendChild(
      el(
        "p",
        { class: "muted" },
        "Click a day with orders to see the buy / sell breakdown.",
      ),
    );
  }
}

function navButton(label, onClick) {
  const btn = el("button", { type: "button" }, label);
  btn.style.maxWidth = "150px";
  btn.addEventListener("click", onClick);
  return el("div", { class: "field" }, el("label", {}, " "), btn);
}

function stepMonth(delta) {
  let m = ordersState.month + delta;
  let y = ordersState.year;
  while (m < 1) {
    m += 12;
    y -= 1;
  }
  while (m > 12) {
    m -= 12;
    y += 1;
  }
  ordersState.year = y;
  ordersState.month = m;
  ordersState.selected = null;
  renderOrdersCalendar(document.getElementById("asset-tab-host"));
}

function buildCalendarGrid(payload, hostForRefresh) {
  const grid = el("div", { class: "orders-calendar" });

  // Weekday header row
  const headerRow = el("div", { class: "orders-calendar-row header" });
  for (const label of payload.weekday_labels || []) {
    headerRow.appendChild(el("div", { class: "orders-calendar-cell label" }, label));
  }
  grid.appendChild(headerRow);

  for (const week of payload.weeks || []) {
    const row = el("div", { class: "orders-calendar-row" });
    for (const cell of week) {
      const dayNum = String(parseInt(cell.date.slice(8), 10));
      const cellEl = el("div", {
        class: [
          "orders-calendar-cell",
          cell.in_month ? "" : "out-of-month",
          cell.is_today ? "today" : "",
          cell.total > 0 ? "has-orders" : "",
          ordersState.selected === cell.date ? "selected" : "",
        ]
          .filter(Boolean)
          .join(" "),
      });
      cellEl.appendChild(el("div", { class: "day-num" }, dayNum));
      if (cell.total > 0) {
        const stats = el("div", { class: "day-stats" });
        if (cell.buy > 0)
          stats.appendChild(el("span", { class: "buy" }, `${cell.buy} buy`));
        if (cell.sell > 0)
          stats.appendChild(el("span", { class: "sell" }, `${cell.sell} sell`));
        cellEl.appendChild(stats);
      }
      if (cell.total > 0) {
        cellEl.style.cursor = "pointer";
        cellEl.addEventListener("click", () => {
          ordersState.selected = cell.date;
          renderOrdersCalendar(document.getElementById("asset-tab-host"));
        });
      }
      row.appendChild(cellEl);
    }
    grid.appendChild(row);
  }
  return grid;
}

async function renderOrdersDayDetail(host, isoDate) {
  clear(host);
  host.appendChild(el("h3", {}, `Orders submitted on ${isoDate}`));
  let payload;
  try {
    payload = await api.get("/asset/orders/day", { day: isoDate });
  } catch (err) {
    host.appendChild(notice(`Could not load orders for ${isoDate}: ${err.message}`, "error"));
    return;
  }
  if (!payload.available || !payload.orders.length) {
    host.appendChild(notice(`No orders on ${isoDate}.`, "info"));
    return;
  }

  // Submission summary chips
  const subs = payload.submissions || [];
  if (subs.length) {
    host.appendChild(
      el(
        "p",
        { class: "muted" },
        `${subs.length} submission file(s): ${subs.map((s) => `${s.time} (${s.rows} rows)`).join(", ")}.`,
      ),
    );
  }

  const buys = payload.orders.filter((o) => String(o.side || "").toUpperCase() === "BUY");
  const sells = payload.orders.filter((o) => String(o.side || "").toUpperCase() === "SELL");
  host.appendChild(
    el(
      "div",
      { class: "kpi-row" },
      kpi("Buys", fmtInt(buys.length), null, "positive"),
      kpi("Sells", fmtInt(sells.length), null, "negative"),
      kpi("Total", fmtInt(payload.orders.length)),
    ),
  );

  const rows = payload.orders.map((o) => ({
    submission_time: o._submission_time,
    symbol: o.symbol,
    side: o.side,
    qty: o.qty,
    ref_price: o.ref_price,
    status: o.status,
    filled: o.filled,
    avg_fill_price: o.avgfillprice,
  }));
  host.appendChild(
    makeTable(rows, [
      { key: "submission_time", label: "Time" },
      { key: "symbol", label: "Symbol" },
      {
        key: "side",
        label: "Side",
        cellClass: (v) => (String(v).toUpperCase() === "BUY" ? "positive" : "negative"),
      },
      { key: "qty", label: "Qty", format: (v) => fmtInt(v) },
      { key: "ref_price", label: "Ref price", format: (v) => fmtNumber(v, 2) },
      { key: "status", label: "Status" },
      { key: "filled", label: "Filled", format: (v) => fmtInt(v) },
      { key: "avg_fill_price", label: "Avg fill", format: (v) => fmtNumber(v, 2) },
    ]),
  );
}
