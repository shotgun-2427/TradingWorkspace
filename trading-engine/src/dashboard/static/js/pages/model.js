import { api } from "../lib/api.js";
import { lineChart, areaChart } from "../lib/charts.js";
import {
  clear,
  el,
  fmtDollars,
  fmtInt,
  fmtNumber,
  fmtPct,
  humanize,
  kpi,
  makeTable,
  notice,
} from "../lib/dom.js";
import { getState, on } from "../lib/state.js";

let pageNode = null;
let activeTab = "engine";

const modelsState = {
  selected: [],
  preset: "Custom",
  start: null,
  end: null,
  show_aggregate_portfolio: false,
  show_spx: false,
  // Overlay buy-and-hold on the per-ETF chart so the user can see at a
  // glance whether the model adds value vs. just owning the ETF.
  show_buy_hold: true,
  log_scale: false,
  // Cascading ETF -> per-ETF model filter. null = portfolio-level view.
  etf: null,
};

const pricesState = {
  selected_symbols: [],
  start: null,
  end: null,
  normalize: "indexed",
};

// Models native to the trading engine. These run per-ETF; the portfolio's
// ensemble combines them. Used to populate the model dropdown when an ETF is
// picked in Engine Backtests. Time-series models (the second group) are
// designed to beat buy-and-hold on a single ticker; the cross-sectional
// signals in the first group score better at the portfolio level once an
// optimizer combines them.
const PER_ETF_MODELS = [
  // Time-series — should beat B&H on a single ticker.
  { id: "vol_target_trend", label: "Vol-Targeted Trend" },
  { id: "trend_filter", label: "Trend Filter (200d SMA)" },
  { id: "adaptive_trend", label: "Adaptive Trend (50/100/200)" },
  { id: "tsmom_ts", label: "Time-Series Momentum (12-1)" },
  { id: "buy_and_hold", label: "Buy & Hold (benchmark)" },
  // Cross-sectional — designed for the portfolio ensemble, kept here for
  // visibility on the per-ETF view.
  { id: "ensemble", label: "Ensemble (XSec, default)" },
  { id: "amma", label: "AMMA Trend" },
  { id: "amma_mirror", label: "AMMA (mirror)" },
  { id: "momentum", label: "Momentum (XSec)" },
  { id: "natr_mean_reversion", label: "NATR Mean Reversion" },
  { id: "inverse_momentum_mean_reversion", label: "Inverse Momentum MR" },
];

export async function renderModel(node) {
  pageNode = node;
  buildScaffold();
  await renderActiveTab();
}

on("profile", () => {
  if (pageNode && !pageNode.hidden) renderActiveTab();
});

function buildScaffold() {
  clear(pageNode);
  pageNode.appendChild(
    el(
      "div",
      { class: "page-header" },
      el("h2", {}, "Backtester"),
      el(
        "p",
        {},
        "Engine backtests, ETF prices, the live portfolio composition, and a slot for new models from Quant Research.",
      ),
    ),
  );
  pageNode.appendChild(renderHeaderMeta());

  const tabs = el("div", { class: "tabs" });
  for (const tab of [
    { id: "engine", label: "Engine backtests" },
    { id: "prices", label: "ETF prices" },
    { id: "composition", label: "Live composition" },
    { id: "qr", label: "Quant Research" },
  ]) {
    const btn = el("button", { type: "button", "data-tab": tab.id }, tab.label);
    btn.classList.toggle("active", tab.id === activeTab);
    btn.addEventListener("click", () => {
      activeTab = tab.id;
      tabs.querySelectorAll("button").forEach((x) =>
        x.classList.toggle("active", x === btn),
      );
      renderActiveTab();
    });
    tabs.appendChild(btn);
  }
  pageNode.appendChild(tabs);

  pageNode.appendChild(el("div", { id: "model-tab-host" }));
}

function renderHeaderMeta() {
  const host = el("div", { class: "section", id: "model-meta" });
  host.appendChild(el("p", { class: "muted" }, "Loading audit metadata..."));
  return host;
}

async function renderHeaderMetaContent() {
  const profile = getState().profile;
  const meta = await api.get("/model/meta", { profile });
  const host = document.getElementById("model-meta");
  if (!host) return;
  clear(host);
  host.appendChild(
    el(
      "div",
      { class: "kpi-row" },
      kpi(
        `Latest ${capitalize(profile)} production audit`,
        meta.latest_production_audit || "Unavailable",
      ),
      kpi(
        `Latest ${capitalize(profile)} simulations audit`,
        meta.latest_simulations_audit || "Unavailable",
      ),
      kpi("Active optimizer", meta.active_optimizer || "-"),
    ),
  );
  if (
    meta.latest_production_audit &&
    meta.latest_simulations_audit &&
    meta.latest_production_audit !== meta.latest_simulations_audit
  ) {
    host.appendChild(
      notice(
        "Production and simulations audits are on different dates. Marginal views align on overlapping dates only.",
        "info",
      ),
    );
  }
}

async function renderActiveTab() {
  await renderHeaderMetaContent();
  const host = document.getElementById("model-tab-host");
  if (!host) return;
  clear(host);
  // Legacy hash redirects to the new ids.
  if (activeTab === "models") activeTab = "engine";
  if (activeTab === "engine") return renderModelsTab(host);
  if (activeTab === "prices") return renderPricesTab(host);
  if (activeTab === "composition") return renderCompositionTab(host);
  if (activeTab === "qr") return renderQuantResearchTab(host);
}

// ── Models tab ─────────────────────────────────────────────────────────────

async function renderModelsTab(host) {
  const profile = getState().profile;
  const meta = await api.get("/model/meta", { profile });
  const portfolioModels = meta.available_options || [];
  const etfUniverse = meta.etf_universe || [];

  // No portfolio audit data AND no ETF master — there's truly nothing to show.
  if (!portfolioModels.length && !etfUniverse.length) {
    host.appendChild(
      notice(
        "No production audit data is available yet. Run the pipeline or drop a reconciliation file.",
        "info",
      ),
    );
    return;
  }

  // Controls
  const controlsBox = el("div", { class: "section" });
  host.appendChild(controlsBox);

  // ── ETF picker (cascades into the Models dropdown below) ──
  const etfSelect = el("select", {});
  etfSelect.appendChild(el("option", { value: "" }, "Portfolio-level (all)"));
  for (const sym of etfUniverse) {
    const o = el("option", { value: sym }, sym);
    if (modelsState.etf === sym) o.selected = true;
    etfSelect.appendChild(o);
  }
  etfSelect.addEventListener("change", () => {
    modelsState.etf = etfSelect.value || null;
    // Reset selected models when switching context — the option lists differ.
    modelsState.selected = [];
    renderActiveTab();
  });

  // Model options: portfolio audit when no ETF picked; per-ETF strategies otherwise.
  const modelOptions = modelsState.etf
    ? PER_ETF_MODELS.map((m) => ({ value: m.id, label: m.label }))
    : portfolioModels.map((m) => ({ value: m, label: humanize(m) }));

  const selector = el("select", {
    multiple: true,
    size: Math.min(8, Math.max(3, modelOptions.length)),
  });
  selector.style.minWidth = "260px";
  for (const opt of modelOptions) {
    const o = el("option", { value: opt.value }, opt.label);
    if (modelsState.selected.includes(opt.value)) o.selected = true;
    selector.appendChild(o);
  }
  selector.addEventListener("change", () => {
    modelsState.selected = Array.from(selector.selectedOptions).map((o) => o.value);
    refreshModelsChart();
  });

  const preset = el(
    "select",
    {},
    ["Custom", "3M", "6M", "1Y", "YTD"].map((p) => {
      const o = el("option", { value: p }, p);
      if (p === modelsState.preset) o.selected = true;
      return o;
    }),
  );
  preset.addEventListener("change", () => {
    modelsState.preset = preset.value;
    if (modelsState.preset !== "Custom") {
      modelsState.start = null;
      modelsState.end = null;
    }
    refreshModelsChart();
  });

  const startInput = el("input", {
    type: "date",
    value: modelsState.start || "",
  });
  const endInput = el("input", {
    type: "date",
    value: modelsState.end || "",
  });
  startInput.addEventListener("change", (e) => {
    modelsState.start = e.target.value || null;
    modelsState.preset = "Custom";
    preset.value = "Custom";
    refreshModelsChart();
  });
  endInput.addEventListener("change", (e) => {
    modelsState.end = e.target.value || null;
    modelsState.preset = "Custom";
    preset.value = "Custom";
    refreshModelsChart();
  });

  const aggCheck = checkbox(
    "Show aggregate portfolio backtest",
    "show_aggregate_portfolio",
    modelsState,
    refreshModelsChart,
  );
  const spxCheck = checkbox("Show S&P 500 equity curve", "show_spx", modelsState, refreshModelsChart);
  // Buy-and-hold overlay only makes sense on the per-ETF view.
  const bhCheck = checkbox(
    "Show buy & hold benchmark",
    "show_buy_hold",
    modelsState,
    refreshModelsChart,
  );
  const logCheck = checkbox("Log scale (base 2)", "log_scale", modelsState, refreshModelsChart);

  controlsBox.appendChild(
    el(
      "div",
      { class: "controls" },
      el("div", { class: "field" }, el("label", {}, "ETF"), etfSelect),
      el(
        "div",
        { class: "field", style: { minWidth: "260px" } },
        el("label", {}, modelsState.etf ? `Models for ${modelsState.etf}` : "Models"),
        selector,
      ),
      el("div", { class: "field" }, el("label", {}, "Preset"), preset),
      el("div", { class: "field" }, el("label", {}, "Start date"), startInput),
      el("div", { class: "field" }, el("label", {}, "End date"), endInput),
      modelsState.etf ? el("div", { class: "field" }, bhCheck) : el("div", { class: "field" }, aggCheck),
      modelsState.etf ? null : el("div", { class: "field" }, spxCheck),
      el("div", { class: "field" }, logCheck),
    ),
  );

  const chartBox = el("div", { class: "section", id: "model-chart-box" });
  host.appendChild(chartBox);

  const metricsBox = el("div", { class: "section", id: "model-metrics-box" });
  host.appendChild(metricsBox);

  await refreshModelsChart();
}

async function refreshModelsChart() {
  // Per-ETF cascade: short-circuit to an indexed-price chart for just that ETF.
  // The per-ETF backtest pipeline isn't producing artifacts yet, so this is the
  // most useful thing we can show right now.
  if (modelsState.etf) {
    return refreshPerEtfChart();
  }

  const profile = getState().profile;
  const params = {
    profile,
    selected: modelsState.selected,
    show_aggregate_portfolio: modelsState.show_aggregate_portfolio,
    show_spx: modelsState.show_spx,
  };
  if (modelsState.preset && modelsState.preset !== "Custom") {
    params.preset = modelsState.preset;
  } else {
    params.start = modelsState.start;
    params.end = modelsState.end;
  }

  let payload;
  try {
    payload = await api.get("/model/models", params);
  } catch (err) {
    const box = document.getElementById("model-chart-box");
    clear(box);
    box.appendChild(notice(`Could not load models: ${err.message}`, "error"));
    return;
  }

  const chartBox = document.getElementById("model-chart-box");
  clear(chartBox);
  chartBox.appendChild(el("h3", {}, "Equity curves"));

  if (payload?.error) {
    chartBox.appendChild(notice(payload.error, "error"));
    return;
  }
  if (!payload?.series?.length) {
    if (!modelsState.selected.length) {
      chartBox.appendChild(
        notice("Pick one or more models above to plot equity curves.", "info"),
      );
      document.getElementById("model-metrics-box").innerHTML = "";
      return;
    }
    // No per-model artifacts present - fall back to the ETF price chart on the
    // same window (mirrors the old Streamlit fallback view).
    chartBox.appendChild(
      notice(
        "No per-model backtest artifacts found. Showing indexed ETF prices for the same window instead.",
        "warning",
      ),
    );
    let pricesPayload;
    try {
      pricesPayload = await api.get("/model/prices", {
        start: modelsState.start || payload.range?.start,
        end: modelsState.end || payload.range?.end,
        normalize: "indexed",
      });
    } catch (err) {
      chartBox.appendChild(notice(`Could not load fallback prices: ${err.message}`, "error"));
      return;
    }
    if (pricesPayload?.series?.length) {
      const fallbackHost = el("div", { class: "chart-host" });
      chartBox.appendChild(fallbackHost);
      lineChart(fallbackHost, pricesPayload.series, { yLabel: "Indexed to 100" });
    }
    document.getElementById("model-metrics-box").innerHTML = "";
    return;
  }

  const useLog = modelsState.log_scale;
  const chartHost = el("div", { class: "chart-host" });
  chartBox.appendChild(chartHost);
  lineChart(chartHost, payload.series, { logScale: useLog, yLabel: "Equity" });

  // Metrics
  const metricsBox = document.getElementById("model-metrics-box");
  clear(metricsBox);
  metricsBox.appendChild(el("h3", {}, "Performance metrics"));
  metricsBox.appendChild(el("p", { class: "muted" }, "For the selected date range"));

  const rows = Object.entries(payload.metrics || {})
    .filter(([_k, v]) => v && !v.error)
    .map(([model, m]) => ({
      model: humanize(model),
      total_return: m.total_return,
      annualized_return: m.annualized_return,
      annualized_volatility: m.annualized_volatility,
      sharpe_ratio: m.sharpe_ratio,
      sortino_ratio: m.sortino_ratio,
      max_drawdown: m.max_drawdown,
      avg_drawdown: m.avg_drawdown,
      win_rate: m.win_rate,
      avg_daily_return: m.avg_daily_return,
    }));
  if (!rows.length) return;
  metricsBox.appendChild(
    makeTable(rows, [
      { key: "model", label: "Model" },
      { key: "total_return", label: "Total return", format: (v) => fmtPct(v) },
      { key: "annualized_return", label: "Annualized return", format: (v) => fmtPct(v) },
      { key: "annualized_volatility", label: "Annualized vol", format: (v) => fmtPct(v) },
      { key: "sharpe_ratio", label: "Sharpe", format: (v) => fmtNumber(v, 2) },
      { key: "sortino_ratio", label: "Sortino", format: (v) => fmtNumber(v, 2) },
      { key: "max_drawdown", label: "Max DD", format: (v) => fmtPct(v) },
      { key: "avg_drawdown", label: "Avg DD", format: (v) => fmtPct(v) },
      { key: "win_rate", label: "Win rate", format: (v) => fmtPct(v) },
      { key: "avg_daily_return", label: "Avg daily ret", format: (v) => fmtPct(v, 3) },
    ]),
  );
}

async function refreshPerEtfChart() {
  const chartBox = document.getElementById("model-chart-box");
  const metricsBox = document.getElementById("model-metrics-box");
  if (!chartBox || !metricsBox) return;
  clear(chartBox);
  clear(metricsBox);

  // Try the real per-ETF backtest artifact first. Falls back to indexed
  // price only when nothing's been produced for this (ticker, model) pair.
  const selectedModelId = (modelsState.selected || [])[0] || null;
  if (selectedModelId) {
    let bt;
    try {
      bt = await api.get("/model/per-etf-backtest", {
        ticker: modelsState.etf,
        model: selectedModelId,
        include_buy_hold: modelsState.show_buy_hold,
      });
    } catch (_e) {
      bt = null;
    }
    if (bt && bt.available && bt.series?.length) {
      return paintPerEtfBacktest(chartBox, metricsBox, bt);
    }
  }

  // Fallback: indexed price for the selected window (legacy behavior).
  chartBox.appendChild(el("h3", {}, `${modelsState.etf} indexed price`));
  chartBox.appendChild(
    notice(
      selectedModelId
        ? `No per-ETF artifact for (${modelsState.etf}, ${selectedModelId}). Showing indexed price instead — run \`python -m src.research.per_etf_backtest\` to produce real backtest curves.`
        : "Pick one or more models above to plot the model's equity curve on this ETF.",
      "info",
    ),
  );
  let pricesPayload;
  try {
    pricesPayload = await api.get("/model/prices", {
      symbols: [modelsState.etf],
      start: modelsState.start,
      end: modelsState.end,
      normalize: "indexed",
    });
  } catch (err) {
    chartBox.appendChild(notice(`Could not load price series: ${err.message}`, "error"));
    return;
  }
  if (!pricesPayload?.series?.length) {
    chartBox.appendChild(notice("No price data for this ETF.", "info"));
    return;
  }
  const chartHost = el("div", { class: "chart-host" });
  chartBox.appendChild(chartHost);
  lineChart(chartHost, pricesPayload.series, { yLabel: "Indexed to 100" });
}

function paintPerEtfBacktest(chartBox, metricsBox, payload) {
  const s = payload.summary || {};
  const bh = payload.buy_hold_summary || {};
  chartBox.appendChild(
    el(
      "h3",
      {},
      `${payload.ticker} · ${payload.model} — backtest equity`,
    ),
  );
  chartBox.appendChild(
    el(
      "p",
      { class: "muted" },
      `${payload.range?.start || "?"} → ${payload.range?.end || "?"}. Long-only, monthly rebalance, no slippage/commissions (research view).`,
    ),
  );
  const chartHost = el("div", { class: "chart-host" });
  chartBox.appendChild(chartHost);

  // Overlay buy-and-hold as a second series when present + opt-in. The
  // server sends ``buy_hold_points`` aligned to the model's window so the
  // two curves start at 1.0 and are directly comparable.
  const series = [...payload.series];
  if (modelsState.show_buy_hold && payload.buy_hold_points?.length) {
    series.push({
      name: `${payload.ticker} · buy & hold`,
      kind: "buy_hold",
      points: payload.buy_hold_points,
    });
  }
  lineChart(chartHost, series, { yLabel: "Equity (1.0 = start)", logScale: !!modelsState.log_scale });

  // Drawdown panel below the equity curve so the user can read both at once.
  if (payload.drawdown_points?.length) {
    const ddBox = el("div", { class: "section" });
    ddBox.appendChild(el("h3", {}, "Drawdown"));
    const ddHost = el("div", { class: "chart-host" });
    ddBox.appendChild(ddHost);
    chartBox.appendChild(ddBox);
    areaChart(ddHost, [{ name: "Drawdown", points: payload.drawdown_points }], {
      yLabel: "Drawdown",
    });
  }

  // Summary KPIs in the metrics box. When B&H is present we render a
  // side-by-side comparison so the user can eyeball "did this model add
  // value over just owning the ETF?" without staring at the curves.
  metricsBox.appendChild(el("h3", {}, "Performance"));
  metricsBox.appendChild(
    el(
      "div",
      { class: "kpi-row" },
      kpi(
        "Sharpe",
        s.sharpe_ratio == null ? "-" : fmtNumber(s.sharpe_ratio, 2),
        bh.sharpe_ratio == null ? "rf=0" : `B&H: ${fmtNumber(bh.sharpe_ratio, 2)}`,
        s.sharpe_ratio == null ? null : s.sharpe_ratio >= 0 ? "positive" : "negative",
      ),
      kpi(
        "Total return",
        s.total_return == null ? "-" : fmtPct(s.total_return),
        bh.total_return == null ? null : `B&H: ${fmtPct(bh.total_return)}`,
        s.total_return == null ? null : s.total_return >= 0 ? "positive" : "negative",
      ),
      kpi(
        "Annualized",
        s.annualized_return == null ? "-" : fmtPct(s.annualized_return),
        bh.annualized_return == null ? null : `B&H: ${fmtPct(bh.annualized_return)}`,
      ),
      kpi(
        "Max drawdown",
        s.max_drawdown == null ? "-" : fmtPct(s.max_drawdown),
        bh.max_drawdown == null ? null : `B&H: ${fmtPct(bh.max_drawdown)}`,
        "negative",
      ),
      kpi(
        "Signals",
        s.num_signals == null ? "-" : fmtInt(s.num_signals),
      ),
      kpi(
        "Years observed",
        s.years_observed == null ? "-" : fmtNumber(s.years_observed, 1),
      ),
    ),
  );
}

// ── Composition sub-tab (moved here from Asset Analysis) ───────────────────

async function renderCompositionTab(host) {
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
    return;
  }
  host.appendChild(
    el(
      "div",
      { class: "section" },
      el("h3", {}, "Latest portfolio composition"),
      el("p", { class: "muted" }, `Audit date: ${payload.audit_date || "-"}`),
      makeTable(payload.rows || [], [
        { key: "ticker", label: "Ticker" },
        {
          key: "unrealized_pnl",
          label: "Unrealized PnL",
          format: (v) => fmtDollars(v),
          cellClass: (v) => (v > 0 ? "positive" : v < 0 ? "negative" : ""),
        },
        { key: "weight", label: "Composition", format: (v) => fmtPct(v, 1) },
        { key: "target_shares", label: "Target shares", format: (v) => fmtInt(v) },
      ]),
    ),
  );
}

// ── Quant Research sub-tab ─────────────────────────────────────────────────
//
// Tab state. Persisted across re-renders so filters survive a tab switch.
const qrState = {
  runs: [],                  // cached run list from /api/qr/runs
  selected: new Set(),       // run_ids currently selected for the equity overlay
  filter_ticker: "",         // empty = all tickers
  filter_model: "",          // case-insensitive substring match on model_name
  sort_key: "sharpe",        // sharpe | total_return | max_dd | trades | created
  sort_desc: true,
  equity_cache: new Map(),   // run_id -> array of equity points (avoid refetching)
};

const _QR_SORT_KEYS = {
  sharpe: (r) => _qrSummary(r).sharpe_ratio,
  total_return: (r) => _qrSummary(r).total_return,
  max_dd: (r) => _qrSummary(r).max_drawdown,
  annualized: (r) => _qrSummary(r).annualized_return,
  trades: (r) => _qrSummary(r).num_trades,
  created: (r) => r.created_at || "",
};

function _qrSummary(run) {
  return run.summary || {};
}

function _qrShortRunId(run_id) {
  // The full run_id is too long for a table cell. Keep the leading
  // "MODEL_DATE-DATE" chunk and the trailing 4 chars of the hash.
  if (!run_id || run_id.length < 30) return run_id || "";
  const parts = run_id.split("_");
  if (parts.length < 3) return run_id;
  // strip the timestamp tail "20260507T013149Z"
  const head = parts.slice(0, -1).join("_");
  return head.length > 60 ? head.slice(0, 57) + "…" : head;
}

async function renderQuantResearchTab(host) {
  let status;
  try {
    status = await api.get("/qr/status");
  } catch (_e) {
    status = { running: false };
  }

  // ── Header bar ────────────────────────────────────────────────────
  const headerBox = el("div", { class: "section" });
  const headerRow = el("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "12px" } });
  const headerLeft = el(
    "div",
    {},
    el("h3", { style: { margin: 0 } }, "Quant Research"),
    el(
      "p",
      { class: "muted", style: { margin: "2px 0 0" } },
      "Browse research runs from your quant-research/ workspace. Select multiple runs to overlay their equity curves.",
    ),
  );
  const dot = el("span", { class: ["dot", status?.running ? "dot-on" : "dot-off"].join(" ") });
  const headerRight = el(
    "p",
    { class: "meta", style: { margin: 0, whiteSpace: "nowrap" } },
    dot,
    ` API ${status?.running ? `:${status.port || "8000"}` : "offline"}`,
  );
  headerRow.appendChild(headerLeft);
  headerRow.appendChild(headerRight);
  headerBox.appendChild(headerRow);

  if (!status?.running) {
    headerBox.appendChild(
      notice(
        "Quant Research FastAPI isn't running. Restart the dashboard with QR_AUTO_START=1, or run `uvicorn api.main:app --reload --port 8000` from quant-research/.",
        "warning",
      ),
    );
    host.appendChild(headerBox);
    return;
  }
  host.appendChild(headerBox);

  // ── Load runs (cached) ────────────────────────────────────────────
  if (!qrState.runs.length) {
    try {
      const runs = await api.get("/qr/runs", { limit: 200 });
      qrState.runs = Array.isArray(runs) ? runs : (runs?.runs || []);
    } catch (err) {
      host.appendChild(notice(`Could not load runs: ${err.message}`, "error"));
      return;
    }
  }

  if (!qrState.runs.length) {
    host.appendChild(
      notice(
        "No runs found. Create one in quant-research/ and it will appear here.",
        "info",
      ),
    );
    return;
  }

  // ── Summary KPIs ──────────────────────────────────────────────────
  const summaryBox = el("div", { class: "section" });
  const distinctTickers = new Set();
  const distinctModels = new Set();
  let latestDate = "";
  let bestSharpe = -Infinity;
  let bestSharpeRun = null;
  for (const r of qrState.runs) {
    const s = _qrSummary(r);
    if (r.primary_ticker) distinctTickers.add(r.primary_ticker);
    if (r.model_name) distinctModels.add(r.model_name);
    if (r.created_at && r.created_at > latestDate) latestDate = r.created_at;
    if (typeof s.sharpe_ratio === "number" && s.sharpe_ratio > bestSharpe) {
      bestSharpe = s.sharpe_ratio;
      bestSharpeRun = r;
    }
  }
  summaryBox.appendChild(
    el(
      "div",
      { class: "kpi-row" },
      kpi("Runs", fmtInt(qrState.runs.length)),
      kpi("Models", fmtInt(distinctModels.size)),
      kpi("Tickers", fmtInt(distinctTickers.size)),
      kpi(
        "Best Sharpe",
        bestSharpeRun ? fmtNumber(bestSharpe, 2) : "-",
        bestSharpeRun ? bestSharpeRun.model_name : null,
        bestSharpe > 0 ? "positive" : null,
      ),
      kpi("Latest run", latestDate ? latestDate.slice(0, 10) : "-"),
    ),
  );
  host.appendChild(summaryBox);

  // ── Filters ───────────────────────────────────────────────────────
  const filtersBox = el("div", { class: "section" });

  const tickerOptions = ["", ...Array.from(distinctTickers).sort()];
  const tickerSelect = el(
    "select",
    {},
    tickerOptions.map((t) => {
      const o = el("option", { value: t }, t || "All tickers");
      if (t === qrState.filter_ticker) o.selected = true;
      return o;
    }),
  );
  tickerSelect.addEventListener("change", () => {
    qrState.filter_ticker = tickerSelect.value;
    renderActiveTab();
  });

  const modelInput = el("input", {
    type: "text",
    placeholder: "Filter model name…",
    value: qrState.filter_model,
  });
  modelInput.addEventListener("input", () => {
    qrState.filter_model = modelInput.value;
    rerenderQrTable();
  });

  const sortSelect = el(
    "select",
    {},
    [
      ["sharpe", "Sharpe"],
      ["total_return", "Total return"],
      ["annualized", "Annualized return"],
      ["max_dd", "Max drawdown"],
      ["trades", "# Trades"],
      ["created", "Recency"],
    ].map(([v, label]) => {
      const o = el("option", { value: v }, label);
      if (v === qrState.sort_key) o.selected = true;
      return o;
    }),
  );
  sortSelect.addEventListener("change", () => {
    qrState.sort_key = sortSelect.value;
    rerenderQrTable();
  });

  const orderBtn = el("button", { type: "button" }, qrState.sort_desc ? "↓ Descending" : "↑ Ascending");
  orderBtn.style.maxWidth = "150px";
  orderBtn.addEventListener("click", () => {
    qrState.sort_desc = !qrState.sort_desc;
    orderBtn.textContent = qrState.sort_desc ? "↓ Descending" : "↑ Ascending";
    rerenderQrTable();
  });

  const clearBtn = el("button", { type: "button", class: "ghost" }, "Clear selection");
  clearBtn.style.maxWidth = "150px";
  clearBtn.addEventListener("click", () => {
    qrState.selected.clear();
    rerenderQrTable();
    refreshQrDetail();
  });

  filtersBox.appendChild(
    el(
      "div",
      { class: "controls" },
      el("div", { class: "field" }, el("label", {}, "Ticker"), tickerSelect),
      el("div", { class: "field" }, el("label", {}, "Model"), modelInput),
      el("div", { class: "field" }, el("label", {}, "Sort by"), sortSelect),
      el("div", { class: "field" }, el("label", {}, "Order"), orderBtn),
      el("div", { class: "field" }, el("label", {}, "Selection"), clearBtn),
    ),
  );
  host.appendChild(filtersBox);

  // ── Runs table + detail panel ─────────────────────────────────────
  const runsBox = el("div", { class: "section", id: "qr-runs-box" });
  host.appendChild(runsBox);

  const detailBox = el("div", { class: "section", id: "qr-run-detail" });
  host.appendChild(detailBox);

  rerenderQrTable();
  refreshQrDetail();
}

function _filteredSortedRuns() {
  const ticker = qrState.filter_ticker;
  const modelQ = qrState.filter_model.trim().toLowerCase();
  const keyFn = _QR_SORT_KEYS[qrState.sort_key] || _QR_SORT_KEYS.sharpe;

  let rows = qrState.runs.slice();
  if (ticker) rows = rows.filter((r) => r.primary_ticker === ticker);
  if (modelQ) {
    rows = rows.filter((r) =>
      String(r.model_name || "").toLowerCase().includes(modelQ),
    );
  }
  rows.sort((a, b) => {
    const va = keyFn(a);
    const vb = keyFn(b);
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    if (va < vb) return qrState.sort_desc ? 1 : -1;
    if (va > vb) return qrState.sort_desc ? -1 : 1;
    return 0;
  });
  return rows;
}

function rerenderQrTable() {
  const host = document.getElementById("qr-runs-box");
  if (!host) return;
  const rows = _filteredSortedRuns();
  clear(host);
  host.appendChild(
    el(
      "h3",
      {},
      `Runs · ${rows.length} match${rows.length === 1 ? "" : "es"}${qrState.selected.size ? ` · ${qrState.selected.size} selected` : ""}`,
    ),
  );

  if (!rows.length) {
    host.appendChild(notice("No runs match the current filters.", "info"));
    return;
  }

  const columns = [
    {
      key: "_select",
      label: "",
      render: (_v, row) => {
        const cb = el("input", { type: "checkbox" });
        cb.checked = qrState.selected.has(row.run_id);
        cb.addEventListener("change", () => {
          if (cb.checked) qrState.selected.add(row.run_id);
          else qrState.selected.delete(row.run_id);
          refreshQrDetail();
        });
        return cb;
      },
    },
    {
      key: "run_id",
      label: "Run",
      render: (_v, row) =>
        el(
          "span",
          { title: row.run_id, style: { fontFamily: "ui-monospace, monospace", fontSize: "12px" } },
          _qrShortRunId(row.run_id),
        ),
    },
    { key: "primary_ticker", label: "Ticker" },
    {
      key: "model_name",
      label: "Model",
      render: (v) => el("span", { style: { fontFamily: "ui-monospace, monospace", fontSize: "12px" } }, v || "-"),
    },
    {
      key: "_period",
      label: "Period",
      render: (_v, row) => `${row.start || "?"} → ${row.end || "?"}`,
    },
    {
      key: "_sharpe",
      label: "Sharpe",
      render: (_v, row) => {
        const v = _qrSummary(row).sharpe_ratio;
        return v == null ? "-" : fmtNumber(v, 2);
      },
      cellClass: (_v, row) => {
        const v = _qrSummary(row).sharpe_ratio;
        return v == null ? "" : v >= 0 ? "positive" : "negative";
      },
    },
    {
      key: "_total_return",
      label: "Total return",
      render: (_v, row) => {
        const v = _qrSummary(row).total_return;
        return v == null ? "-" : fmtPct(v);
      },
      cellClass: (_v, row) => {
        const v = _qrSummary(row).total_return;
        return v == null ? "" : v >= 0 ? "positive" : "negative";
      },
    },
    {
      key: "_max_dd",
      label: "Max DD",
      render: (_v, row) => {
        const v = _qrSummary(row).max_drawdown;
        return v == null ? "-" : fmtPct(v);
      },
      cellClass: () => "negative",
    },
    {
      key: "_trades",
      label: "Trades",
      render: (_v, row) => {
        const v = _qrSummary(row).num_trades;
        return v == null ? "-" : fmtInt(v);
      },
    },
  ];

  host.appendChild(makeTable(rows, columns));
}

async function refreshQrDetail() {
  const host = document.getElementById("qr-run-detail");
  if (!host) return;
  clear(host);

  if (qrState.selected.size === 0) {
    host.appendChild(el("h3", {}, "Detail"));
    host.appendChild(
      el(
        "p",
        { class: "muted" },
        "Select one or more runs above to inspect. One = full summary + equity + drawdown. Many = overlay of normalized equity curves.",
      ),
    );
    return;
  }

  if (qrState.selected.size === 1) {
    const runId = Array.from(qrState.selected)[0];
    const run = qrState.runs.find((r) => r.run_id === runId);
    await renderQrSingleRun(host, run);
    return;
  }

  await renderQrOverlay(host, Array.from(qrState.selected));
}

async function _loadQrEquity(runId) {
  if (qrState.equity_cache.has(runId)) return qrState.equity_cache.get(runId);
  const data = await api.get(`/qr/runs/${encodeURIComponent(runId)}/equity`);
  qrState.equity_cache.set(runId, data);
  return data;
}

async function renderQrSingleRun(host, run) {
  if (!run) {
    host.appendChild(notice("Selected run not found in cache.", "error"));
    return;
  }
  const s = _qrSummary(run);

  host.appendChild(el("h3", {}, run.model_name || "Run detail"));
  host.appendChild(
    el(
      "p",
      { class: "muted", style: { fontFamily: "ui-monospace, monospace", fontSize: "11px" } },
      run.run_id,
    ),
  );
  host.appendChild(
    el(
      "div",
      { class: "kpi-row" },
      kpi("Sharpe", s.sharpe_ratio == null ? "-" : fmtNumber(s.sharpe_ratio, 2), null,
         s.sharpe_ratio == null ? null : s.sharpe_ratio >= 0 ? "positive" : "negative"),
      kpi("Total return", s.total_return == null ? "-" : fmtPct(s.total_return), null,
         s.total_return == null ? null : s.total_return >= 0 ? "positive" : "negative"),
      kpi("Annualized", s.annualized_return == null ? "-" : fmtPct(s.annualized_return)),
      kpi("Max drawdown", s.max_drawdown == null ? "-" : fmtPct(s.max_drawdown), null, "negative"),
      kpi("Trades", s.num_trades == null ? "-" : fmtInt(s.num_trades)),
      kpi("Period", `${run.start || "?"} → ${run.end || "?"}`),
    ),
  );

  let points;
  try {
    points = await _loadQrEquity(run.run_id);
  } catch (err) {
    host.appendChild(notice(`Could not load equity: ${err.message}`, "error"));
    return;
  }
  if (!Array.isArray(points) || !points.length) {
    host.appendChild(notice("No equity points returned for this run.", "info"));
    return;
  }

  const equitySeries = [
    {
      name: "Portfolio",
      points: points
        .map((p) => ({
          date: p.date,
          value: typeof p.portfolio_value === "number" ? p.portfolio_value : p.value,
        }))
        .filter((p) => p.date && Number.isFinite(p.value)),
    },
  ];
  const ddSeries = [
    {
      name: "Drawdown",
      points: points
        .map((p) => ({ date: p.date, value: typeof p.drawdown === "number" ? p.drawdown : null }))
        .filter((p) => p.date && Number.isFinite(p.value)),
    },
  ];

  const eqBox = el("div", { class: "section" });
  eqBox.appendChild(el("h3", {}, "Equity curve"));
  const eqHost = el("div", { class: "chart-host" });
  eqBox.appendChild(eqHost);
  host.appendChild(eqBox);
  lineChart(eqHost, equitySeries, { yLabel: "Portfolio value" });

  if (ddSeries[0].points.length) {
    const ddBox = el("div", { class: "section" });
    ddBox.appendChild(el("h3", {}, "Drawdown"));
    const ddHost = el("div", { class: "chart-host" });
    ddBox.appendChild(ddHost);
    host.appendChild(ddBox);
    areaChart(ddHost, ddSeries, { yLabel: "Drawdown" });
  }
}

async function renderQrOverlay(host, runIds) {
  host.appendChild(el("h3", {}, `Overlay · ${runIds.length} runs (normalized to 1.0 at start)`));

  // Summary table
  const summaryRows = runIds.map((id) => {
    const r = qrState.runs.find((x) => x.run_id === id);
    if (!r) return null;
    const s = _qrSummary(r);
    return {
      model: r.model_name,
      ticker: r.primary_ticker,
      sharpe: s.sharpe_ratio,
      total_return: s.total_return,
      max_dd: s.max_drawdown,
      period: `${r.start} → ${r.end}`,
    };
  }).filter(Boolean);
  host.appendChild(
    makeTable(summaryRows, [
      { key: "ticker", label: "Ticker" },
      { key: "model", label: "Model" },
      {
        key: "sharpe",
        label: "Sharpe",
        format: (v) => (v == null ? "-" : fmtNumber(v, 2)),
        cellClass: (v) => (v == null ? "" : v >= 0 ? "positive" : "negative"),
      },
      {
        key: "total_return",
        label: "Total return",
        format: (v) => (v == null ? "-" : fmtPct(v)),
        cellClass: (v) => (v == null ? "" : v >= 0 ? "positive" : "negative"),
      },
      { key: "max_dd", label: "Max DD", format: (v) => (v == null ? "-" : fmtPct(v)), cellClass: () => "negative" },
      { key: "period", label: "Period" },
    ]),
  );

  // Load all selected runs' equity curves in parallel
  let payloads;
  try {
    payloads = await Promise.all(runIds.map((id) => _loadQrEquity(id).then((p) => [id, p]).catch((e) => [id, null])));
  } catch (err) {
    host.appendChild(notice(`Could not load equity for overlay: ${err.message}`, "error"));
    return;
  }

  const series = [];
  for (const [runId, points] of payloads) {
    if (!Array.isArray(points) || !points.length) continue;
    const run = qrState.runs.find((r) => r.run_id === runId);
    const label = run ? `${run.primary_ticker} · ${run.model_name}` : runId;
    const first = points.find((p) => Number.isFinite(p.portfolio_value));
    if (!first || first.portfolio_value === 0) continue;
    const base = first.portfolio_value;
    series.push({
      name: label,
      points: points
        .map((p) => ({
          date: p.date,
          value: Number.isFinite(p.portfolio_value) ? p.portfolio_value / base : null,
        }))
        .filter((p) => p.date && Number.isFinite(p.value)),
    });
  }
  if (!series.length) {
    host.appendChild(notice("None of the selected runs returned equity points.", "info"));
    return;
  }
  const chartBox = el("div", { class: "section" });
  chartBox.appendChild(el("h3", {}, "Equity overlay"));
  const chartHost = el("div", { class: "chart-host tall" });
  chartBox.appendChild(chartHost);
  host.appendChild(chartBox);
  lineChart(chartHost, series, { yLabel: "Normalized (start = 1.0)" });
}

// ── Prices tab ─────────────────────────────────────────────────────────────

async function renderPricesTab(host) {
  // Initial fetch — populates symbol checkboxes and the initial chart.
  let payload;
  try {
    payload = await api.get("/model/prices", {
      symbols: pricesState.selected_symbols,
      start: pricesState.start,
      end: pricesState.end,
      normalize: pricesState.normalize,
    });
  } catch (err) {
    host.appendChild(notice(`Could not load prices: ${err.message}`, "error"));
    return;
  }

  if (!payload.available) {
    host.appendChild(
      notice(
        "No price data found. Run the pipeline or drop etf_prices_master.parquet in place.",
        "warning",
      ),
    );
    return;
  }

  if (!pricesState.selected_symbols.length) {
    pricesState.selected_symbols = payload.selected_symbols || [];
  }
  if (!pricesState.start) pricesState.start = payload.range.start;
  if (!pricesState.end) pricesState.end = payload.range.end;

  // Build scaffold once. Checkbox / date / normalize handlers call the
  // lightweight `refreshPricesChart()` helper, which only re-fetches data and
  // redraws the chart + quotes — the controls themselves stay in place so a
  // checkbox toggle just updates the line in the existing chart instead of
  // tearing down the whole tab.
  const controlsBox = el("div", { class: "section" });
  host.appendChild(controlsBox);

  const symbolGrid = el("div", { class: "symbol-grid", id: "prices-symbol-grid" });
  for (const sym of payload.all_symbols) {
    const id = `sym-${sym}`;
    const cb = el("input", { type: "checkbox", id });
    cb.checked = pricesState.selected_symbols.includes(sym);
    cb.addEventListener("change", () => {
      pricesState.selected_symbols = Array.from(
        symbolGrid.querySelectorAll("input:checked"),
      ).map((x) => x.id.replace("sym-", ""));
      refreshPricesChart();
    });
    symbolGrid.appendChild(el("label", { for: id }, cb, " ", sym));
  }

  const startInput = el("input", {
    type: "date",
    value: pricesState.start || payload.date_min || "",
    min: payload.date_min || "",
    max: payload.date_max || "",
  });
  const endInput = el("input", {
    type: "date",
    value: pricesState.end || payload.date_max || "",
    min: payload.date_min || "",
    max: payload.date_max || "",
  });
  startInput.addEventListener("change", (e) => {
    pricesState.start = e.target.value || null;
    refreshPricesChart();
  });
  endInput.addEventListener("change", (e) => {
    pricesState.end = e.target.value || null;
    refreshPricesChart();
  });

  const normalize = el(
    "select",
    {},
    ["indexed", "raw"].map((v) => {
      const o = el("option", { value: v }, v === "indexed" ? "Indexed to 100" : "Raw close");
      if (v === pricesState.normalize) o.selected = true;
      return o;
    }),
  );
  normalize.addEventListener("change", () => {
    pricesState.normalize = normalize.value;
    refreshPricesChart();
  });

  controlsBox.appendChild(
    el(
      "div",
      { class: "controls" },
      el("div", { class: "field" }, el("label", {}, "Y-axis"), normalize),
      el("div", { class: "field" }, el("label", {}, "Start date"), startInput),
      el("div", { class: "field" }, el("label", {}, "End date"), endInput),
    ),
  );
  controlsBox.appendChild(el("label", {}, "Symbols"));
  controlsBox.appendChild(symbolGrid);

  const chartBox = el("div", { class: "section", id: "prices-chart-box" });
  host.appendChild(chartBox);

  const quotesBox = el("div", { class: "section", id: "prices-quotes-box" });
  host.appendChild(quotesBox);

  // Initial draw using the payload we already fetched.
  paintPricesChartFromPayload(payload);
}

function paintPricesChartFromPayload(payload) {
  const chartBox = document.getElementById("prices-chart-box");
  const quotesBox = document.getElementById("prices-quotes-box");
  if (!chartBox || !quotesBox) return;

  clear(chartBox);
  chartBox.appendChild(el("h3", {}, payload.y_label || ""));
  if (!payload.series?.length) {
    chartBox.appendChild(notice(payload.message || "Nothing to chart.", "info"));
  } else {
    const chartHost = el("div", { class: "chart-host" });
    chartBox.appendChild(chartHost);
    lineChart(chartHost, payload.series, { yLabel: payload.y_label || "" });
  }

  clear(quotesBox);
  quotesBox.appendChild(el("h3", {}, "Latest quotes"));
  if (payload.latest_quotes?.length) {
    quotesBox.appendChild(
      makeTable(payload.latest_quotes, [
        { key: "symbol", label: "Symbol" },
        { key: "as_of", label: "As of" },
        { key: "last_close", label: "Last close", format: (v) => fmtNumber(v, 2) },
      ]),
    );
  }
}

async function refreshPricesChart() {
  let payload;
  try {
    payload = await api.get("/model/prices", {
      symbols: pricesState.selected_symbols,
      start: pricesState.start,
      end: pricesState.end,
      normalize: pricesState.normalize,
    });
  } catch (err) {
    const chartBox = document.getElementById("prices-chart-box");
    if (chartBox) {
      clear(chartBox);
      chartBox.appendChild(notice(`Could not load prices: ${err.message}`, "error"));
    }
    return;
  }
  paintPricesChartFromPayload(payload);
}

function checkbox(label, key, state, onChange) {
  const id = `m-${key}`;
  const input = el("input", { type: "checkbox", id });
  input.checked = !!state[key];
  input.addEventListener("change", () => {
    state[key] = input.checked;
    onChange();
  });
  return el(
    "label",
    { for: id, style: { textTransform: "none", letterSpacing: "normal" } },
    input,
    " ",
    label,
  );
}

function capitalize(s) {
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : "";
}
