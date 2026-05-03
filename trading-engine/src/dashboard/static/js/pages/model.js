import { api } from "../lib/api.js";
import { lineChart, areaChart } from "../lib/charts.js";
import {
  clear,
  el,
  fmtNumber,
  fmtPct,
  humanize,
  kpi,
  makeTable,
  notice,
} from "../lib/dom.js";
import { getState, on } from "../lib/state.js";

let pageNode = null;
let activeTab = "models";

const modelsState = {
  selected: [],
  preset: "Custom",
  start: null,
  end: null,
  show_aggregate_portfolio: false,
  show_spx: false,
  log_scale: false,
};

const pricesState = {
  selected_symbols: [],
  start: null,
  end: null,
  normalize: "indexed",
};

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
      el("h2", {}, "Model Analysis"),
      el(
        "p",
        {},
        "Compare model backtests, marginal contributions, and the underlying ETF prices.",
      ),
    ),
  );
  pageNode.appendChild(renderHeaderMeta());

  const tabs = el(
    "div",
    { class: "tabs" },
    el(
      "button",
      { type: "button", "data-tab": "models" },
      "Models",
    ),
    el(
      "button",
      { type: "button", "data-tab": "prices" },
      "Prices",
    ),
  );
  tabs.querySelectorAll("button").forEach((b) => {
    b.classList.toggle("active", b.dataset.tab === activeTab);
    b.addEventListener("click", () => {
      activeTab = b.dataset.tab;
      tabs.querySelectorAll("button").forEach((x) => x.classList.toggle("active", x === b));
      renderActiveTab();
    });
  });
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
  if (activeTab === "models") {
    await renderModelsTab(host);
  } else {
    await renderPricesTab(host);
  }
}

// ── Models tab ─────────────────────────────────────────────────────────────

async function renderModelsTab(host) {
  const profile = getState().profile;
  const meta = await api.get("/model/meta", { profile });
  const allOptions = meta.available_options || [];

  if (!allOptions.length) {
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

  const selector = el("select", { multiple: true, size: Math.min(8, allOptions.length) });
  selector.style.minWidth = "260px";
  for (const opt of allOptions) {
    const o = el("option", { value: opt }, humanize(opt));
    if (modelsState.selected.includes(opt)) o.selected = true;
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
  const logCheck = checkbox("Log scale (base 2)", "log_scale", modelsState, refreshModelsChart);

  controlsBox.appendChild(
    el(
      "div",
      { class: "controls" },
      el("div", { class: "field", style: { minWidth: "260px" } }, el("label", {}, "Models"), selector),
      el("div", { class: "field" }, el("label", {}, "Preset"), preset),
      el("div", { class: "field" }, el("label", {}, "Start date"), startInput),
      el("div", { class: "field" }, el("label", {}, "End date"), endInput),
      el("div", { class: "field" }, aggCheck),
      el("div", { class: "field" }, spxCheck),
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

// ── Prices tab ─────────────────────────────────────────────────────────────

async function renderPricesTab(host) {
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

  // First load: populate defaults from server
  if (!pricesState.selected_symbols.length) {
    pricesState.selected_symbols = payload.selected_symbols || [];
  }
  if (!pricesState.start) pricesState.start = payload.range.start;
  if (!pricesState.end) pricesState.end = payload.range.end;

  const controlsBox = el("div", { class: "section" });
  host.appendChild(controlsBox);

  const symbolGrid = el("div", { class: "symbol-grid" });
  for (const sym of payload.all_symbols) {
    const id = `sym-${sym}`;
    const cb = el("input", { type: "checkbox", id });
    cb.checked = pricesState.selected_symbols.includes(sym);
    cb.addEventListener("change", () => {
      pricesState.selected_symbols = Array.from(
        symbolGrid.querySelectorAll("input:checked"),
      ).map((x) => x.id.replace("sym-", ""));
      renderPricesTab(host);
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
    renderPricesTab(host);
  });
  endInput.addEventListener("change", (e) => {
    pricesState.end = e.target.value || null;
    renderPricesTab(host);
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
    renderPricesTab(host);
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

  const chartBox = el("div", { class: "section" });
  chartBox.appendChild(el("h3", {}, payload.y_label || ""));
  host.appendChild(chartBox);
  if (!payload.series?.length) {
    chartBox.appendChild(notice(payload.message || "Nothing to chart.", "info"));
  } else {
    const chartHost = el("div", { class: "chart-host" });
    chartBox.appendChild(chartHost);
    lineChart(chartHost, payload.series, { yLabel: payload.y_label || "" });
  }

  const quotesBox = el("div", { class: "section" });
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
  host.appendChild(quotesBox);
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
