import { api } from "../lib/api.js";
import { lineChart, areaChart } from "../lib/charts.js";
import {
  clear,
  el,
  fmtDollars,
  fmtNumber,
  fmtPct,
  kpi,
  notice,
} from "../lib/dom.js";
import { getState, on } from "../lib/state.js";

const initialControls = {
  start: null,
  end: null,
  show_spx: true,
  show_drawdown: false,
  log_scale: false,
  extend_history: false,
};

let pageNode = null;
let controls = { ...initialControls };

export async function renderPortfolio(node) {
  pageNode = node;
  controls = { ...initialControls };
  buildScaffold();
  await loadAndRender();
}

on("profile", () => {
  if (pageNode && !pageNode.hidden) {
    controls = { ...initialControls };
    loadAndRender();
  }
});

function buildScaffold() {
  clear(pageNode);
  pageNode.appendChild(
    el(
      "div",
      { class: "page-header" },
      el("h2", {}, "Portfolio Performance"),
      el(
        "p",
        {},
        "Equity curves for the selected portfolio and the S&P 500 benchmark.",
      ),
    ),
  );

  // Order on the page: controls -> top metrics -> chart -> drawdown -> footer summary
  const ids = ["pp-controls", "pp-metrics", "pp-chart-box", "pp-drawdown-box", "pp-summary"];
  for (const id of ids) {
    const box = el("div", { class: "section" });
    box.id = id;
    if (id === "pp-drawdown-box") box.hidden = !controls.show_drawdown;
    pageNode.appendChild(box);
  }
}

async function loadAndRender() {
  const profile = getState().profile;
  setLoading(true);
  let payload;
  try {
    payload = await api.get("/portfolio/summary", {
      profile,
      start: controls.start,
      end: controls.end,
      include_spx: controls.show_spx,
      extend_history: controls.extend_history,
    });
  } catch (err) {
    document.getElementById("pp-chart-box").innerHTML = "";
    document
      .getElementById("pp-chart-box")
      .appendChild(notice(`Could not load portfolio data: ${err.message}`, "error"));
    setLoading(false);
    return;
  }
  setLoading(false);

  if (!controls.start && payload.range?.start) controls.start = payload.range.start;
  if (!controls.end && payload.range?.end) controls.end = payload.range.end;

  renderControls(payload);
  renderMetrics(payload);
  renderEquityChart(payload);
  renderDrawdown(payload);
  renderFooterSummary(payload);
}

function setLoading(flag) {
  document.getElementById("pp-chart-box").style.opacity = flag ? "0.6" : "1";
}

function renderControls(payload) {
  const host = document.getElementById("pp-controls");
  clear(host);
  if (payload?.error) host.appendChild(notice(payload.error, "error"));

  const range = payload?.range || {};
  const startInput = el("input", {
    type: "date",
    value: controls.start || range.start || "",
    min: range.picker_min || "",
    max: range.picker_max || "",
  });
  const endInput = el("input", {
    type: "date",
    value: controls.end || range.end || "",
    min: range.picker_min || "",
    max: range.picker_max || "",
  });
  startInput.addEventListener("change", (e) => {
    controls.start = e.target.value || null;
    loadAndRender();
  });
  endInput.addEventListener("change", (e) => {
    controls.end = e.target.value || null;
    loadAndRender();
  });

  const checkbox = (label, key) => {
    const id = `pp-${key}`;
    const input = el("input", { type: "checkbox", id });
    input.checked = !!controls[key];
    input.addEventListener("change", () => {
      controls[key] = input.checked;
      loadAndRender();
    });
    return el(
      "label",
      { for: id, style: { textTransform: "none", letterSpacing: "normal" } },
      input,
      " ",
      label,
    );
  };

  host.appendChild(
    el(
      "div",
      { class: "controls" },
      el("div", { class: "field" }, el("label", {}, "Start date"), startInput),
      el("div", { class: "field" }, el("label", {}, "End date"), endInput),
      el("div", { class: "field" }, checkbox("Show S&P 500", "show_spx")),
      el("div", { class: "field" }, checkbox("Show drawdown", "show_drawdown")),
      el("div", { class: "field" }, checkbox("Log scale (base 2)", "log_scale")),
      el(
        "div",
        { class: "field" },
        checkbox("Hypothetical backtest before paper start", "extend_history"),
      ),
    ),
  );

  if (payload?.extend_history && payload?.paper_start) {
    host.appendChild(
      notice(
        `Showing hypothetical backtest before ${payload.paper_start}. The curve uses today's position vector applied to historical close prices.`,
        "warning",
      ),
    );
  }
}

function renderMetrics(payload) {
  const host = document.getElementById("pp-metrics");
  clear(host);
  if (!payload || !payload.available || !payload.metrics) {
    host.appendChild(notice(payload?.message || "No data available.", "info"));
    return;
  }
  const m = payload.metrics;
  const years = m.years_observed || 0;

  const annualizedReturn =
    years >= 0.25 && m.annualized_return !== null && m.annualized_return !== undefined
      ? fmtPct(m.annualized_return)
      : `${fmtPct(m.total_return)} (period)`;

  const alphaText = m.alpha === null || m.alpha === undefined ? "-" : fmtPct(m.alpha);
  const alphaClass =
    m.alpha === null || m.alpha === undefined
      ? null
      : m.alpha >= 0
        ? "positive"
        : "negative";

  const totalReturnClass =
    m.total_return === null || m.total_return === undefined
      ? null
      : m.total_return >= 0
        ? "positive"
        : "negative";

  const cards = [
    kpi("Annualized return", annualizedReturn, null, alphaClass /* re-use sign of alpha vs spx */),
    kpi("Annualized volatility", fmtPct(m.annualized_volatility)),
    kpi("Alpha vs S&P 500", alphaText, null, alphaClass),
    kpi("Total return", fmtPct(m.total_return), null, totalReturnClass),
    kpi("Max drawdown", fmtPct(m.max_drawdown), null, "negative"),
  ];

  // Override the colour on the annualized-return card: it shouldn't ride
  // alpha's sign. Easier to just pass null above and let CSS use the default.
  cards[0] = kpi(
    "Annualized return",
    annualizedReturn,
    years < 0.25 ? "Window <3mo - period return shown" : null,
  );

  host.appendChild(el("h3", {}, "Performance metrics"));
  host.appendChild(el("div", { class: "kpi-row" }, ...cards));

  // Sub-line with sharpe + sample window
  const lines = [];
  if (m.sharpe_ratio !== null && m.sharpe_ratio !== undefined && years >= 0.25) {
    lines.push(`Sharpe (rf=0): ${fmtNumber(m.sharpe_ratio, 2)}`);
  }
  if (m.benchmark) {
    if (m.benchmark.annualized_return !== null && m.benchmark.annualized_return !== undefined) {
      lines.push(`S&P 500 annualized: ${fmtPct(m.benchmark.annualized_return)}`);
    }
    if (m.benchmark.max_drawdown !== null && m.benchmark.max_drawdown !== undefined) {
      lines.push(`S&P 500 max DD: ${fmtPct(m.benchmark.max_drawdown)}`);
    }
  }
  if (m.days_observed) lines.push(`Sample: ${m.days_observed.toFixed(0)} calendar days`);
  if (lines.length) {
    host.appendChild(
      el("p", { class: "muted", style: { marginTop: "10px" } }, lines.join("  -  ")),
    );
  }
}

function renderEquityChart(payload) {
  const host = document.getElementById("pp-chart-box");
  clear(host);
  host.appendChild(el("h3", {}, "Equity curves"));

  if (!payload?.available || !payload.series?.length) {
    host.appendChild(
      notice(payload?.message || payload?.error || "Nothing to chart.", "info"),
    );
    return;
  }

  const useLog =
    controls.log_scale &&
    payload.series.every((s) => s.points.every((p) => p.value > 0));
  if (controls.log_scale && !useLog) {
    host.appendChild(
      notice(
        "Log base-2 scale requires positive equity values. Showing linear scale.",
        "warning",
      ),
    );
  }

  const chartHost = el("div", { class: "chart-host" });
  host.appendChild(chartHost);
  lineChart(chartHost, payload.series, {
    logScale: useLog,
    yLabel: "Cumulative returns",
  });
}

function renderDrawdown(payload) {
  const host = document.getElementById("pp-drawdown-box");
  host.hidden = !controls.show_drawdown;
  if (!controls.show_drawdown) return;
  clear(host);
  host.appendChild(el("h3", {}, "Drawdown"));
  if (!payload?.available || !payload.drawdown?.length) {
    host.appendChild(notice("No drawdown data.", "info"));
    return;
  }
  const chartHost = el("div", { class: "chart-host" });
  host.appendChild(chartHost);
  areaChart(chartHost, payload.drawdown, { yLabel: "Drawdown" });
}

function renderFooterSummary(payload) {
  const host = document.getElementById("pp-summary");
  clear(host);
  if (!payload || !payload.available) return;
  const navText =
    payload.latest_nav !== null && payload.latest_nav !== undefined
      ? fmtDollars(payload.latest_nav, { digits: 0 })
      : "-";
  host.appendChild(
    el(
      "div",
      { class: "kpi-row" },
      kpi(`${capitalize(payload.profile || "paper")} NAV`, navText),
      kpi("Range", `${payload.range?.start || "-"} -> ${payload.range?.end || "-"}`),
      kpi(
        "Sample window",
        payload.metrics?.days_observed
          ? `${Math.round(payload.metrics.days_observed)} days`
          : "-",
      ),
    ),
  );
  if (payload.nav_source) {
    host.appendChild(
      el("p", { class: "muted", style: { marginTop: "10px" } }, `NAV ${payload.nav_source}.`),
    );
  }
}

function capitalize(s) {
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : "";
}
