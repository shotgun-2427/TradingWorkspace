import { api } from "../lib/api.js";
import { barChart, dualAxisBarLine, scatterChart } from "../lib/charts.js";
import {
  clear,
  el,
  fmtBps,
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

const state = {
  start: null,
  end: null,
  mode: "Signed Slippage",
  sort_column: "mean_gross_cost_bps",
};

let pageNode = null;

export async function renderSlippage(node) {
  pageNode = node;
  build();
  await reload();
}

on("profile", () => {
  if (pageNode && !pageNode.hidden) reload();
});

function build() {
  clear(pageNode);
  pageNode.appendChild(
    el(
      "div",
      { class: "page-header" },
      el("h2", {}, "Slippage Analysis"),
      el(
        "p",
        {},
        "Signed and absolute slippage, commissions, and realized execution costs from trade reconciliation.",
      ),
    ),
  );
  pageNode.appendChild(el("div", { id: "slippage-host" }));
}

async function reload() {
  const profile = getState().profile;
  let payload;
  try {
    payload = await api.get("/slippage/report", {
      profile,
      start: state.start,
      end: state.end,
      mode: state.mode,
    });
  } catch (err) {
    document.getElementById("slippage-host").innerHTML = "";
    document
      .getElementById("slippage-host")
      .appendChild(notice(`Could not load slippage data: ${err.message}`, "error"));
    return;
  }

  const host = document.getElementById("slippage-host");
  clear(host);

  const meta = await api.get("/slippage/meta", { profile });

  // Controls
  const controlsBox = el("div", { class: "section" });
  const startInput = el("input", {
    type: "date",
    value: state.start || meta.earliest || "",
    min: meta.earliest || "",
    max: meta.latest || "",
  });
  const endInput = el("input", {
    type: "date",
    value: state.end || meta.latest || "",
    min: meta.earliest || "",
    max: meta.latest || "",
  });
  startInput.addEventListener("change", (e) => {
    state.start = e.target.value || null;
    reload();
  });
  endInput.addEventListener("change", (e) => {
    state.end = e.target.value || null;
    reload();
  });
  const modeToggle = el("div", { class: "segmented" });
  for (const opt of meta.modes || []) {
    const btn = el("button", { type: "button" }, opt);
    if (state.mode === opt) btn.classList.add("active");
    btn.addEventListener("click", () => {
      state.mode = opt;
      reload();
    });
    modeToggle.appendChild(btn);
  }
  controlsBox.appendChild(
    el(
      "div",
      { class: "controls" },
      el("div", { class: "field" }, el("label", {}, "Start date"), startInput),
      el("div", { class: "field" }, el("label", {}, "End date"), endInput),
      el("div", { class: "field" }, el("label", {}, "Slippage view"), modeToggle),
    ),
  );
  if (meta.preview_root) {
    controlsBox.appendChild(
      notice(`Local preview mode active: ${meta.preview_root}`, "info"),
    );
  }
  host.appendChild(controlsBox);

  if (!payload.available) {
    host.appendChild(notice(payload.message || payload.error || "No data.", "info"));
    return;
  }
  if (payload.error) {
    host.appendChild(notice(payload.error, "error"));
    return;
  }
  if (payload.legacy_reports_skipped) {
    host.appendChild(
      notice(
        `Skipped ${payload.legacy_reports_skipped} legacy slippage report(s). Only v2 reports are shown.`,
        "warning",
      ),
    );
  }

  const ov = payload.overview || {};
  // Overview
  host.appendChild(
    el(
      "div",
      { class: "section" },
      el("h3", {}, "Overview"),
      el(
        "div",
        { class: "kpi-row" },
        kpi("Reports loaded", fmtInt(payload.reports_loaded)),
        kpi("Trades", fmtInt(ov.trades)),
        kpi("Total fill notional", fmtDollars(ov.total_fill_notional, { compact: true })),
        kpi("Total slippage", fmtDollars(ov.signed_slippage_dollars, { compact: true })),
        kpi("Weighted slippage", fmtBps(ov.weighted_slippage_bps)),
        kpi("Total commissions", fmtDollars(ov.total_commission_dollars, { compact: true })),
        kpi("Weighted commission", fmtBps(ov.weighted_commission_bps)),
        kpi("Total net cost", fmtDollars(ov.total_net_cost_dollars, { compact: true })),
        kpi("Weighted net cost", fmtBps(ov.weighted_net_cost_bps)),
      ),
    ),
  );

  // Trade mix
  host.appendChild(
    el(
      "div",
      { class: "section" },
      el("h3", {}, "Trade mix"),
      el(
        "div",
        { class: "kpi-row" },
        kpi("Favorable fills", fmtPct(ov.favorable_fill_rate, 1)),
        kpi("Adverse fills", fmtPct(ov.adverse_fill_rate, 1)),
        kpi("Small-trade share (<$500)", fmtPct(ov.small_trade_share, 1)),
        kpi("One-share trades", fmtInt(ov.one_share_trade_count)),
      ),
      el(
        "p",
        { class: "muted", style: { marginTop: "8px" } },
        `${fmtPct(ov.flat_fill_rate, 1)} of fills were exactly at the decision price.`,
      ),
    ),
  );

  // Calibration
  const cal = payload.calibration || {};
  const calBox = el("div", { class: "section" });
  calBox.appendChild(el("h3", {}, "Calibration"));
  const calRow = el("div", { class: "row-1-2" });

  const calKpis = el(
    "div",
    {},
    el(
      "div",
      { class: "kpi-row" },
      kpi("Simulated slippage", fmtBps(cal.simulated_slippage_bps)),
      kpi(
        "Weighted gross cost",
        fmtBps(cal.weighted_gross_cost_bps),
        cal.delta_bps !== null ? fmtBps(cal.delta_bps) : null,
        cal.delta_bps !== null && cal.delta_bps < 0 ? "positive" : "negative",
      ),
      kpi("Weighted net cost", fmtBps(cal.weighted_net_cost_bps)),
    ),
  );
  if (cal.weighted_gross_cost_bps !== null) {
    if (cal.weighted_gross_cost_bps < cal.simulated_slippage_bps * 0.8) {
      calKpis.appendChild(
        notice("Gross realized cost is below the simulation friction assumption.", "info"),
      );
    } else if (cal.weighted_gross_cost_bps > cal.simulated_slippage_bps * 1.2) {
      calKpis.appendChild(
        notice("Gross realized cost is above the simulation friction assumption.", "error"),
      );
    } else {
      calKpis.appendChild(
        notice("Gross realized cost is close to the simulation friction assumption.", "info"),
      );
    }
  }

  const impactHost = el("div", { class: "chart-host tall" });
  calRow.appendChild(calKpis);
  calRow.appendChild(impactHost);
  calBox.appendChild(calRow);
  host.appendChild(calBox);

  if (payload.daily?.length) {
    dualAxisBarLine(impactHost, {
      dates: payload.daily.map((r) => r.trade_date),
      slippageBps: payload.daily.map((r) => r.slippage_impact_bps || 0),
      commissionBps: payload.daily.map((r) => r.commission_impact_bps || 0),
      cumulativeBps: payload.daily.map((r) => r.cumulative_execution_impact_bps || 0),
      costSlippage: payload.daily.map((r) => r.total_slippage_cost || 0),
      costCommission: payload.daily.map((r) => r.total_commission || 0),
      cumulativeCost: payload.daily.map((r) => r.cumulative_execution_cost || 0),
    });
  }

  // Asset and order size
  const assetRow = el("div", { class: "row-2" });

  const assetBox = el("div", { class: "section" });
  assetBox.appendChild(el("h3", {}, `${payload.metric_label} by asset`));
  const assetHost = el("div", { class: "chart-host tall" });
  assetBox.appendChild(assetHost);

  const scatterBox = el("div", { class: "section" });
  scatterBox.appendChild(el("h3", {}, `${payload.metric_label} vs order size`));
  const scatterHost = el("div", { class: "chart-host tall" });
  scatterBox.appendChild(scatterHost);

  assetRow.appendChild(assetBox);
  assetRow.appendChild(scatterBox);
  host.appendChild(assetRow);

  if (payload.asset_summary?.length) {
    barChart(assetHost, {
      labels: payload.asset_summary.map((r) => r.ticker),
      values: payload.asset_summary.map((r) => r.average_metric_bps),
      label: payload.metric_label,
    });
  }

  if (payload.scatter?.length) {
    const buys = payload.scatter
      .filter((r) => r.action === "BUY")
      .map((r) => ({ x: r.nav_pct, y: r.metric }));
    const sells = payload.scatter
      .filter((r) => r.action === "SELL")
      .map((r) => ({ x: r.nav_pct, y: r.metric }));
    scatterChart(
      scatterHost,
      [
        { label: "BUY", data: buys, backgroundColor: "#6ea882", pointRadius: 3 },
        { label: "SELL", data: sells, backgroundColor: "#c8a25c", pointRadius: 3 },
      ],
      { xLabel: "Order size (% NAV)", yLabel: `${payload.metric_label} (bps)` },
    );
  }

  // Ticker breakdown
  const tickerBox = el("div", { class: "section" });
  tickerBox.appendChild(el("h3", {}, "Ticker breakdown"));

  const sortColumns = [
    "mean_gross_cost_bps",
    "mean_net_cost_bps",
    "mean_absolute_slippage_bps",
    "mean_signed_slippage_bps",
    "mean_commission_bps",
    "trades",
  ];
  const sortSelect = el(
    "select",
    {},
    sortColumns.map((c) => {
      const o = el("option", { value: c }, humanize(c));
      if (c === state.sort_column) o.selected = true;
      return o;
    }),
  );
  sortSelect.addEventListener("change", () => {
    state.sort_column = sortSelect.value;
    reload();
  });
  tickerBox.appendChild(
    el(
      "div",
      { class: "controls" },
      el("div", { class: "field" }, el("label", {}, "Sort by"), sortSelect),
    ),
  );

  const sortedTickers = [...(payload.ticker_table || [])].sort(
    (a, b) => (b[state.sort_column] || 0) - (a[state.sort_column] || 0),
  );

  tickerBox.appendChild(
    makeTable(sortedTickers, [
      { key: "ticker", label: "Ticker" },
      { key: "trades", label: "Trades", format: (v) => fmtInt(v) },
      { key: "mean_signed_slippage_bps", label: "Signed slip (bps)", format: (v) => fmtNumber(v, 2) },
      { key: "mean_absolute_slippage_bps", label: "Abs slip (bps)", format: (v) => fmtNumber(v, 2) },
      { key: "mean_commission_bps", label: "Commission (bps)", format: (v) => fmtNumber(v, 2) },
      { key: "mean_net_cost_bps", label: "Net cost (bps)", format: (v) => fmtNumber(v, 2) },
      { key: "mean_gross_cost_bps", label: "Gross cost (bps)", format: (v) => fmtNumber(v, 2) },
      { key: "median_fill_ratio", label: "Median fill", format: (v) => fmtNumber(v, 2) },
    ]),
  );
  host.appendChild(tickerBox);
}
