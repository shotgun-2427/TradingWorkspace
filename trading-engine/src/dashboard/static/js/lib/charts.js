// Chart.js wrappers. All charts use a shared muted palette and date axis.

// ── Chart.js loader (multiple CDN fallbacks) ─────────────────────────────────

const CHART_JS_SOURCES = [
  "https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js",
  "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.4/chart.umd.min.js",
  "https://unpkg.com/chart.js@4.4.4/dist/chart.umd.min.js",
];

let _chartLoadPromise = null;

function loadChartJs() {
  if (window.Chart) return Promise.resolve(window.Chart);
  if (_chartLoadPromise) return _chartLoadPromise;
  _chartLoadPromise = (async () => {
    let lastError = null;
    for (const src of CHART_JS_SOURCES) {
      try {
        await new Promise((resolve, reject) => {
          const s = document.createElement("script");
          s.src = src;
          s.async = false;
          s.onload = () => resolve();
          s.onerror = () => reject(new Error(`failed to load ${src}`));
          document.head.appendChild(s);
        });
        if (window.Chart) return window.Chart;
      } catch (err) {
        lastError = err;
        // eslint-disable-next-line no-console
        console.warn("[charts] CDN failed:", src, err);
      }
    }
    throw new Error(
      `Could not load Chart.js from any CDN. Last error: ${lastError ? lastError.message : "n/a"}`,
    );
  })();
  return _chartLoadPromise;
}

const PALETTE = [
  "#6aa3ff",
  "#c8a25c",
  "#6ea882",
  "#c5705c",
  "#9784c4",
  "#6ec0c0",
  "#c08a6e",
  "#7e9cd6",
  "#a18acb",
  "#a3a48a",
  "#88aebd",
  "#bf8b8b",
];

const COMMON_OPTIONS = {
  responsive: true,
  maintainAspectRatio: false,
  interaction: { mode: "nearest", intersect: false, axis: "x" },
  plugins: {
    legend: {
      labels: { color: "#d6dae3", boxWidth: 12, padding: 12, font: { size: 12 } },
    },
    tooltip: {
      backgroundColor: "#1c212b",
      titleColor: "#d6dae3",
      bodyColor: "#d6dae3",
      borderColor: "#262d3a",
      borderWidth: 1,
    },
  },
};

const AXES = {
  x: {
    type: "category",
    grid: { color: "rgba(124,135,148,0.15)" },
    ticks: {
      color: "#8a93a3",
      maxRotation: 0,
      autoSkip: true,
      maxTicksLimit: 10,
    },
  },
  y: {
    grid: { color: "rgba(124,135,148,0.15)" },
    ticks: { color: "#8a93a3" },
  },
};

const CATEGORY_AXES = {
  x: {
    type: "category",
    grid: { color: "rgba(124,135,148,0.15)" },
    ticks: { color: "#8a93a3", maxRotation: 60, autoSkip: false, font: { size: 11 } },
  },
  y: {
    grid: { color: "rgba(124,135,148,0.15)" },
    ticks: { color: "#8a93a3" },
  },
};

export function paletteColor(i) {
  return PALETTE[i % PALETTE.length];
}

function destroy(chart) {
  if (chart && typeof chart.destroy === "function") chart.destroy();
}

function ensureCanvas(host) {
  // Wipe anything that's there (canvas, error notice, etc.) and start clean.
  while (host.firstChild) host.removeChild(host.firstChild);
  const canvas = document.createElement("canvas");
  host.appendChild(canvas);
  return canvas;
}

function showChartError(host, err) {
  while (host.firstChild) host.removeChild(host.firstChild);
  const notice = document.createElement("div");
  notice.className = "notice error";
  notice.textContent = `Chart failed: ${err && err.message ? err.message : err}`;
  host.appendChild(notice);
  // eslint-disable-next-line no-console
  console.error("[charts]", err);
}

function chartGuard(fn) {
  return async function (host, ...rest) {
    // Replace any previous content with a small spinner while we wait for
    // Chart.js to load (first call only).
    if (!window.Chart) {
      while (host.firstChild) host.removeChild(host.firstChild);
      const spinner = document.createElement("div");
      spinner.className = "muted";
      spinner.style.padding = "12px";
      spinner.textContent = "Loading chart...";
      host.appendChild(spinner);
    }
    try {
      await loadChartJs();
      return fn(host, ...rest);
    } catch (err) {
      showChartError(host, err);
    }
  };
}

function unionLabels(series) {
  const all = new Set();
  for (const s of series) for (const p of s.points) all.add(p.date);
  return Array.from(all).sort();
}

function datasetForLabels(series, labels) {
  return series.map((s) => {
    const map = new Map(s.points.map((p) => [p.date, p.value]));
    return labels.map((l) => (map.has(l) ? map.get(l) : null));
  });
}

export const lineChart = chartGuard(function (host, series, { logScale = false, yLabel = "" } = {}) {
  destroy(host._chart);
  const canvas = ensureCanvas(host);
  const labels = unionLabels(series);
  const aligned = datasetForLabels(series, labels);
  const datasets = series.map((s, i) => ({
    label: s.name,
    data: aligned[i],
    borderColor: paletteColor(i),
    backgroundColor: paletteColor(i),
    pointRadius: 0,
    tension: 0,
    borderWidth: 1.7,
    spanGaps: true,
  }));
  host._chart = new window.Chart(canvas, {
    type: "line",
    data: { labels, datasets },
    options: {
      ...COMMON_OPTIONS,
      scales: {
        x: AXES.x,
        y: {
          ...AXES.y,
          type: logScale ? "logarithmic" : "linear",
          title: yLabel ? { display: true, text: yLabel, color: "#8a93a3" } : undefined,
        },
      },
    },
  });
});

export const areaChart = chartGuard(function (host, series, { yLabel = "", stacked = false } = {}) {
  destroy(host._chart);
  const canvas = ensureCanvas(host);
  const labels = unionLabels(series);
  const aligned = datasetForLabels(series, labels);
  const datasets = series.map((s, i) => ({
    label: s.name,
    data: aligned[i],
    borderColor: paletteColor(i),
    backgroundColor: paletteColor(i) + "55",
    pointRadius: 0,
    fill: true,
    tension: 0,
    borderWidth: 1.5,
    spanGaps: true,
  }));
  host._chart = new window.Chart(canvas, {
    type: "line",
    data: { labels, datasets },
    options: {
      ...COMMON_OPTIONS,
      scales: {
        x: AXES.x,
        y: {
          ...AXES.y,
          stacked,
          title: yLabel ? { display: true, text: yLabel, color: "#8a93a3" } : undefined,
        },
      },
    },
  });
});

export const barChart = chartGuard(function (
  host,
  { labels, values, colors, label, horizontal = false, hoverFormat } = {},
) {
  destroy(host._chart);
  const canvas = ensureCanvas(host);
  const palette =
    colors ||
    values.map((v) => (v >= 0 ? "#6ea882" : "#c5705c"));
  host._chart = new window.Chart(canvas, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label,
          data: values,
          backgroundColor: palette,
          borderColor: palette,
        },
      ],
    },
    options: {
      ...COMMON_OPTIONS,
      indexAxis: horizontal ? "y" : "x",
      plugins: {
        ...COMMON_OPTIONS.plugins,
        legend: { display: false },
        tooltip: hoverFormat
          ? { ...COMMON_OPTIONS.plugins.tooltip, callbacks: { label: hoverFormat } }
          : COMMON_OPTIONS.plugins.tooltip,
      },
      scales: CATEGORY_AXES,
    },
  });
});

export const scatterChart = chartGuard(function (host, datasets, { xLabel = "", yLabel = "" } = {}) {
  destroy(host._chart);
  const canvas = ensureCanvas(host);
  host._chart = new window.Chart(canvas, {
    type: "scatter",
    data: { datasets },
    options: {
      ...COMMON_OPTIONS,
      scales: {
        x: {
          ...AXES.y,
          title: xLabel ? { display: true, text: xLabel, color: "#8a93a3" } : undefined,
        },
        y: {
          ...AXES.y,
          title: yLabel ? { display: true, text: yLabel, color: "#8a93a3" } : undefined,
        },
      },
    },
  });
});

export const dualAxisBarLine = chartGuard(function (
  host,
  { dates, slippageBps, commissionBps, cumulativeBps, costSlippage, costCommission, cumulativeCost },
) {
  destroy(host._chart);
  const canvas = ensureCanvas(host);
  const labels = dates;
  const slipColors = slippageBps.map((v) => (v >= 0 ? "#c5705c" : "#6ea882"));
  host._chart = new window.Chart(canvas, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          type: "bar",
          label: "Slippage (bps)",
          data: slippageBps,
          backgroundColor: slipColors,
          stack: "impact",
          yAxisID: "y",
        },
        {
          type: "bar",
          label: "Commission (bps)",
          data: commissionBps,
          backgroundColor: "rgba(106,143,139,0.7)",
          stack: "impact",
          yAxisID: "y",
        },
        {
          type: "line",
          label: "Cumulative impact (bps)",
          data: cumulativeBps,
          borderColor: "#d3c39a",
          borderWidth: 2,
          pointRadius: 0,
          fill: false,
          yAxisID: "y2",
          tension: 0,
        },
      ],
    },
    options: {
      ...COMMON_OPTIONS,
      plugins: {
        ...COMMON_OPTIONS.plugins,
        tooltip: {
          ...COMMON_OPTIONS.plugins.tooltip,
          callbacks: {
            label(ctx) {
              const i = ctx.dataIndex;
              if (ctx.dataset.label.startsWith("Slippage")) {
                return `Slippage: ${ctx.parsed.y.toFixed(2)} bps  ($${(costSlippage[i] || 0).toFixed(2)})`;
              }
              if (ctx.dataset.label.startsWith("Commission")) {
                return `Commission: ${ctx.parsed.y.toFixed(2)} bps  ($${(costCommission[i] || 0).toFixed(2)})`;
              }
              return `Cumulative: ${ctx.parsed.y.toFixed(2)} bps  ($${(cumulativeCost[i] || 0).toFixed(2)})`;
            },
          },
        },
      },
      scales: {
        x: {
          type: "category",
          grid: { display: false },
          ticks: { color: "#8a93a3", maxRotation: 0, autoSkip: true, maxTicksLimit: 12 },
        },
        y: {
          grid: { color: "rgba(124,135,148,0.15)" },
          ticks: { color: "#8a93a3" },
          title: { display: true, text: "Daily impact (bps)", color: "#8a93a3" },
        },
        y2: {
          position: "right",
          grid: { display: false },
          ticks: { color: "#8a93a3" },
          title: { display: true, text: "Cumulative (bps)", color: "#8a93a3" },
        },
      },
    },
  });
});
