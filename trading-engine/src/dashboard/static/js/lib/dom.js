// DOM helpers + formatting.

// Convert snake_case / camelCase / lower numbers into Title Case For Display.
// "momentum_top5" -> "Momentum Top 5"
// "inverse_vol"   -> "Inverse Vol"
// "net_pnl"       -> "Net PnL"
const HUMANIZE_OVERRIDES = {
  pnl: "PnL",
  etf: "ETF",
  spx: "SPX",
  nav: "NAV",
  bps: "bps",
  ytd: "YTD",
  spy: "SPY",
  qqq: "QQQ",
  sp: "S&P",
  vs: "vs",
};

export function humanize(input) {
  if (input === null || input === undefined) return "";
  const s = String(input)
    .replace(/_/g, " ")
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/([a-zA-Z])(\d)/g, "$1 $2")
    .replace(/(\d)([a-zA-Z])/g, "$1 $2")
    .trim();
  return s
    .split(/\s+/)
    .map((word) => {
      const lower = word.toLowerCase();
      if (HUMANIZE_OVERRIDES[lower]) return HUMANIZE_OVERRIDES[lower];
      return lower.charAt(0).toUpperCase() + lower.slice(1);
    })
    .join(" ");
}

export function el(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(props || {})) {
    if (v === undefined || v === null) continue;
    if (k === "class") node.className = v;
    else if (k === "style" && typeof v === "object") Object.assign(node.style, v);
    else if (k === "html") node.innerHTML = v;
    else if (k.startsWith("on") && typeof v === "function") {
      node.addEventListener(k.slice(2).toLowerCase(), v);
    } else if (k === "dataset" && typeof v === "object") {
      for (const [dk, dv] of Object.entries(v)) node.dataset[dk] = dv;
    } else {
      node.setAttribute(k, v);
    }
  }
  appendChildren(node, children);
  return node;
}

function appendChildren(node, children) {
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    if (child instanceof Node) node.appendChild(child);
    else node.appendChild(document.createTextNode(String(child)));
  }
}

export function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

export function fmtPct(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return `${(value * 100).toFixed(digits)}%`;
}

export function fmtNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

export function fmtInt(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return Number(value).toLocaleString();
}

export function fmtDollars(value, { compact = false, digits = 0 } = {}) {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  const v = Number(value);
  const sign = v < 0 ? "-" : "";
  const abs = Math.abs(v);
  if (compact && abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(2)}M`;
  if (compact && abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(1)}K`;
  return `${sign}$${abs.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })}`;
}

export function fmtBps(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return `${Number(value).toFixed(digits)} bps`;
}

export function kpi(label, value, deltaText, deltaClass) {
  return el(
    "div",
    { class: "kpi" },
    el("div", { class: "label" }, label),
    el("div", { class: "value" }, value === null || value === undefined ? "-" : value),
    deltaText
      ? el("div", { class: `delta${deltaClass ? " " + deltaClass : ""}` }, deltaText)
      : null,
  );
}

export function makeTable(rows, columns) {
  const table = el("table", { class: "data" });
  const thead = el("thead");
  const tr = el("tr");
  for (const col of columns) tr.appendChild(el("th", {}, col.label));
  thead.appendChild(tr);
  table.appendChild(thead);

  const tbody = el("tbody");
  for (const row of rows) {
    const trBody = el("tr");
    for (const col of columns) {
      const raw = row[col.key];
      const formatted = col.format ? col.format(raw, row) : raw;
      const td = el(
        "td",
        col.cellClass
          ? { class: typeof col.cellClass === "function" ? col.cellClass(raw, row) : col.cellClass }
          : {},
        formatted === null || formatted === undefined ? "-" : formatted,
      );
      trBody.appendChild(td);
    }
    tbody.appendChild(trBody);
  }
  table.appendChild(tbody);
  return table;
}

export function notice(text, kind = "info") {
  return el("div", { class: `notice ${kind}` }, text);
}
