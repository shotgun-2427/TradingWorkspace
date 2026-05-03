import { api } from "./lib/api.js";
import { humanize } from "./lib/dom.js";
import { emit, on, setOptimizer, setProfile, toast } from "./lib/state.js";

import { renderPortfolio } from "./pages/portfolio.js";
import { renderModel } from "./pages/model.js";
import { renderAsset } from "./pages/asset.js";
import { renderSlippage } from "./pages/slippage.js";
import { renderAudit } from "./pages/audit.js";

const PAGES = {
  portfolio: { node: () => document.getElementById("page-portfolio"), render: renderPortfolio },
  model: { node: () => document.getElementById("page-model"), render: renderModel },
  asset: { node: () => document.getElementById("page-asset"), render: renderAsset },
  slippage: { node: () => document.getElementById("page-slippage"), render: renderSlippage },
  audit: { node: () => document.getElementById("page-audit"), render: renderAudit },
};

let activePage = null;
let autoRefreshTimer = null;

document.addEventListener("DOMContentLoaded", async () => {
  wireProfile();
  wireNav();
  wireSidebarActions();
  wireAutoRefresh();
  await bootstrapState();
  navigate(currentPageFromHash());

  on("profile", () => navigate(activePage, { force: true }));
});

function currentPageFromHash() {
  const hash = window.location.hash.replace(/^#\/?/, "").trim();
  return PAGES[hash] ? hash : "portfolio";
}

window.addEventListener("hashchange", () => navigate(currentPageFromHash()));

function navigate(page, { force = false } = {}) {
  if (!PAGES[page]) page = "portfolio";
  if (!force && activePage === page) return;
  activePage = page;
  for (const [name, def] of Object.entries(PAGES)) {
    const node = def.node();
    if (!node) continue;
    node.hidden = name !== page;
  }
  document.querySelectorAll("#nav .nav-item").forEach((a) => {
    a.classList.toggle("active", a.dataset.page === page);
  });
  PAGES[page].render(PAGES[page].node());
}

// ── Sidebar wiring ─────────────────────────────────────────────────────────

function wireProfile() {
  const toggle = document.getElementById("profile-toggle");
  toggle.querySelectorAll("button").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const value = btn.dataset.value;
      toggle.querySelectorAll("button").forEach((b) => b.classList.toggle("active", b === btn));
      try {
        await api.post("/profile", { profile: value });
        setProfile(value);
      } catch (err) {
        toast(`Could not change profile: ${err.message}`, "error");
      }
    });
  });
}

function wireNav() {
  document.querySelectorAll("#nav .nav-item").forEach((a) => {
    a.addEventListener("click", (event) => {
      event.preventDefault();
      const page = a.dataset.page;
      window.location.hash = `#/${page}`;
    });
  });
}

function wireSidebarActions() {
  document.getElementById("refresh-etf").addEventListener("click", async () => {
    const btn = document.getElementById("refresh-etf");
    const original = btn.textContent;
    btn.disabled = true;
    btn.textContent = "Refreshing...";
    try {
      const r = await api.post("/refresh-etf");
      if (r.ok) {
        toast(r.message || "ETF data refreshed", "success");
        await refreshMasterMeta();
        navigate(activePage, { force: true });
      } else {
        toast(r.error || "Refresh failed", "error", 7000);
      }
    } catch (err) {
      toast(err.message, "error", 7000);
    } finally {
      btn.disabled = false;
      btn.textContent = original;
    }
  });

  document.getElementById("clear-cache").addEventListener("click", async () => {
    try {
      await api.post("/cache/clear");
      toast("Cache cleared.", "success");
      navigate(activePage, { force: true });
    } catch (err) {
      toast(err.message, "error");
    }
  });

  document.getElementById("run-audit").addEventListener("click", async () => {
    const btn = document.getElementById("run-audit");
    const original = btn.textContent;
    btn.disabled = true;
    btn.textContent = "Running audit...";
    try {
      const r = await api.post("/run-audit");
      if (r.ok) {
        toast(`Audit OK - ${r.summary || "complete"}`, "success", 6000);
        await refreshMasterMeta();
        navigate(activePage, { force: true });
      } else {
        toast(r.error || "Audit failed", "error", 7000);
      }
    } catch (err) {
      toast(err.message, "error", 7000);
    } finally {
      btn.disabled = false;
      btn.textContent = original;
    }
  });

  document.getElementById("optimizer-select").addEventListener("change", async (e) => {
    const value = e.target.value;
    try {
      await api.post("/optimizer", { optimizer: value });
      setOptimizer(value);
      toast(`Optimizer set to ${value}`, "success");
    } catch (err) {
      toast(err.message, "error");
    }
  });
}

function wireAutoRefresh() {
  document.getElementById("auto-refresh-select").addEventListener("change", (e) => {
    const seconds = parseInt(e.target.value, 10);
    if (autoRefreshTimer) {
      clearInterval(autoRefreshTimer);
      autoRefreshTimer = null;
    }
    if (seconds > 0) {
      autoRefreshTimer = setInterval(() => navigate(activePage, { force: true }), seconds * 1000);
    }
  });
}

async function bootstrapState() {
  try {
    const state = await api.get("/state");
    document
      .getElementById("profile-toggle")
      .querySelectorAll("button")
      .forEach((b) => b.classList.toggle("active", b.dataset.value === state.profile));
    setProfile(state.profile);

    const optSelect = document.getElementById("optimizer-select");
    optSelect.innerHTML = "";
    const descriptions = state.optimizer_descriptions || {};
    for (const name of state.optimizers) {
      const opt = document.createElement("option");
      opt.value = name;
      opt.textContent = humanize(name);
      if (name === state.optimizer) opt.selected = true;
      optSelect.appendChild(opt);
    }
    setOptimizer(state.optimizer);
    document.getElementById("optimizer-help").textContent =
      descriptions[state.optimizer] || "";
    optSelect.addEventListener("change", () => {
      document.getElementById("optimizer-help").textContent =
        descriptions[optSelect.value] || "";
    });

    renderMasterMeta(state.master);
  } catch (err) {
    toast(`Could not load state: ${err.message}`, "error", 7000);
  }
}

async function refreshMasterMeta() {
  try {
    const meta = await api.get("/master-meta");
    renderMasterMeta(meta);
  } catch (_e) {
    /* no-op */
  }
}

function renderMasterMeta(meta) {
  const node = document.getElementById("master-meta");
  if (!meta || !meta.found) {
    node.textContent = "No price data on disk.";
    return;
  }
  node.innerHTML = `Latest bar: <strong>${meta.latest_date || "?"}</strong><br/>File mtime: <code>${meta.mtime || "?"}</code>`;
}
