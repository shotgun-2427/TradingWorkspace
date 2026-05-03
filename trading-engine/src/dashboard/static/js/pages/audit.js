import { api } from "../lib/api.js";
import { clear, el, fmtInt, kpi, makeTable, notice } from "../lib/dom.js";
import { getState, on } from "../lib/state.js";

let pageNode = null;

export async function renderAudit(node) {
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
      el("h2", {}, "System Audit"),
      el(
        "p",
        {},
        "Sanity-check every data source the dashboard reads. Run this before market open or after pipeline edits.",
      ),
    ),
  );

  const refresh = el("button", { type: "button" }, "Re-run all checks");
  refresh.style.maxWidth = "240px";
  refresh.addEventListener("click", () => reload(true));
  pageNode.appendChild(
    el("div", { class: "section" }, refresh, el("div", { id: "audit-host" })),
  );
}

async function reload(force = false) {
  const profile = getState().profile;
  let payload;
  try {
    payload = await api.get("/audit/run", { profile, refresh: force });
  } catch (err) {
    document.getElementById("audit-host").innerHTML = "";
    document
      .getElementById("audit-host")
      .appendChild(notice(`Could not run audit: ${err.message}`, "error"));
    return;
  }

  const host = document.getElementById("audit-host");
  clear(host);

  const counts = payload.counts || {};
  host.appendChild(
    el(
      "div",
      { style: { marginTop: "16px" } },
      el(
        "div",
        { class: "kpi-row" },
        kpi("Total checks", fmtInt(payload.total)),
        kpi("Pass", fmtInt(counts.pass), null, "positive"),
        kpi("Warn", fmtInt(counts.warn)),
        kpi("Fail", fmtInt(counts.fail), null, counts.fail ? "negative" : null),
      ),
    ),
  );

  if (counts.fail > 0) {
    host.appendChild(notice(`${counts.fail} check(s) failed. Trading is not safe until these are fixed.`, "error"));
  } else if (counts.warn > 0) {
    host.appendChild(
      notice(`${counts.warn} warning(s). Pipeline will likely run but data may be stale.`, "warning"),
    );
  } else {
    host.appendChild(notice("All systems green. Pipeline is healthy.", "info"));
  }

  host.appendChild(el("hr", { class: "divider" }));
  host.appendChild(el("h3", {}, "Details"));
  host.appendChild(
    makeTable(payload.checks || [], [
      {
        key: "status",
        label: "Status",
        format: (v) =>
          el(
            "span",
            { class: `tag ${v}` },
            v ? v.toUpperCase() : "?",
          ),
      },
      { key: "check", label: "Check" },
      { key: "detail", label: "Detail" },
      { key: "severity", label: "Severity" },
    ]),
  );
}
