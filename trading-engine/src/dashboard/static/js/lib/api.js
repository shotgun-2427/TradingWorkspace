// Tiny fetch wrapper for the dashboard API.
const BASE = "/api";

async function call(method, path, { params, body } = {}) {
  const url = new URL(BASE + path, window.location.origin);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v === undefined || v === null) continue;
      if (Array.isArray(v)) {
        for (const item of v) url.searchParams.append(k, item);
      } else if (typeof v === "boolean") {
        url.searchParams.set(k, v ? "true" : "false");
      } else {
        url.searchParams.set(k, String(v));
      }
    }
  }
  const opts = {
    method,
    headers: body !== undefined ? { "Content-Type": "application/json" } : {},
    body: body !== undefined ? JSON.stringify(body) : undefined,
  };
  const res = await fetch(url.toString(), opts);
  if (!res.ok) {
    let detail = "";
    try {
      detail = (await res.json()).detail || "";
    } catch (_e) {
      detail = await res.text();
    }
    throw new Error(`${res.status} ${res.statusText}${detail ? `: ${detail}` : ""}`);
  }
  return res.json();
}

export const api = {
  get: (path, params) => call("GET", path, { params }),
  post: (path, body, params) => call("POST", path, { body, params }),
};
