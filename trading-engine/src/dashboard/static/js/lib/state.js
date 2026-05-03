// Tiny event bus + global state.

const listeners = new Map();
const state = {
  profile: "paper",
  optimizer: null,
};

export function on(event, fn) {
  if (!listeners.has(event)) listeners.set(event, new Set());
  listeners.get(event).add(fn);
  return () => listeners.get(event)?.delete(fn);
}

export function emit(event, payload) {
  const fns = listeners.get(event);
  if (!fns) return;
  for (const fn of fns) fn(payload);
}

export function getState() {
  return state;
}

export function setProfile(p) {
  if (state.profile === p) return;
  state.profile = p;
  emit("profile", p);
}

export function setOptimizer(name) {
  if (state.optimizer === name) return;
  state.optimizer = name;
  emit("optimizer", name);
}

export function toast(message, kind = "info", duration = 4000) {
  const host = document.getElementById("toaster");
  if (!host) return;
  const t = document.createElement("div");
  t.className = `toast ${kind}`;
  t.textContent = message;
  host.appendChild(t);
  setTimeout(() => {
    t.style.opacity = "0";
    t.style.transition = "opacity 0.3s ease";
    setTimeout(() => t.remove(), 320);
  }, duration);
}
