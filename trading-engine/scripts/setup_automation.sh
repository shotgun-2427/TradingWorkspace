#!/usr/bin/env bash
# setup_automation.sh — One-shot bootstrap for hands-off paper trading.
#
# What this does:
#   1. Verifies the venv Python and required packages
#   2. Installs / reloads the LaunchAgent (Mon-Fri 16:32 ET)
#   3. Schedules a weekday wake at 16:30 via pmset (needs sudo)
#   4. Disables system sleep when on AC power
#   5. Prints checklist for the bits that need manual user action
#      (TWS / IB Gateway, Auto-login)
#
# Usage:
#   cd /Users/tradingworkspace/TradingWorkspace/trading-engine
#   bash scripts/setup_automation.sh
#
# Re-running is safe — every step is idempotent.

set -euo pipefail

# ── Paths ──────────────────────────────────────────────────────────────
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV_PYTHON="${VENV_PYTHON:-/Users/tradingworkspace/TradingWorkspace/.venv/bin/python}"
LAUNCHAGENT_LABEL="com.capitalfund.daily-runner"
LAUNCHAGENT_PLIST="$HOME/Library/LaunchAgents/${LAUNCHAGENT_LABEL}.plist"

# Default: TWS paper port. Override with IBKR_PORT_PAPER=4002 for IB Gateway.
IBKR_PORT_PAPER="${IBKR_PORT_PAPER:-7497}"
IBKR_PORT_LIVE="${IBKR_PORT_LIVE:-7496}"

# Fire time in local time (24h)
WAKE_HOUR=16
WAKE_MINUTE=30
RUN_HOUR=16
RUN_MINUTE=32

# ── Helpers ────────────────────────────────────────────────────────────
green()  { printf '\033[1;32m%s\033[0m\n' "$1"; }
yellow() { printf '\033[1;33m%s\033[0m\n' "$1"; }
red()    { printf '\033[1;31m%s\033[0m\n' "$1"; }
hr()     { printf '\n──────────────────────────────────────────────────────────────\n'; }

# ── 1. Check venv + deps ───────────────────────────────────────────────
hr
green "Step 1/5 · Verify Python environment"

if [[ ! -x "$VENV_PYTHON" ]]; then
  red "✗ Cannot find venv python at $VENV_PYTHON"
  red "  Set VENV_PYTHON=/path/to/python and re-run."
  exit 1
fi
green "  ✓ Found $VENV_PYTHON"

required_pkgs=(pandas pyarrow streamlit ib_async plotly)
missing=()
for pkg in "${required_pkgs[@]}"; do
  if ! "$VENV_PYTHON" -c "import $pkg" 2>/dev/null; then
    missing+=("$pkg")
  fi
done
if (( ${#missing[@]} > 0 )); then
  yellow "  ⚠ Missing packages: ${missing[*]}"
  echo "    Installing now…"
  "$VENV_PYTHON" -m pip install --quiet "${missing[@]}"
  green "  ✓ Installed"
else
  green "  ✓ All required packages present"
fi

# ── 2. Install / reload LaunchAgent ────────────────────────────────────
hr
green "Step 2/5 · Install + load LaunchAgent"

cd "$PROJECT_ROOT"

# Tear down any previous registration
launchctl bootout "gui/$(id -u)/${LAUNCHAGENT_LABEL}" 2>/dev/null || true

# Re-generate the plist with current ports
IBKR_PORT_PAPER="$IBKR_PORT_PAPER" IBKR_PORT_LIVE="$IBKR_PORT_LIVE" \
  "$VENV_PYTHON" -m src.production.scheduler --install-launchagent

if [[ ! -f "$LAUNCHAGENT_PLIST" ]]; then
  red "✗ Plist was not created"
  exit 1
fi
green "  ✓ Plist installed at $LAUNCHAGENT_PLIST"
green "  ✓ Configured for IBKR_PORT_PAPER=$IBKR_PORT_PAPER, IBKR_PORT_LIVE=$IBKR_PORT_LIVE"
green "  ✓ Will fire Mon-Fri at ${RUN_HOUR}:$(printf '%02d' "$RUN_MINUTE") local"

# ── 3. Schedule a weekday wake ─────────────────────────────────────────
hr
green "Step 3/5 · Schedule weekday wake at ${WAKE_HOUR}:$(printf '%02d' "$WAKE_MINUTE") (needs sudo)"
echo "  This wakes the Mac on Mon-Fri at ${WAKE_HOUR}:$(printf '%02d' "$WAKE_MINUTE") so the"
echo "  ${RUN_HOUR}:$(printf '%02d' "$RUN_MINUTE") LaunchAgent has 2 minutes of warmup."
echo
read -rp "  Run 'sudo pmset repeat wakeorpoweron MTWRF ${WAKE_HOUR}:$(printf '%02d' "$WAKE_MINUTE"):00'? [y/N] " yn
if [[ "$yn" =~ ^[Yy]$ ]]; then
  sudo pmset repeat wakeorpoweron MTWRF "${WAKE_HOUR}:$(printf '%02d' "$WAKE_MINUTE"):00"
  green "  ✓ Wake scheduled"
  pmset -g sched
else
  yellow "  ⚠ Skipped — Mac may sleep through the LaunchAgent fire time"
fi

# ── 4. Disable sleep on AC ─────────────────────────────────────────────
hr
green "Step 4/5 · Configure power management for unattended operation (needs sudo)"
echo "  This prevents sleep when plugged in (good for desktops / always-plugged laptops)."
echo "  On battery your Mac will still sleep normally."
echo
read -rp "  Run 'sudo pmset -c sleep 0 disksleep 0 displaysleep 30'? [y/N] " yn
if [[ "$yn" =~ ^[Yy]$ ]]; then
  sudo pmset -c sleep 0 disksleep 0 displaysleep 30
  green "  ✓ AC sleep disabled (display still sleeps after 30 min)"
else
  yellow "  ⚠ Skipped — Mac may sleep on AC"
fi

# ── 5. Manual checklist ────────────────────────────────────────────────
hr
green "Step 5/5 · Manual checklist (one-time)"
cat <<'EOF'

The remaining pieces require user action — script can't do them safely:

  [ ] TWS / IB Gateway running and logged in (paper account)
        File → Global Configuration → API → Settings:
          ☑ Enable ActiveX and Socket Clients
          Socket port: 7497 (TWS paper) or 4002 (IB Gateway paper)
          ☐ Read-Only API   (must be UNCHECKED)
        Save with File → Save Settings

  [ ] Auto-login enabled
        System Settings → Users & Groups → 'Automatically log in as'
        (without this, the LaunchAgent won't fire across reboots)

  [ ] Display sleep but not system sleep
        System Settings → Energy / Battery
        (sleep settings just changed by step 4 also help here)

  [ ] IBKR Gateway / TWS auto-restart enabled
        TWS:        File → Global Configuration → Lock and Exit → Auto restart
        IB Gateway: Configure → Settings → Lock and Exit → Auto restart

  [ ] If you want trading to fire even when no user is logged in,
      see scripts/HEADLESS.md for the IBC + LaunchDaemon path.

EOF

hr
green "Bootstrap complete."
echo
echo "Verify by opening the dashboard:"
echo "  cd $PROJECT_ROOT"
echo "  $VENV_PYTHON -m streamlit run src/dashboard/streamlit_entrypoint.py"
echo
echo "Then go to Analytics → System Audit. You want every row green."
echo "Anything yellow tells you exactly which command to run to fix it."
