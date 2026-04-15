# Trading Engine: Automated Momentum-Based ETF Portfolio Management

## Overview

This repository contains a production-grade quantitative trading system that implements AMMA (Adaptive Momentum), a systematic momentum-based asset allocation strategy. The system automatically manages a multi-million-dollar paper trading portfolio by selecting and rebalancing a diversified basket of exchange-traded funds (ETFs) based on 126-day price momentum signals. The entire pipeline—from data ingestion through order submission—runs autonomously on a daily schedule post-market close.

## Strategy: AMMA (Adaptive Momentum)

### Methodology

AMMA is a rules-based momentum rotation strategy that operates on the following principles:

**Universe of Assets:** 22 ETFs spanning multiple asset classes
- Equities: SPY, QQQ, TQQQ, VEA, VWO, INDA, VFH, VHT, VIS, VNQ, VPU
- Commodities: GLD, SLV, USO, UNG
- Fixed Income: TLT, IEI, SHY, BIL
- Digital Assets: IBIT
- Volatility: VIXY
- Blockchain: ETHA

**Signal Computation:** Momentum is calculated as 126-day (approximately 6-month) price returns. This lookback window captures intermediate-term trend persistence while filtering out noise from daily volatility.

**Portfolio Construction:** 
- Selection: Top 5 ETFs ranked by momentum score
- Allocation: Equal weight (20% each) across selected holdings
- Cash Buffer: 0.5% held in cash for operational liquidity

**Rebalancing Frequency:** Monthly, executed on the last trading day of each month when the momentum signal update indicates a new allocation.

### Theoretical Justification

Momentum strategies exploit the empirical observation that asset price trends exhibit mean-reversion over short horizons and continuation over intermediate horizons (Jegadeesh & Titman, 1993; Novy-Marx & Velikov, 2016). By systematically rotating into the strongest performers on a 6-month lookback, AMMA captures this continuation effect while maintaining diversification across asset classes. Monthly rebalancing provides a balance between capturing trend persistence and avoiding excessive turnover costs.

## System Architecture

### Core Components

**Data Pipeline**
- Price ingestion from Interactive Brokers (IBKR) via the native Python API
- Daily bars (OHLCV) fetched for all 22 ETFs post-market close
- Master price database maintained as Parquet (columnar format for efficient querying)
- Automatic detection and backfilling of data gaps

**Signal Generation**
- 126-day rolling momentum calculation using normalized returns
- Monthly target generation that identifies top-5 performers
- Target file stores (symbol, momentum_rank, target_weight, reference_price, rebalance_date)

**Order Management**
- Automated detection of rebalance dates
- Basket construction comparing target allocation vs. current IBKR positions
- Duplicate submission guard to prevent multiple fills for the same rebalance date
- Order staging and audit logging (JSON-formatted run logs)

**Monitoring & Control**
- Real-time dashboard displaying NAV, positions, order status, and risk metrics
- Pre-flight checks: broker connectivity, data freshness, basket integrity
- Manual override capabilities for ad-hoc rebalancing or pause operations

### Daily Automation Pipeline

The system executes the following workflow each weekday at 16:32 ET (post-market close):

1. **Data Append** (5-day lookback IBKR fetch)
   - Connects to IBKR paper account (port 4002)
   - Retrieves daily bars for all 22 symbols
   - Appends to master price file

2. **Target Generation** (momentum calculation)
   - Computes 126-day momentum for each ETF
   - Ranks by signal strength
   - Identifies rebalance trigger (target date not in current month = rebalance needed)

3. **Rebalance Detection** (conditional trigger)
   - Checks if latest target date precedes current month
   - If yes, proceeds to order generation
   - If no, skips order submission (already rebalanced this month)

4. **Basket Construction** (difference calculation)
   - Compares target allocation to current IBKR positions
   - Calculates required shares for each symbol
   - Generates buy/sell orders with proper sizing

5. **Order Submission** (IBKR execution)
   - Submits market orders to paper account
   - Logs execution details and fills
   - Records duplicate submission guard state

Execution logs are written to `artifacts/runs/daily_run_YYYYMMDD_HHMMSS.json` containing step-by-step status and error details.

## Setup and Installation

### Prerequisites

- **Python 3.11+** with Poetry package manager
- **Docker** (for IB Gateway container running paper trading port 4002)
- **macOS or Linux** (scheduler uses LaunchAgent; Windows requires alternative setup)
- **Interactive Brokers Account** with API access enabled (paper account credentials required)

### Initial Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/shotgun-2427/TradingWorkspace.git
   cd trading-engine
   ```

2. Install dependencies via Poetry:
   ```bash
   poetry install
   poetry shell
   ```

3. Start IB Gateway Docker container (paper trading):
   ```bash
   cd ../ibgw
   docker compose up -d
   cd ../trading-engine
   ```

4. Fill initial data gap (one-time backfill of 45 days):
   ```bash
   python -m src.production.backfill_gaps --lookback "45 D"
   ```

5. Generate initial targets:
   ```bash
   python -m src.production.generate_targets
   ```

### Scheduling Daily Runs

**Option A: macOS LaunchAgent (recommended, runs at startup)**
```bash
python -m src.production.scheduler --install-launchagent
```

Runs automatically every weekday at 16:32 ET. Check status:
```bash
launchctl list | grep capitalfund
```

**Option B: APScheduler in-process (keep terminal open)**
```bash
python -m src.production.scheduler
```

**Option C: Manual trigger**
```bash
# Dry run (no orders submitted)
python -m src.production.daily_runner --dry-run

# Full execution
python -m src.production.daily_runner

# Force rebalance regardless of monthly state
python -m src.production.daily_runner --force-rebalance
```

## Dashboard

Launch the monitoring dashboard:
```bash
streamlit run src/dashboard/streamlit_entrypoint.py --server.port 8501
```

Access at http://localhost:8501

**Dashboard Pages:**

- **Home:** System status, NAV, positions, data freshness indicators
- **Run Pipeline:** Manual controls for each pipeline step
- **Basket Review:** Preview pending trades
- **Submit Orders:** Submit paper orders with safety confirmation
- **Orders & Fills:** Historical order and fill record
- **Risk Monitor:** Pre-flight checks before order submission
- **Portfolio Performance:** NAV equity curve vs. S&P 500 benchmark, performance metrics
- **Backtest Charts:** Historical price performance and allocation history

## Project Structure

```
trading-engine/
├── src/
│   ├── production/
│   │   ├── backfill_gaps.py          # Historical data backfill (45-day IBKR fetch)
│   │   ├── daily_runner.py           # Full daily pipeline orchestration
│   │   ├── generate_targets.py       # Momentum calculation and target generation
│   │   ├── scheduler.py              # APScheduler and LaunchAgent management
│   │   └── broker_service.py         # IBKR API wrapper
│   └── dashboard/
│       ├── streamlit_entrypoint.py   # Dashboard entry point
│       ├── services/                 # Data/broker/pipeline service modules
│       └── screens/                  # Dashboard page modules
├── data/
│   ├── market/cleaned/
│   │   ├── prices/                   # Master price parquet (all ETFs, all dates)
│   │   └── targets/                  # Monthly momentum targets
│   └── broker/
│       ├── account/                  # Account snapshots (for NAV history)
│       ├── positions/                # Position snapshots
│       └── orders/                   # Order and fill records
├── artifacts/
│   └── runs/                         # Daily execution logs (JSON)
├── pyproject.toml                    # Poetry dependencies
└── AUTOTRADING_SETUP.md              # Quick reference guide
```

## Key Dependencies

- **ib_async:** Interactive Brokers API client
- **pandas/polars:** Data manipulation and analysis
- **streamlit:** Web-based monitoring dashboard
- **plotly:** Interactive charting (equity curves, allocation history)
- **apscheduler:** Job scheduling
- **pydantic:** Data validation

## Monitoring and Troubleshooting

**IB Gateway Connection Refused (port 4002)**
```bash
cd ../ibgw && docker compose up -d
```

**Data Gap Detection**
Check `Home` page for trading days behind. If gap detected, run:
```bash
python -m src.production.backfill_gaps --lookback "45 D"
```

**Missing Targets**
```bash
python -m src.production.daily_runner --skip-append --force-rebalance --dry-run
```

**Order Submission Locked**
Click "Unlock" in dashboard sidebar or run:
```bash
python -m src.production.daily_runner --force-rebalance
```

## References

Jegadeesh, N., & Titman, S. (1993). Returns to buying winners and selling losers: Implications for stock market efficiency. The Journal of Finance, 48(1), 65-91.

Novy-Marx, R., & Velikov, M. (2016). A taxonomy of anomalies and their trading costs. Financial Analysts Journal, 72(5), 1-33.

## License

Internal Use Only
