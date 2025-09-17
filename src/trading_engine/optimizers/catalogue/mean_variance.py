from typing import Callable, Literal

import numpy as np
import polars as pl
from polars import DataFrame, LazyFrame


def _compute_log_returns(prices: DataFrame) -> DataFrame:
    """
    Compute per-ticker daily log returns from a wide price table.

    :param prices: Polars DataFrame with first column 'date' and remaining columns tickers
    :return: Polars DataFrame of log returns aligned to dates (first row per ticker becomes null then filled with 0)
    """
    tickers = [c for c in prices.columns if c != "date"]
    if not tickers:
        return pl.DataFrame({"date": []})

    lr = (
        prices.with_columns(
            [
                (pl.col(t).cast(pl.Float64) / pl.col(t).shift(1).cast(pl.Float64))
                .log()
                .alias(f"ret_{t}")
                for t in tickers
            ]
        )
        .select(["date", *[f"ret_{t}" for t in tickers]])
        .with_columns([pl.col(f"ret_{t}").fill_null(0.0) for t in tickers])
    )
    return lr


def _solve_regularized_mv(
    cov: np.ndarray, target_w: np.ndarray, lambda_risk: float
) -> np.ndarray:
    """
    Closed-form regularized MV: argmin_w 0.5 w^T Σ w + λ ||w - target||^2
    Solution: w = (Σ + 2λ I)^-1 (2λ target)
    """
    n = cov.shape[0]
    A = cov + 2.0 * lambda_risk * np.eye(n)
    b = 2.0 * lambda_risk * target_w
    try:
        w = np.linalg.solve(A, b)
    except np.linalg.LinAlgError:
        # Fallback to pseudo-inverse
        w = np.linalg.pinv(A) @ b
    return w


def _normalize_mu(mu: np.ndarray, mode: Literal["none", "zscore", "l2"]) -> np.ndarray:
    """
    Normalize cross-sectional signal vector.

    :param mu: raw signal vector
    :param mode: normalization mode
    :return: normalized signal
    """
    if mode == "none":
        return mu
    if mode == "zscore":
        mean = float(mu.mean())
        std = float(mu.std())
        if std == 0.0 or not (std == std):  # guard NaN
            return mu * 0.0
        return (mu - mean) / (std + 1e-12)
    if mode == "l2":
        norm = float(np.linalg.norm(mu))
        if norm == 0.0 or not (norm == norm):
            return mu * 0.0
        return mu / (norm + 1e-12)
    return mu


def MeanVarianceOptimizer(
    cov_window_days: int = 60,
    risk_aversion: float = 1.0,
    solve_mode: Literal["mu", "track"] = "mu",
    normalize_mu: Literal["none", "zscore", "l2"] = "none",
    ridge: float = 1e-3,
) -> Callable[[DataFrame, DataFrame, dict | None], LazyFrame]:
    """
    Mean-variance optimizer using asset covariance with two modes:

    - "track": Regularized tracking of aggregator weights (legacy, stable):
      minimize 0.5 w^T Σ w + λ ||w − w_target||^2 → w = (Σ + 2λ I)^{-1} (2λ w_target)
    - "mu": Use aggregator weights as returns forecast μ (normalized), solve w ∝ Σ^{-1} μ
      with ridge Σ̃ = Σ + ε I for numerical stability: w = Σ̃^{-1} μ

    :param cov_window_days: Rolling window length (days) for covariance estimation.
    :param risk_aversion: λ for tracking mode; ignored in mu mode.
    :param solve_mode: "mu" (default) or "track".
    :param normalize_mu: Cross-sectional normalization for μ (none|zscore|l2).
    :param ridge: ε added to covariance diagonal in mu mode.
    :return: Callable mapping (prices_df, desired_weights_df, config) → LazyFrame wide weights.
    """

    def run(
        prices_df: DataFrame, desired_weights_df: DataFrame, config: dict | None = None
    ) -> LazyFrame:
        if prices_df.is_empty() or desired_weights_df.is_empty():
            return pl.DataFrame({"date": []}).lazy()

        # Align columns and dates
        tickers = [c for c in desired_weights_df.columns if c != "date"]
        if not tickers:
            return pl.DataFrame({"date": []}).lazy()

        # Ensure prices has all tickers (missing -> null -> filled)
        present = [c for c in prices_df.columns if c in tickers]
        missing = [t for t in tickers if t not in prices_df.columns]
        if missing:
            prices_df = prices_df.with_columns([pl.lit(None).alias(t) for t in missing])
        prices_df = (
            prices_df.select(["date", *tickers])
            .fill_null(strategy="forward")
            .fill_null(strategy="backward")
        )

        # Compute log returns and rolling covariance
        returns_df = _compute_log_returns(prices_df)
        ret_cols = [c for c in returns_df.columns if c != "date"]

        # Join desired weights to returns dates
        dw = desired_weights_df
        if dw.schema.get("date") != pl.String:
            dw = dw.with_columns(pl.col("date").cast(pl.String))
        if returns_df.schema.get("date") != pl.String:
            returns_df = returns_df.with_columns(pl.col("date").cast(pl.String))

        joined = returns_df.join(dw, on="date", how="inner")
        if joined.is_empty():
            return pl.DataFrame({"date": []}).lazy()

        # Build rolling windows and solve per-date
        dates = joined.select("date").to_series().to_list()
        ret_mat = joined.select(ret_cols).to_numpy()
        target_mat = joined.select(tickers).to_numpy()

        n = len(tickers)
        out_weights: list[list[float]] = []
        out_dates: list[str] = []

        for i in range(len(dates)):
            if i + 1 < cov_window_days:
                # Not enough history: use target weights passthrough
                out_weights.append(target_mat[i].tolist())
                out_dates.append(dates[i])
                continue

            window = ret_mat[i + 1 - cov_window_days : i + 1]
            # Covariance of asset returns
            cov = np.cov(window, rowvar=False)

            if solve_mode == "track":
                target_w = target_mat[i]
                w = _solve_regularized_mv(cov, target_w, lambda_risk=risk_aversion)
            else:
                # mu-based: w = (Σ + ε I)^{-1} μ
                mu_raw = target_mat[i]
                mu = _normalize_mu(mu_raw, normalize_mu)
                cov_reg = cov + ridge * np.eye(cov.shape[0])
                try:
                    w = np.linalg.solve(cov_reg, mu)
                except np.linalg.LinAlgError:
                    w = np.linalg.pinv(cov_reg) @ mu

            out_weights.append(w.tolist())
            out_dates.append(dates[i])

        result_df = pl.DataFrame(
            {
                "date": out_dates,
                **{t: [row[j] for row in out_weights] for j, t in enumerate(tickers)},
            }
        )
        return result_df.lazy()

    return run
