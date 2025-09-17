from typing import Callable, Optional, Protocol

import numpy as np
import polars as pl
from polars import DataFrame, LazyFrame
from trading_engine.risk.catalogue.sample import SampleCovarianceWithRidge


class RiskModel(Protocol):
    def __call__(self, window_returns: np.ndarray) -> np.ndarray: ...


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


def MeanVarianceOptimizer(
    cov_window_days: int = 60,
    gamma: float = 1.0,
    lambda_te: float = 1.0,
    ridge: float = 1e-2,
    risk_model: Optional[RiskModel] = None,
) -> Callable[[DataFrame, DataFrame, dict | None], LazyFrame]:
    """
    Mean-variance optimizer with tracking error penalty.

    Objective per date t:
      minimize_w  0.5 w^T Σ w  -  γ μ^T w  +  λ ||w - w_target||^2

    Closed form solution:
      (Σ + 2λ I) w = γ μ + 2λ w_target  →  w = (Σ + 2λ I + ε I)^{-1} (γ μ + 2λ w_target)

    :param cov_window_days: Rolling window length (days) for covariance estimation.
    :param gamma: Weight on the expected-return term μ^T w (higher → more return-seeking).
                    Roughly, when λ_te = 0 this aligns with classic Markowitz via γ ≈ 1/λ_MPT,
                    where λ_MPT is the risk-aversion multiplying Σ.
    :param lambda_te: L2 tracking penalty weight to stay close to aggregator.
    :param ridge: Small ε added to covariance diagonal for stability.
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

        # Default risk model: sample covariance with ridge
        rm = (
            risk_model
            if risk_model is not None
            else SampleCovarianceWithRidge(ridge=ridge)
        )

        for i in range(len(dates)):
            if i + 1 < cov_window_days:
                # Not enough history: use target weights passthrough
                out_weights.append(target_mat[i].tolist())
                out_dates.append(dates[i])
                continue

            window = ret_mat[i + 1 - cov_window_days : i + 1]
            # Covariance of asset returns via pluggable risk model
            cov = rm(window)

            # μ and tracking target both come from aggregator weights (desired weights)
            mu = target_mat[i]
            target_w = target_mat[i]

            # (Σ + 2λ I) w = γ μ + 2λ w_target  (ridge handled inside risk model by default)
            A = cov + (2.0 * lambda_te) * np.eye(cov.shape[0])
            b = gamma * mu + 2.0 * lambda_te * target_w
            try:
                w = np.linalg.solve(A, b)
            except np.linalg.LinAlgError:
                w = np.linalg.pinv(A) @ b

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
