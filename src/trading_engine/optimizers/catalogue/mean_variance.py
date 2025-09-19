from typing import Callable, Optional, Protocol, List, Tuple

import numpy as np
import polars as pl
from polars import DataFrame, LazyFrame


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


def _ensure_date_string(df: DataFrame) -> DataFrame:
    """
    Ensure the 'date' column is string-typed for reliable joins.

    :param df: Input DataFrame
    :return: DataFrame with 'date' as pl.String
    """
    if df.schema.get("date") != pl.String:
        return df.with_columns(pl.col("date").cast(pl.String))
    return df


def _align_prices_to_tickers(prices_df: DataFrame, tickers: List[str]) -> DataFrame:
    """
    Ensure prices table includes all tickers, forward/backward filling nulls.

    :param prices_df: Input wide prices DataFrame
    :param tickers: Target tickers to include
    :return: Prices DataFrame aligned to tickers
    """
    present = [c for c in prices_df.columns if c in tickers]
    missing = [t for t in tickers if t not in prices_df.columns]
    if missing:
        prices_df = prices_df.with_columns([pl.lit(None).alias(t) for t in missing])
    prices_df = (
        prices_df.select(["date", *tickers])
        .fill_null(strategy="forward")
        .fill_null(strategy="backward")
    )
    return prices_df


def _prepare_joined(
    prices_df: DataFrame, desired_weights_df: DataFrame
) -> Tuple[List[str], DataFrame]:
    """
    Align prices to desired weights tickers, compute returns, and inner-join by date.

    :param prices_df: Wide prices table with columns ['date', <tickers...>]
    :param desired_weights_df: Desired weights wide table with ['date', <tickers...>]
    :return: (tickers, joined_df) where joined has returns and target weights
    """
    tickers = [c for c in desired_weights_df.columns if c != "date"]
    if not tickers:
        return [], pl.DataFrame({"date": []})

    prices_df = _align_prices_to_tickers(prices_df, tickers)

    returns_df = _compute_log_returns(prices_df)
    returns_df = _ensure_date_string(returns_df)
    dw = _ensure_date_string(desired_weights_df)

    joined = returns_df.join(dw, on="date", how="inner")
    return tickers, joined


def _build_matrices(
    joined: DataFrame, tickers: List[str]
) -> Tuple[List[str], np.ndarray, np.ndarray, List[str]]:
    """
    Build numpy matrices needed for rolling optimization.

    :param joined: Joined DataFrame of returns and target weights
    :param tickers: List of asset tickers
    :return: (ret_cols, ret_mat, target_mat, dates)
    """
    ret_cols: List[str] = [c for c in joined.columns if c.startswith("ret_")]
    dates: List[str] = joined.select("date").to_series().to_list()
    ret_mat: np.ndarray = joined.select(ret_cols).to_numpy()
    target_mat: np.ndarray = joined.select(tickers).to_numpy()
    return ret_cols, ret_mat, target_mat, dates


def _solve_mv(
    cov: np.ndarray,
    mu: np.ndarray,
    target_w: np.ndarray,
    gamma: float,
    lambda_te: float,
    kappa: float,
) -> np.ndarray:
    """
    Solve the mean-variance tracking-error objective:

    (κ Σ + 2λ I) w = γ μ + 2λ w_target

    :param cov: Covariance matrix Σ (N x N)
    :param mu: Expected returns vector μ (N,)
    :param target_w: Tracking target weights (N,)
    :param gamma: Return-seeking weight γ
    :param lambda_te: Tracking-error weight λ (L2)
    :param kappa: Risk scaling κ applied to Σ
    :return: Solution vector w (N,)
    """
    A = (kappa * cov) + (2.0 * lambda_te) * np.eye(cov.shape[0])
    b = gamma * mu + 2.0 * lambda_te * target_w
    try:
        return np.linalg.solve(A, b)
    except np.linalg.LinAlgError:
        return np.linalg.pinv(A) @ b


def _rolling_optimize(
    dates: List[str],
    ret_mat: np.ndarray,
    target_mat: np.ndarray,
    cov_window_days: int,
    risk_model: RiskModel,
    gamma: float,
    lambda_te: float,
    kappa: float,
) -> Tuple[List[str], List[List[float]]]:
    """
    Compute rolling mean-variance optimized weights over dates.

    :param dates: Date index
    :param ret_mat: Matrix of asset returns (T, N)
    :param target_mat: Desired weights per date (T, N)
    :param cov_window_days: Rolling window for covariance estimation
    :param risk_model: Callable mapping window returns -> covariance
    :param gamma: Return-seeking weight
    :param lambda_te: Tracking-error weight
    :param kappa: Risk scaling
    :return: (out_dates, out_weights)
    """
    out_weights: List[List[float]] = []
    out_dates: List[str] = []

    for i in range(len(dates)):
        if i + 1 < cov_window_days:
            # Not enough history: use target weights passthrough
            out_weights.append(target_mat[i].tolist())
            out_dates.append(dates[i])
            continue

        window = ret_mat[i + 1 - cov_window_days : i + 1]
        cov = risk_model(window)
        mu = target_mat[i]
        target_w = target_mat[i]

        w = _solve_mv(
            cov=cov,
            mu=mu,
            target_w=target_w,
            gamma=gamma,
            lambda_te=lambda_te,
            kappa=kappa,
        )
        out_weights.append(w.tolist())
        out_dates.append(dates[i])

    return out_dates, out_weights


def MeanVarianceOptimizer(
    cov_window_days: int = 60,
    gamma: float = 1.0,
    lambda_te: float = 1.0,
    risk_model: Optional[RiskModel] = None,
    kappa: float = 1.0,
) -> Callable[[DataFrame, DataFrame, dict | None], LazyFrame]:
    """
    Mean-variance optimizer with tracking error penalty.

    Objective per date t:
      minimize_w  0.5 w^T Σ w  -  γ μ^T w  +  λ ||w - w_target||^2

    Closed form solution:
      (Σ + 2λ I) w = γ μ + 2λ w_target  →  w = (Σ + 2λ I + ε I)^{-1} (γ μ + 2λ w_target)

    :param cov_window_days: Rolling window length (days) for covariance Σ_assets estimation.
    :param gamma: Weight on the expected-return term μ_assets^T w (higher → more return-seeking).
                  When λ_te = 0, this aligns with classic Markowitz where γ rescales μ.
    :param lambda_te: L2 tracking penalty weight to stay close to desired weights w_target.
    :param risk_model: Callable(window_returns)->covariance; must be provided by the caller.
    :param kappa: Risk aversion scaling on Σ_assets (κ Σ). κ=1.0 preserves current behavior.
    :return: Callable mapping (prices_df, desired_weights_df, config) → LazyFrame wide weights.
    """

    def run(
        prices_df: DataFrame, desired_weights_df: DataFrame, config: dict | None = None
    ) -> LazyFrame:
        if prices_df.is_empty() or desired_weights_df.is_empty():
            return pl.DataFrame({"date": []}).lazy()

        if risk_model is None:
            raise ValueError(
                "risk_model must be provided: a callable(window_returns)->covariance"
            )

        tickers, joined = _prepare_joined(
            prices_df=prices_df, desired_weights_df=desired_weights_df
        )
        if not tickers or joined.is_empty():
            return pl.DataFrame({"date": []}).lazy()

        ret_cols, ret_mat, target_mat, dates = _build_matrices(joined, tickers)
        out_dates, out_weights = _rolling_optimize(
            dates=dates,
            ret_mat=ret_mat,
            target_mat=target_mat,
            cov_window_days=cov_window_days,
            risk_model=risk_model,
            gamma=gamma,
            lambda_te=lambda_te,
            kappa=kappa,
        )

        result_df = pl.DataFrame(
            {
                "date": out_dates,
                **{t: [row[j] for row in out_weights] for j, t in enumerate(tickers)},
            }
        )
        return result_df.lazy()

    return run
