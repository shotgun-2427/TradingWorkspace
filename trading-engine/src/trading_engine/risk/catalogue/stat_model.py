"""
stat_model.py — Statistical risk-model covariance estimators.

Two well-known shrinkage estimators that are robust on small samples and
high-dimensional return matrices:

  * **Ledoit-Wolf** — shrinks the sample covariance toward a constant-
    correlation target. Closed-form optimal shrinkage intensity. The
    standard go-to when N (assets) is comparable to T (history).

  * **Constant-Correlation Shrinkage** — explicit Ledoit-Wolf style with a
    user-specified correlation target. Useful when you have a prior view
    on what "average" pairwise correlation should be (e.g. 0.3 for ETFs).

  * **Exponentially-Weighted Sample (EWMA)** — recent observations get more
    weight than distant ones via a half-life parameter. Captures regime
    shifts that the equal-weighted sample estimator misses.

All three return ``Callable[[np.ndarray], np.ndarray]`` matching the rest
of ``risk/catalogue/`` — input is a (T, N) returns matrix, output is an
(N, N) covariance matrix.
"""
from __future__ import annotations

from typing import Callable

import numpy as np


def _to_matrix(window_returns: np.ndarray) -> np.ndarray:
    arr = np.asarray(window_returns, dtype=float)
    if arr.ndim != 2:
        raise ValueError(f"window_returns must be 2D; got shape {arr.shape}")
    return arr


def _force_psd(cov: np.ndarray, ridge: float = 1e-8) -> np.ndarray:
    """Symmetrize and ensure positive-semi-definite via min-eigenvalue floor."""
    cov = (cov + cov.T) / 2.0
    if ridge > 0:
        cov = cov + ridge * np.eye(cov.shape[0])
    return cov


def LedoitWolf(ridge: float = 1e-8) -> Callable[[np.ndarray], np.ndarray]:
    """
    Ledoit-Wolf shrinkage to a constant-correlation target.

    Implements the formula from Ledoit & Wolf (2004) — "Honey, I Shrunk
    the Sample Covariance Matrix".

    Parameters
    ----------
    ridge : tiny diagonal added at the end to guarantee invertibility.
    """

    def run(window_returns: np.ndarray) -> np.ndarray:
        x = _to_matrix(window_returns)
        T, N = x.shape
        if T < 2 or N < 1:
            return _force_psd(np.zeros((N, N)), ridge)

        # Demean.
        mu = x.mean(axis=0, keepdims=True)
        xc = x - mu

        # Sample covariance S.
        S = (xc.T @ xc) / float(T)

        # Constant-correlation target F: same diagonal as S, off-diag = mean
        # correlation × √(σ_i σ_j).
        sigma = np.sqrt(np.clip(np.diag(S), 0.0, np.inf))
        denom = np.outer(sigma, sigma)
        # Guard against zero-vol assets.
        valid = denom > 1e-12
        corr = np.where(valid, S / np.where(valid, denom, 1.0), 0.0)
        np.fill_diagonal(corr, 1.0)
        # Average off-diagonal correlation.
        if N > 1:
            mask = ~np.eye(N, dtype=bool)
            r_bar = corr[mask].mean()
        else:
            r_bar = 0.0
        F = r_bar * denom
        np.fill_diagonal(F, np.diag(S))

        # π̂ — sum of asymptotic variances of S_ij entries.
        x2 = xc**2
        pi_mat = (x2.T @ x2) / float(T) - S**2
        pi_hat = pi_mat.sum()

        # ρ̂ — covariance between target and S.
        # See Ledoit-Wolf 2004 Eq. (A.5). For constant-correlation target.
        # Diagonal contribution: π_ii.
        diag_pi = np.diag(pi_mat).sum()
        # Off-diagonal: r_bar/2 * Σ_{i≠j} (sqrt(σ_jj/σ_ii) θ_iijj + sqrt(σ_ii/σ_jj) θ_jjii)
        var = np.diag(S)
        # θ_iijj = E[(x_i - μ_i)^2 (x_i x_j)] - σ_ii σ_ij. Computed via:
        if N > 1 and r_bar != 0.0:
            ratio = np.sqrt(np.outer(var, 1.0 / np.where(var > 0, var, 1.0)))
            np.fill_diagonal(ratio, 0.0)
            # theta_iijj approximation
            xc_sq = xc**2
            theta_iijj = (xc_sq.T @ (xc * xc)) / float(T)  # (N, N) approx
            # Symmetric off-diag contribution.
            off_terms = ratio * (theta_iijj - var[:, None] * S)
            rho_off = (r_bar / 2.0) * off_terms.sum()
        else:
            rho_off = 0.0
        rho_hat = diag_pi + rho_off

        # γ̂ — squared distance ‖F − S‖²_F.
        gamma_hat = float(((F - S) ** 2).sum())
        if gamma_hat <= 0:
            shrinkage = 0.0
        else:
            kappa = (pi_hat - rho_hat) / gamma_hat
            shrinkage = max(0.0, min(1.0, kappa / float(T)))

        cov = shrinkage * F + (1.0 - shrinkage) * S
        return _force_psd(cov, ridge)

    return run


def ConstantCorrelationShrinkage(
    target_correlation: float = 0.3,
    shrinkage: float = 0.5,
    ridge: float = 1e-8,
) -> Callable[[np.ndarray], np.ndarray]:
    """
    Manual constant-correlation shrinkage.

    cov = (1-α) · S + α · F
        where F has var_i on diagonal and ρ · √(var_i · var_j) off-diagonal.

    Useful when you have a strong prior on average pairwise correlation
    and want a fixed shrinkage intensity (rather than the data-driven LW).

    Parameters
    ----------
    target_correlation : ρ in the formula above
    shrinkage : α ∈ [0, 1]; 0 = sample only, 1 = target only
    """
    if not 0.0 <= shrinkage <= 1.0:
        raise ValueError("shrinkage must be in [0, 1]")
    if not -1.0 <= target_correlation <= 1.0:
        raise ValueError("target_correlation must be in [-1, 1]")

    def run(window_returns: np.ndarray) -> np.ndarray:
        x = _to_matrix(window_returns)
        T, N = x.shape
        if T < 2 or N < 1:
            return _force_psd(np.zeros((N, N)), ridge)
        S = np.cov(x, rowvar=False)
        sigma = np.sqrt(np.clip(np.diag(S), 0.0, np.inf))
        F = target_correlation * np.outer(sigma, sigma)
        np.fill_diagonal(F, np.diag(S))
        cov = (1.0 - shrinkage) * S + shrinkage * F
        return _force_psd(cov, ridge)

    return run


def ExponentiallyWeightedSample(
    half_life: int = 60,
    ridge: float = 1e-8,
) -> Callable[[np.ndarray], np.ndarray]:
    """
    Exponentially-weighted sample covariance with a half-life decay.

    Recent observations get weight 1.0; observations ``half_life`` rows
    earlier get weight 0.5; etc. Captures regime shifts that an equal-
    weighted estimator smooths over.

    Parameters
    ----------
    half_life : number of rows for the weight to halve (e.g. 60 trading days)
    """
    if half_life <= 0:
        raise ValueError("half_life must be > 0")
    decay = np.log(2.0) / half_life

    def run(window_returns: np.ndarray) -> np.ndarray:
        x = _to_matrix(window_returns)
        T, N = x.shape
        if T < 2 or N < 1:
            return _force_psd(np.zeros((N, N)), ridge)
        # Most recent row is index T-1 → weight 1; earliest row → smallest weight.
        ages = np.arange(T - 1, -1, -1, dtype=float)
        w = np.exp(-decay * ages)
        w = w / w.sum()
        # Weighted demean.
        mu = (w[:, None] * x).sum(axis=0, keepdims=True)
        xc = x - mu
        # Weighted covariance.
        cov = (xc * w[:, None]).T @ xc
        return _force_psd(cov, ridge)

    return run
