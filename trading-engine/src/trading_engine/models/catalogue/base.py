"""
base.py — Uniform contract for model factories in the catalogue.

This file is a contract, not a base class. Every factory in
``trading_engine.models.catalogue`` follows the same shape so that the
orchestrator, tests, and the dashboard can treat them interchangeably.

The contract is intentionally brain-dead simple: a factory takes
parameters, returns a ``run_model`` callable, and that callable produces
a wide LazyFrame keyed by date with one weight column per traded ticker.

Adding a new model:
  1. Drop ``my_model.py`` in this directory exporting ``MyModel(...)``.
  2. The factory MUST return ``Callable[[LazyFrame], LazyFrame]`` *or*
     ``Callable[[ModelStateBundle], LazyFrame]`` (AMMA-style).
  3. The returned LazyFrame MUST have a ``date`` column and one
     ``Float64`` column per traded ticker, in {-1.0, 0.0, +1.0} or any
     bounded weight space the orchestrator clamps later.
  4. Register one or more named parameterizations in ``models/registry.py``.
"""
from __future__ import annotations

from typing import Callable, Protocol, Union, runtime_checkable

import polars as pl
from polars import LazyFrame

from common.bundles import ModelStateBundle

# ── Type aliases ─────────────────────────────────────────────────────────────

#: A model factory that consumes a flat LazyFrame (date, ticker, features).
#: Most catalogue entries use this shape.
LazyFrameRunner = Callable[[LazyFrame], LazyFrame]

#: A model factory that consumes the rich ModelStateBundle (model_state +
#: supplemental_model_state). AMMA-style models use this shape so they
#: can read precomputed features from the supplemental panel.
BundleRunner = Callable[[ModelStateBundle], LazyFrame]

#: Either runner is acceptable. The orchestrator inspects the registry's
#: ``input_mode`` field to decide which one to call.
ModelRunner = Union[LazyFrameRunner, BundleRunner]


@runtime_checkable
class ModelFactory(Protocol):
    """Structural type — anything callable that returns a ModelRunner.

    Used in tests as ``isinstance(factory, ModelFactory)`` to confirm a
    factory has the right shape before exercising it.
    """

    def __call__(self, *args, **kwargs) -> ModelRunner: ...


# ── Output validators ────────────────────────────────────────────────────────
# Used by tests and the orchestrator. NOT called from inside the model
# factories — keep models brain-dead simple. Validation happens at the
# boundary, not inside every model.

REQUIRED_OUTPUT_COLUMNS = {"date"}


def validate_weight_frame(
    weights: LazyFrame | pl.DataFrame,
    expected_tickers: list[str],
    *,
    allow_short: bool = True,
    weight_bound: float = 5.0,
    require_nonempty: bool = False,
) -> None:
    """Sanity-check a model's weight frame.

    Raises ``ValueError`` with a descriptive message on the first failure.
    Use this in tests and at the orchestrator boundary, NOT inside the
    factory itself.

    Checks:
      * has a ``date`` column
      * no duplicate column names
      * has every ticker as a Float64 column
      * no NaN / non-finite weights
      * weights inside ``[-weight_bound, weight_bound]`` (or
        ``[0, weight_bound]`` if ``allow_short=False``)
      * (optional) frame is non-empty (``require_nonempty=True``)
    """
    if not (isinstance(weight_bound, (int, float)) and weight_bound > 0):
        raise ValueError(f"weight_bound must be > 0, got {weight_bound!r}")
    if not isinstance(expected_tickers, (list, tuple)):
        raise TypeError(
            f"expected_tickers must be list/tuple, got {type(expected_tickers).__name__}"
        )

    df = weights.collect() if isinstance(weights, LazyFrame) else weights
    cols_list = list(df.columns)
    cols = set(cols_list)

    # Duplicate column detection (Polars allows them by default).
    if len(cols_list) != len(cols):
        seen: set[str] = set()
        dups = []
        for c in cols_list:
            if c in seen:
                dups.append(c)
            seen.add(c)
        raise ValueError(f"weights frame has duplicate columns: {dups}")

    missing = REQUIRED_OUTPUT_COLUMNS - cols
    if missing:
        raise ValueError(f"weights frame missing required columns: {sorted(missing)}")

    if require_nonempty and df.height == 0:
        raise ValueError("weights frame is empty but require_nonempty=True")

    for t in expected_tickers:
        if not isinstance(t, str) or not t:
            raise ValueError(f"expected ticker must be non-empty string, got {t!r}")
        if t not in cols:
            raise ValueError(f"weights frame missing ticker column: {t!r}")
        col = df.get_column(t)
        if col.dtype != pl.Float64:
            raise ValueError(
                f"ticker column {t!r} has dtype {col.dtype}, expected Float64"
            )
        n_total = col.len()
        if n_total == 0:
            # Empty column — nothing to check; bounds inapplicable.
            continue
        n_finite = int(col.is_finite().sum() or 0)
        if n_finite != n_total:
            raise ValueError(
                f"ticker column {t!r} has {n_total - n_finite} non-finite values "
                f"(NaN, +inf, -inf, or null)"
            )
        lo = -weight_bound if allow_short else 0.0
        hi = weight_bound
        col_min = col.min()
        col_max = col.max()
        if col_min is not None and col_min < lo:
            raise ValueError(
                f"ticker {t!r} min weight {col_min} below allowed lower bound {lo}"
            )
        if col_max is not None and col_max > hi:
            raise ValueError(
                f"ticker {t!r} max weight {col_max} above allowed upper bound {hi}"
            )


__all__ = [
    "ModelRunner",
    "LazyFrameRunner",
    "BundleRunner",
    "ModelFactory",
    "validate_weight_frame",
    "REQUIRED_OUTPUT_COLUMNS",
]
