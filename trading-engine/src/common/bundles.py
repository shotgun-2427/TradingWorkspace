"""
bundles.py — Data-shape contracts shared between data ingest, model state,
and orchestration.

These dataclasses are the formal interface between layers of the pipeline.
Keeping them in ``common`` (rather than colocating with the data ingest)
means any package can import them without creating a dependency on the
heavy data layer.

There are two bundles:

* ``RawDataBundle``        — output of ``read_data(include_supplemental=True)``.
                             Wraps the lazy primary record stream plus an
                             optional supplemental stream (macro / sentiment
                             / non-ticker series).

* ``ModelStateBundle``     — output of ``create_model_state(return_bundle=True)``.
                             Bundles the per-ticker model_state DataFrame
                             alongside the wide-format supplemental_model_state
                             DataFrame so individual model runners can opt
                             into either or both via the registry's
                             ``input_mode`` field.

Both bundles intentionally store **eager** Polars DataFrames where the
pipeline has already collected, and **lazy** LazyFrames at the ingest
boundary. The orchestrator in ``trading_engine/core.py`` honours that
distinction.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import polars as pl


@dataclass(slots=True)
class RawDataBundle:
    """The raw data inputs the pipeline can consume.

    Attributes
    ----------
    raw_records : pl.LazyFrame
        Per-ticker OHLCV-style records. Required columns at minimum:
        ``date``, ``ticker``, ``adjusted_close_1d``. Optional rich columns
        like ``adjusted_high_1d`` / ``adjusted_low_1d`` enable NATR-family
        features.
    raw_supplemental_records : pl.LazyFrame, optional
        Non-ticker series stored long-format with columns
        ``record_timestamp``, ``series_id``, ``value``. Use this for
        macro indicators (VIX, CPI), sentiment series, options
        implied-vol surfaces, etc.
    """

    raw_records: pl.LazyFrame
    raw_supplemental_records: Optional[pl.LazyFrame] = None


@dataclass(slots=True)
class ModelStateBundle:
    """Computed feature panel ready for model runners.

    Attributes
    ----------
    model_state : pl.DataFrame
        Per-ticker, per-date feature table. Already filtered to the
        configured universe and date range with the appropriate lookback
        warmup applied. Columns vary by configured features.
    supplemental_model_state : pl.DataFrame
        Wide-format non-ticker features keyed by ``date``. May be empty
        when no supplemental records were ingested.

    A registry entry can opt into ``input_mode="bundle"`` to receive this
    object directly; legacy entries continue to receive a slim filtered
    ``LazyFrame`` and don't need to know about the supplemental side.
    """

    model_state: pl.DataFrame
    supplemental_model_state: pl.DataFrame
