"""Unit tests for src/storage/*.

The repo modules in ``src/storage/`` are placeholders (empty files) that
will be filled when the postgres-backed persistence layer is enabled.
For now this test pins down the contract every repo will follow so we
catch drift the moment one of them gets implemented.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


REPO_MODULES = [
    "src.storage.fills_repo",
    "src.storage.market_data_repo",
    "src.storage.orders_repo",
    "src.storage.positions_repo",
    "src.storage.run_log_repo",
    "src.storage.signals_repo",
    "src.storage.targets_repo",
]


@pytest.mark.parametrize("modname", REPO_MODULES)
def test_repo_module_importable(modname: str):
    """Every storage repo must be importable, even if empty."""
    importlib.import_module(modname)


def test_storage_package_importable():
    importlib.import_module("src.storage")


@pytest.mark.parametrize("modname", REPO_MODULES)
def test_repo_module_exposes_no_unsafe_globals(modname: str):
    """No accidental top-level execution that would touch a real DB."""
    mod = importlib.import_module(modname)
    forbidden = {"connect", "execute", "drop_all"}
    leaked = forbidden & set(vars(mod).keys())
    assert not leaked, (
        f"{modname} exposes top-level callables {leaked}; "
        "these should be on a class, not the module global."
    )
