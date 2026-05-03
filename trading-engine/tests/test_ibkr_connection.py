from __future__ import annotations

import os

import pytest

try:
    from ib_async import IB
except ImportError:  # pragma: no cover
    from ib_insync import IB  # type: ignore


RUN_IBKR_TESTS = os.getenv("RUN_IBKR_TESTS") == "1"
IBKR_HOST = os.getenv("IBKR_HOST", "127.0.0.1")
IBKR_PORT = int(os.getenv("IBKR_PORT_PAPER", "4004"))
IBKR_CLIENT_ID = int(os.getenv("IBKR_CLIENT_ID_PAPER", "991"))
IBKR_TIMEOUT = float(os.getenv("IBKR_TIMEOUT", "10"))


@pytest.mark.skipif(not RUN_IBKR_TESTS, reason="Set RUN_IBKR_TESTS=1 to run IBKR integration smoke tests.")
def test_ibkr_paper_connection_smoke() -> None:
    ib = IB()

    try:
        connected = ib.connect(
            IBKR_HOST,
            IBKR_PORT,
            clientId=IBKR_CLIENT_ID,
            timeout=IBKR_TIMEOUT,
            readonly=True,
        )
        assert connected
        assert ib.isConnected()

        managed_accounts = ib.managedAccounts()
        assert isinstance(managed_accounts, list)
        assert len(managed_accounts) >= 1

        account_values = ib.accountValues()
        assert isinstance(account_values, list)
        assert len(account_values) > 0

        positions = ib.positions()
        assert isinstance(positions, list)

    finally:
        if ib.isConnected():
            ib.disconnect()