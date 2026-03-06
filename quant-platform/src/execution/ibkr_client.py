from __future__ import annotations

from ib_insync import IB, MarketOrder, Stock


class IBKRClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 7497, client_id: int = 1) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib = IB()

    def connect_to_tws(self) -> bool:
        self.ib.connect(host=self.host, port=self.port, clientId=self.client_id)
        return self.ib.isConnected()

    def place_order(self, symbol: str, quantity: float, side: str):
        action = "BUY" if side.upper() == "BUY" else "SELL"
        contract = Stock(symbol, "SMART", "USD")
        order = MarketOrder(action, quantity)
        return self.ib.placeOrder(contract, order)

    def get_positions(self):
        return self.ib.positions()

    def get_account_summary(self):
        return self.ib.accountSummary()
