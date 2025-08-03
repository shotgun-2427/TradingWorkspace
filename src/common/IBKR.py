from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from ib_async import IB, Order, Stock


@dataclass
class AccountData:
    nav: float
    positions: dict


class IBKR:
    def __init__(self):
        self.ib: Optional[IB] = None
        self.account = AccountData(-1, {})

    @classmethod
    async def create(cls, hostname: str, port: int, client_id: int = 1):
        self = cls()
        self.ib = IB()
        await self.ib.connectAsync(hostname, port, clientId=client_id)
        await self.__fetch_initial_data()
        return self

    async def __fetch_initial_data(self):
        await self.__fetch_nav()
        # await self.__fetch_positions()

    async def __fetch_nav(self):
        for v in self.ib.accountValues():
            if v.tag == "NetLiquidation":
                self.account.nav = float(v.value)
                break

    # async def __fetch_positions(self):
    #     for p in self.ib.positions():
    #         self.account.positions[p.contract.symbol] = p

    def get_nav(self) -> float:
        return self.account.nav

    def get_position(self, ticker: str):
        sym = ticker.split("-")[0]
        return self.account.positions.get(sym, 0)

    async def get_latest_positions(self):
        return self.ib.positions()

    async def get_open_trades(self):
        return self.ib.openTrades()  # local cache; no await

    async def get_contract(self, ticker: str):
        sym = ticker.split("-")[0]
        c = Stock(sym, "SMART", "USD")
        await self.ib.qualifyContractsAsync(c)
        return c

    def generate_order_ref(self, ticker: str) -> str:
        return f"{datetime.now().strftime('%Y%m%d')}.{ticker}"

    def construct_order(self, ticker: str, qty: int):
        if qty == 0:
            return None
        o = Order()
        o.action = "BUY" if qty > 0 else "SELL"
        o.totalQuantity = abs(qty)
        o.orderType = "MOC"  # ensure venue supports MOC; SMART often doesn’t
        o.orderRef = self.generate_order_ref(ticker)
        return o

    def cleanup(self):
        self.ib.disconnect()
