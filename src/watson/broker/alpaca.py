import asyncio
from typing import Dict, List, Any
from datetime import datetime
import pandas as pd

from alpaca.trading.client import TradingClient
from alpaca.trading.models import TradeAccount, Order
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest, GetOptionContractsRequest, ClosePositionRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderStatus, AssetStatus, ContractType
from alpaca.data.historical import StockHistoricalDataClient, OptionHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest, StockLatestBarRequest, OptionSnapshotRequest, OptionChainRequest
from alpaca.data.enums import OptionsFeed

from watson.settings import ALPACA_CONFIG
from .broker import Broker, BrokerError
from watson.logger import get_logger

logger = get_logger(__name__)


class Alpaca(Broker):

    def __init__(self, paper: bool = True, dev_mode=True):
        super().__init__(paper)
        self.dev_mode = dev_mode
        self.trade_client = TradingClient(ALPACA_CONFIG["api_key"], ALPACA_CONFIG["secret_key"], paper=paper)
        self.stock_data_client = StockHistoricalDataClient(ALPACA_CONFIG["api_key"], ALPACA_CONFIG["secret_key"])
        self.option_data_client = OptionHistoricalDataClient(ALPACA_CONFIG["api_key"], ALPACA_CONFIG["secret_key"])

        self.account:TradeAccount = None

    async def initialize(self):

        self.account = await self.get_account_info()

        if self.account.trading_blocked or self.account.account_blocked:
            raise BrokerError(f"{self.__class__.__name__}: Trading is blocked")

        clock = self.trade_client.get_clock()
        if not clock.is_open and not self.dev_mode:
            raise BrokerError(f"{self.__class__.__name__}: Market is closed")

    async def get_account_info(self) -> Dict[str, Any]:
        """Get account information and portfolio value."""
        return self.trade_client.get_account()

    async def get_all_positions(self) -> List[Dict[str, Any]]:
        return self.trade_client.get_all_positions()

    async def get_order_by_id(self, id) -> Order:
        return self.trade_client.get_order_by_id(id)

    async def get_stock_latest_quote(self, symbol: str) -> float:
        req = StockLatestQuoteRequest(symbol_or_symbols=symbol)
        return self.stock_data_client.get_stock_latest_quote(req)

    async def get_stock_latest_bar(self, symbol: str) -> float:
        req = StockLatestBarRequest(symbol_or_symbols=symbol)
        return self.stock_data_client.get_stock_latest_bar(req)

    async def wait_for_order_status(self, order: Order, status: OrderStatus) -> bool:
        while status != order.status:
            logger.debug(f"Checking order status for {order.id} - {order.status}")
            await asyncio.sleep(0.1)
            order = self.trade_client.get_order_by_id(order.id)
            if order.status == status:
                logger.info(f"Order {order.id} {status}")
                return True
            else:
                logger.debug(f"Order {order.id} not {status}")
        return False

    async def market_order(self, symbol: str, qty: float, side: str = 'buy', time_in_force: TimeInForce = TimeInForce.DAY) -> Order:
        market_order_data = MarketOrderRequest(
                    symbol=symbol,
                    qty=qty,
                    side=OrderSide.BUY if side == 'buy' else OrderSide.SELL,
                    time_in_force=time_in_force
                    )
        try:
            market_order = self.trade_client.submit_order(
                order_data=market_order_data
            )
        except Exception as e:
            raise BrokerError(f"Failed to submit market order for {symbol}: {e}")

        if await self.wait_for_order_status(market_order, OrderStatus.FILLED):
            return self.trade_client.get_order_by_id(market_order.id)
        else:
            logger.error(f"Failed to buy {symbol}")
            return None

    async def close_order(self, symbol: str, qty: int, take_profit_order_id: str = None) -> Order:
        # Cancel all open orders for the symbol
        try:
            if take_profit_order_id:
                self.trade_client.cancel_order_by_id(take_profit_order_id)
        except Exception as e:
            raise BrokerError(f"Failed to cancel take profit order for {symbol}: {e}")

        req = ClosePositionRequest(
            qty=str(qty),
        )
        try:
            close_order = self.trade_client.close_position(symbol, close_options=req)
        except Exception as e:
            raise BrokerError(f"Failed to close position for {symbol}: {e}")

        if await self.wait_for_order_status(close_order, OrderStatus.FILLED):
            return self.trade_client.get_order_by_id(close_order.id)
        else:
            logger.error(f"Failed to buy {symbol}")
            return None

    async def set_take_profit(self, order: Order) -> Order:
        limit_order_data = LimitOrderRequest(
                    symbol=order.symbol,
                    limit_price=f"{float(order.filled_avg_price) * 1.02:.2f}",
                    qty=order.filled_qty,
                    side=OrderSide.SELL if order.side == OrderSide.BUY else OrderSide.BUY,
                    time_in_force=TimeInForce.GTC
                   )

        limit_order = self.trade_client.submit_order(
                        order_data=limit_order_data
                    )

        if await self.wait_for_order_status(limit_order, OrderStatus.NEW):
            return self.trade_client.get_order_by_id(limit_order.id)
        else:
            logger.error(f"Failed to set take profit for {symbol}")
            return None