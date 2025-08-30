from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import pandas as pd

from watson.logger import get_logger
from watson.broker.broker import Broker,BrokerError
from watson.core.database import Database

import asyncio

logger = get_logger(__name__)


class PortfolioError(Exception):
    pass


class PortfolioManager:

    def __init__(self, broker: Broker):
        self.broker = broker
        self.database = Database()

    async def initialize(self):
        if not self.database.connect():
            logger.error("Failed to connect to database")
            raise RuntimeError("Database connection failed")

    async def info(self):
        """Get portfolio information."""
        account_info = await self.broker.get_account_info()
        logger.info(f"Account number: {account_info.account_number}")
        logger.info(f"Trading blocked : {account_info.trading_blocked}")
        logger.info(f"Account blocked: {account_info.account_blocked}")
        logger.info(f"Shorting allowed: {account_info.shorting_enabled}")
        logger.info(f"Buying power: {account_info.buying_power}")
        logger.info(f"Option buying power: {account_info.options_buying_power}")
        logger.info(f"Cash: {account_info.cash}")
        logger.info(f"Portfolio value: {account_info.portfolio_value}")
        logger.info(f"Pending transfer in: {account_info.pending_transfer_in}")
        return account_info

    async def update(self):
        """Check for filled take profit and stop loss orders and update the database."""

        account_info = await self.info()

        logger.info("Updating portfolio...")
        open_trades = self.get_open_trades()
        for symbol, row in open_trades.iterrows():
            try:
                order = await self.broker.get_order_by_id(row['take_profit_order_id'])
                if order.status == 'filled':
                    logger.info(f"Take profit order {order.id} for {symbol} was filled")
                    self.update_trade(row['id'], {
                        'status': 'closed',
                        'exit_at': order.filled_at,
                        'exit_qty': order.filled_qty,
                        'exit_price': order.filled_avg_price,
                        'exit_reason': 'TakeProfit',
                        'exit_order_id': str(order.id)
                    })
            except Exception as e:
                logger.error(f"Error updating portfolio: {e}")
        
        return open_trades

    async def open_trades(self, signals: pd.DataFrame):
        """Open a trade."""
        for symbol, row in signals.iterrows():
            try:
                order = await self.broker.market_order(symbol, row['quantity'], side='buy')
            except BrokerError as e:
                raise PortfolioError(f"Error opening trade: {e}")
            
            if order:
                logger.info(f"Bought {order.filled_qty} shares of {order.symbol}@{order.filled_avg_price}")
                trade_id = await self.database.add_trade(
                    symbol=symbol,
                    filled_at=order.filled_at,
                    filled_qty=order.filled_qty,
                    filled_price=order.filled_avg_price,
                    side=order.side,
                    entry_order_id=str(order.id),
                    status='open'
                )
                # TODO: Add Take Profit and Stop Loss to Signals
                limit_order = await self.broker.set_take_profit(order)
                if limit_order:
                    logger.info(f"Set take profit for {symbol} at {limit_order.limit_price}")
                    self.update_trade(trade_id, {
                        'take_profit': limit_order.limit_price,
                        'take_profit_order_id': str(limit_order.id)
                        })

                # stop_loss_order = await self.broker.set_stop_loss(order)
                # if stop_loss_order:
                #     logger.info(f"Set stop loss for {symbol} at {stop_loss_order.limit_price}")
                #     self.update_trade(trade_id, {
                #         'stop_loss': stop_loss_order.limit_price,
                #         'stop_loss_order_id': str(stop_loss_order.id)
                #         })

    async def close_trades(self, signals: pd.DataFrame):
        """Close a trade."""
        for symbol, row in signals.iterrows():
            try:
                order = await self.broker.close_order(symbol, row['quantity'], take_profit_order_id=row['take_profit_order_id'])
            except BrokerError as e:
                raise PortfolioError(f"Error closing trade: {e}")
            
            if order:
                logger.info(f"Sold {order.filled_qty} shares of {order.symbol}@{order.filled_avg_price}")
                self.update_trade_exit(
                    row['id'],
                    order.filled_at,
                    order.filled_qty,
                    order.filled_avg_price,
                    row['reason'],
                    str(order.id)
                )








    # async def get_open_positions(self) -> List[Position]:
    #     """Get all open positions."""
    #     positions = await self.broker.get_all_positions()
    #     return [Position(position.symbol, position.qty, position.avg_entry_price, position.entry_date, position.sector) for position in positions]

    def add_trade(self, 
                  symbol: str,
                  filled_at: datetime,
                  filled_qty: float,
                  filled_price: float,
                  side: str,
                  entry_order_id: str,
                  status: str = 'open') -> Optional[int]:
        """
        Add a new trade to the database.
        
        Args:
            symbol: Stock symbol
            filled_at: When the trade was filled
            filled_qty: Quantity filled
            filled_price: Price at which it was filled
            side: 'buy' or 'sell'
            entry_order_id: Order ID from broker
            status: Trade status ('open', 'closed', 'cancelled')
            
        Returns:
            Trade ID if successful, None otherwise
        """
        try:
            trade_id = self.database.add_trade(
                symbol=symbol,
                filled_at=filled_at,
                filled_qty=filled_qty,
                filled_price=filled_price,
                side=side,
                entry_order_id=entry_order_id,
                status=status
            )
            
            if trade_id:
                logger.info(f"Trade added to database: {symbol} {filled_qty} @ {filled_price}")
            else:
                logger.error(f"Failed to add trade to database: {symbol}")
                
            return trade_id
            
        except Exception as e:
            logger.error(f"Error adding trade: {e}")
            return None

    def update_trade_exit(self,
                         trade_id: int,
                         exit_at: datetime,
                         exit_qty: float,
                         exit_price: float,
                         exit_reason: str,
                         exit_order_id: str,
                         status: str = 'closed') -> bool:
        """
        Update a trade with exit information.
        
        Args:
            trade_id: Database trade ID
            exit_at: When the trade was exited
            exit_qty: Quantity exited
            exit_price: Price at which it was exited
            exit_reason: Reason for exit ('take_profit', 'stop_loss', 'manual', etc.)
            exit_order_id: Exit order ID from broker
            status: New trade status (usually 'closed')
            
        Returns:
            True if successful, False otherwise
        """
        try:
            success = self.database.update_trade_exit(
                trade_id=trade_id,
                exit_at=exit_at,
                exit_qty=exit_qty,
                exit_price=exit_price,
                exit_reason=exit_reason,
                exit_order_id=exit_order_id,
                status=status
            )
            
            if success:
                logger.info(f"Trade exit updated: ID {trade_id} exited @ {exit_price}")
            else:
                logger.error(f"Failed to update trade exit: ID {trade_id}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error updating trade exit: {e}")
            return False

    def get_trade_by_id(self, trade_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific trade by ID."""
        try:
            return self.database.get_trade_by_id(trade_id)
        except Exception as e:
            logger.error(f"Error getting trade by ID: {e}")
            return None

    def get_trade_by_order_id(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Get a trade by entry or exit order ID."""
        try:
            return self.database.get_trade_by_order_id(order_id)
        except Exception as e:
            logger.error(f"Error getting trade by order ID: {e}")
            return None

    def get_open_trades(self) -> pd.DataFrame:
        """Get all open trades from database as a pandas DataFrame with symbol as index."""
        try:
            return self.database.get_open_trades()
        except Exception as e:
            logger.error(f"Error getting open trades: {e}")
            return pd.DataFrame()

    def get_closed_trades(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get closed trades from database."""
        try:
            return self.database.get_closed_trades(limit=limit)
        except Exception as e:
            logger.error(f"Error getting closed trades: {e}")
            return []

    def get_trades_by_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """Get all trades for a specific symbol."""
        try:
            return self.database.get_trades_by_symbol(symbol)
        except Exception as e:
            logger.error(f"Error getting trades by symbol: {e}")
            return []

    def update_trade_status(self, trade_id: int, status: str) -> bool:
        """Update trade status."""
        try:
            return self.database.update_trade_status(trade_id, status)
        except Exception as e:
            logger.error(f"Error updating trade status: {e}")
            return False

    def update_take_profit_order_id(self, trade_id: int, take_profit_order_id: str) -> bool:
        """Update take profit order ID for a trade."""
        try:
            return self.database.update_take_profit_order_id(trade_id, take_profit_order_id)
        except Exception as e:
            logger.error(f"Error updating take profit order ID: {e}")
            return False

    def update_stop_loss_order_id(self, trade_id: int, stop_loss_order_id: str) -> bool:
        """Update stop loss order ID for a trade."""
        try:
            return self.database.update_stop_loss_order_id(trade_id, stop_loss_order_id)
        except Exception as e:
            logger.error(f"Error updating stop loss order ID: {e}")
            return False

    def update_take_profit_price(self, trade_id: int, take_profit: float) -> bool:
        """Update take profit price for a trade."""
        try:
            return self.database.update_take_profit_price(trade_id, take_profit)
        except Exception as e:
            logger.error(f"Error updating take profit price: {e}")
            return False

    def update_stop_loss_price(self, trade_id: int, stop_loss: float) -> bool:
        """Update stop loss price for a trade."""
        try:
            return self.database.update_stop_loss_price(trade_id, stop_loss)
        except Exception as e:
            logger.error(f"Error updating stop loss price: {e}")
            return False

    def update_trade(self, trade_id: int, updates: Dict[str, Any]) -> bool:
        """
        General method to update any trade fields.
        
        Args:
            trade_id: The ID of the trade to update
            updates: Dictionary of field names and values to update
            
        Returns:
            True if successful, False otherwise
            
        Allowed fields:
            - status
            - take_profit
            - stop_loss
            - take_profit_order_id
            - stop_loss_order_id
            - exit_at
            - exit_qty
            - exit_price
            - exit_reason
            - exit_order_id
        """
        try:
            return self.database.update_trade(trade_id, updates)
        except Exception as e:
            logger.error(f"Error updating trade: {e}")
            return False

    def get_trade_statistics(self) -> Dict[str, Any]:
        """Get trade statistics."""
        try:
            return self.database.get_trade_statistics()
        except Exception as e:
            logger.error(f"Error getting trade statistics: {e}")
            return {}

    async def record_entry_trade(self, order_data: Dict[str, Any]) -> Optional[int]:
        """
        Record an entry trade from broker order data.
        
        Args:
            order_data: Dictionary containing order information from broker
            
        Returns:
            Trade ID if successful, None otherwise
        """
        try:
            # Extract data from order
            symbol = order_data.get('symbol')
            filled_qty = order_data.get('filled_qty', 0)
            filled_price = order_data.get('filled_avg_price', 0)
            side = order_data.get('side', 'buy')
            order_id = order_data.get('id')
            filled_at = order_data.get('filled_at')
            
            if not all([symbol, filled_qty, filled_price, order_id, filled_at]):
                logger.error(f"Incomplete order data for trade recording: {order_data}")
                return None
            
            # Add trade to database
            trade_id = self.add_trade(
                symbol=symbol,
                filled_at=filled_at,
                filled_qty=filled_qty,
                filled_price=filled_price,
                side=side,
                entry_order_id=order_id,
                status='open'
            )
            
            return trade_id
            
        except Exception as e:
            logger.error(f"Error recording entry trade: {e}")
            return None

    async def record_exit_trade(self, order_data: Dict[str, Any], exit_reason: str = 'manual') -> bool:
        """
        Record an exit trade from broker order data.
        
        Args:
            order_data: Dictionary containing order information from broker
            exit_reason: Reason for exit ('take_profit', 'stop_loss', 'manual', etc.)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract data from order
            symbol = order_data.get('symbol')
            filled_qty = order_data.get('filled_qty', 0)
            filled_price = order_data.get('filled_avg_price', 0)
            order_id = order_data.get('id')
            filled_at = order_data.get('filled_at')
            
            if not all([symbol, filled_qty, filled_price, order_id, filled_at]):
                logger.error(f"Incomplete order data for exit recording: {order_data}")
                return False
            
            # Find the corresponding open trade
            open_trades = self.get_open_trades()
            matching_trade = None
            
            for trade in open_trades:
                if trade['symbol'] == symbol and trade['status'] == 'open':
                    matching_trade = trade
                    break
            
            if not matching_trade:
                logger.error(f"No open trade found for symbol: {symbol}")
                return False
            
            # Update the trade with exit information
            success = self.update_trade_exit(
                trade_id=matching_trade['id'],
                exit_at=filled_at,
                exit_qty=filled_qty,
                exit_price=filled_price,
                exit_reason=exit_reason,
                exit_order_id=order_id,
                status='closed'
            )
            
            return success
            
        except Exception as e:
            logger.error(f"Error recording exit trade: {e}")
            return False

    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get portfolio summary including trade statistics."""
        try:
            # Get trade statistics
            trade_stats = self.get_trade_statistics()
            
            # Get open positions from broker
            open_positions = self.get_open_trades()
            
            # Calculate current portfolio value (you may need to implement this based on your broker)
            # For now, we'll use trade statistics
            summary = {
                'trade_statistics': trade_stats,
                'open_positions_count': len(open_positions),
                'open_positions': [
                    {
                        'symbol': symbol,
                        'quantity': pos['filled_qty'],
                        'entry_price': pos['filled_price'],
                        'entry_date': pos['filled_at'],
                    }
                    for symbol, pos in open_positions.iterrows()
                ]
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting portfolio summary: {e}")
            return {}

    def close_database(self):
        """Close database connection."""
        try:
            self.database.disconnect()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database: {e}")


class Position:
    """Represents a trading position."""
    
    def __init__(
        self,
        symbol: str,
        quantity: int,
        entry_price: float,
        entry_date: datetime,
        sector: Optional[str] = None
    ):
        self.symbol = symbol
        self.quantity = quantity
        self.entry_price = entry_price
        self.entry_date = entry_date
        self.sector = sector
        self.current_price = entry_price
        self.unrealized_pnl = 0.0
        self.realized_pnl = 0.0
        self.exit_price = None
        self.exit_date = None
        self.status = "open"  # open, closed
        
    def update_price(self, current_price: float):
        """Update current price and calculate unrealized P&L."""
        self.current_price = current_price
        self.unrealized_pnl = (current_price - self.entry_price) * self.quantity
        
    def close_position(self, exit_price: float, exit_date: datetime):
        """Close the position."""
        self.exit_price = exit_price
        self.exit_date = exit_date
        self.realized_pnl = (exit_price - self.entry_price) * self.quantity
        self.status = "closed"
        
    def get_return_pct(self) -> float:
        """Get return percentage."""
        if self.status == "closed":
            return (self.exit_price - self.entry_price) / self.entry_price
        else:
            return (self.current_price - self.entry_price) / self.entry_price
    
    def get_holding_days(self) -> int:
        """Get number of days position has been held."""
        if self.status == "closed":
            return (self.exit_date - self.entry_date).days
        else:
            return (datetime.now() - self.entry_date).days

if __name__ == '__main__':

    async def main():
        pm = PortfolioManager(broker=None)
        await pm.initialize()
        await pm.add_trade(symbol='GOOGL', 
                                 filled_at=datetime.strptime('2025-08-20 19:39:04', '%Y-%m-%d %H:%M:%S'),
                                 filled_qty=24.0,
                                 filled_price=199.83,
                                 side='buy',
                                 entry_order_id='727597f0-3db1-421b-ab40-3c070011a0bc',
                                 status='open')

    asyncio.run(main())