import os
import pandas as pd
import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from watson.settings import DATABASE_CONFIG
from watson.logger import get_logger

logger = get_logger(__name__)

# Create declarative base
Base = declarative_base()

class Trade(Base):
    """SQLAlchemy model for trades table."""
    __tablename__ = 'trades'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    status = Column(String(20), nullable=False, default='open')  # open, closed, cancelled
    filled_at = Column(DateTime, nullable=False)
    filled_qty = Column(Float, nullable=False)
    filled_price = Column(Float, nullable=False)
    side = Column(String(10), nullable=False)  # buy, sell
    entry_order_id = Column(String(100), nullable=False)
    take_profit = Column(Float, nullable=True)
    stop_loss = Column(Float, nullable=True)
    take_profit_order_id = Column(String(100), nullable=True)
    stop_loss_order_id = Column(String(100), nullable=True)
    exit_at = Column(DateTime, nullable=True)
    exit_qty = Column(Float, nullable=True)
    exit_price = Column(Float, nullable=True)
    exit_reason = Column(String(50), nullable=True)  # take_profit, stop_loss, manual, etc.
    exit_order_id = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.now(datetime.UTC))
    updated_at = Column(DateTime, default=datetime.datetime.now(datetime.UTC), onupdate=datetime.datetime.now(datetime.UTC))

class Database:
    """SQLAlchemy database manager for tracking trades."""
    
    def __init__(self):
        self.engine = None
        self.SessionLocal = None
        self.config = DATABASE_CONFIG
        
    def connect(self) -> bool:
        """Establish connection to database and create tables."""
        try:
            # Ensure data directory exists
            db_path = self.config["path"]
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
            # Create SQLite URL
            db_url = f"sqlite:///{db_path}"
            
            # Create engine
            self.engine = create_engine(
                db_url,
                echo=False,  # Set to True for SQL debugging
                connect_args={"check_same_thread": False}  # For SQLite
            )
            
            # Create session factory
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            
            # Create tables
            Base.metadata.create_all(bind=self.engine)
            
            logger.info(f"Connected to database: {db_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")
    
    def get_session(self) -> Session:
        """Get a new database session."""
        if not self.SessionLocal:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self.SessionLocal()
    
    async def add_trade(self, 
                  symbol: str,
                  filled_at: datetime.datetime,
                  filled_qty: float,
                  filled_price: float,
                  side: str,
                  entry_order_id: str,
                  status: str = 'open') -> Optional[int]:
        """Add a new trade to the database."""
        if not self.SessionLocal:
            logger.error("Database not connected")
            return None
            
        session = self.get_session()
        try:
            trade = Trade(
                symbol=symbol,
                status=status,
                filled_at=filled_at,
                filled_qty=filled_qty,
                filled_price=filled_price,
                side=side,
                entry_order_id=str(entry_order_id)
            )
            
            session.add(trade)
            session.commit()
            session.refresh(trade)
            
            logger.info(f"Trade added to database: {symbol} {filled_qty} @ {filled_price}")
            return trade.id
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error adding trade: {e}")
            return None
        finally:
            session.close()
    
    def update_trade_exit(self,
                         trade_id: int,
                         exit_at: datetime.datetime,
                         exit_qty: float,
                         exit_price: float,
                         exit_reason: str,
                         exit_order_id: str,
                         status: str = 'closed') -> bool:
        """Update a trade with exit information."""
        updates = {
            'exit_at': exit_at,
            'exit_qty': exit_qty,
            'exit_price': exit_price,
            'exit_reason': exit_reason,
            'exit_order_id': exit_order_id,
            'status': status
        }
        return self.update_trade(trade_id, updates)
    
    def get_trade_by_id(self, trade_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific trade by ID."""
        if not self.SessionLocal:
            logger.error("Database not connected")
            return None
            
        session = self.get_session()
        try:
            trade = session.query(Trade).filter(Trade.id == trade_id).first()
            if trade:
                return {
                    'id': trade.id,
                    'symbol': trade.symbol,
                    'status': trade.status,
                    'filled_at': trade.filled_at,
                    'filled_qty': trade.filled_qty,
                    'filled_price': trade.filled_price,
                    'side': trade.side,
                    'entry_order_id': trade.entry_order_id,
                    'take_profit': trade.take_profit,
                    'stop_loss': trade.stop_loss,
                    'take_profit_order_id': trade.take_profit_order_id,
                    'stop_loss_order_id': trade.stop_loss_order_id,
                    'exit_at': trade.exit_at,
                    'exit_qty': trade.exit_qty,
                    'exit_price': trade.exit_price,
                    'exit_reason': trade.exit_reason,
                    'exit_order_id': trade.exit_order_id,
                    'created_at': trade.created_at,
                    'updated_at': trade.updated_at
                }
            return None
            
        except SQLAlchemyError as e:
            logger.error(f"Error getting trade: {e}")
            return None
        finally:
            session.close()
    
    def get_trade_by_order_id(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Get a trade by entry or exit order ID."""
        if not self.SessionLocal:
            logger.error("Database not connected")
            return None
            
        session = self.get_session()
        try:
            trade = session.query(Trade).filter(
                (Trade.entry_order_id == order_id) | (Trade.exit_order_id == order_id)
            ).first()
            
            if trade:
                return {
                    'id': trade.id,
                    'symbol': trade.symbol,
                    'status': trade.status,
                    'filled_at': trade.filled_at,
                    'filled_qty': trade.filled_qty,
                    'filled_price': trade.filled_price,
                    'side': trade.side,
                    'entry_order_id': trade.entry_order_id,
                    'take_profit': trade.take_profit,
                    'stop_loss': trade.stop_loss,
                    'take_profit_order_id': trade.take_profit_order_id,
                    'stop_loss_order_id': trade.stop_loss_order_id,
                    'exit_at': trade.exit_at,
                    'exit_qty': trade.exit_qty,
                    'exit_price': trade.exit_price,
                    'exit_reason': trade.exit_reason,
                    'exit_order_id': trade.exit_order_id,
                    'created_at': trade.created_at,
                    'updated_at': trade.updated_at
                }
            return None
            
        except SQLAlchemyError as e:
            logger.error(f"Error getting trade by order ID: {e}")
            return None
        finally:
            session.close()
    
    def get_open_trades(self) -> pd.DataFrame:
        """Get all open trades as a pandas DataFrame with symbol as index."""
        if not self.SessionLocal:
            logger.error("Database not connected")
            return pd.DataFrame()
            
        session = self.get_session()
        try:
            trades = session.query(Trade).filter(Trade.status == 'open').all()
            
            if not trades:
                logger.info("No open trades found")
                return pd.DataFrame()
            
            # Prepare data for DataFrame
            data = []
            symbols = []
            
            for trade in trades:
                symbols.append(trade.symbol)
                data.append({
                    'id': trade.id,
                    'filled_at': trade.filled_at,
                    'filled_qty': trade.filled_qty,
                    'filled_price': trade.filled_price,
                    'side': trade.side,
                    'entry_order_id': trade.entry_order_id,
                    'take_profit': trade.take_profit,
                    'stop_loss': trade.stop_loss,
                    'take_profit_order_id': trade.take_profit_order_id,
                    'stop_loss_order_id': trade.stop_loss_order_id,
                    'exit_at': trade.exit_at,
                    'exit_qty': trade.exit_qty,
                    'exit_price': trade.exit_price,
                    'exit_reason': trade.exit_reason,
                    'exit_order_id': trade.exit_order_id,
                })
            
            # Create DataFrame with symbol as index
            df = pd.DataFrame(data, index=symbols)
            df.index.name = 'symbol'
            
            logger.info(f"Retrieved {len(df)} open trades from database")
            return df
            
        except SQLAlchemyError as e:
            logger.error(f"Error getting open trades from database: {e}")
            return pd.DataFrame()
        finally:
            session.close()
    
    def get_closed_trades(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get closed trades with optional limit."""
        if not self.SessionLocal:
            logger.error("Database not connected")
            return []
            
        session = self.get_session()
        try:
            trades = session.query(Trade).filter(
                Trade.status == 'closed'
            ).order_by(Trade.exit_at.desc()).limit(limit).all()
            
            result = []
            for trade in trades:
                result.append({
                    'id': trade.id,
                    'symbol': trade.symbol,
                    'status': trade.status,
                    'filled_at': trade.filled_at,
                    'filled_qty': trade.filled_qty,
                    'filled_price': trade.filled_price,
                    'side': trade.side,
                    'entry_order_id': trade.entry_order_id,
                    'take_profit': trade.take_profit,
                    'stop_loss': trade.stop_loss,
                    'take_profit_order_id': trade.take_profit_order_id,
                    'stop_loss_order_id': trade.stop_loss_order_id,
                    'exit_at': trade.exit_at,
                    'exit_qty': trade.exit_qty,
                    'exit_price': trade.exit_price,
                    'exit_reason': trade.exit_reason,
                    'exit_order_id': trade.exit_order_id,
                    'created_at': trade.created_at,
                    'updated_at': trade.updated_at
                })
            
            logger.info(f"Retrieved {len(result)} closed trades")
            return result
            
        except SQLAlchemyError as e:
            logger.error(f"Error getting closed trades: {e}")
            return []
        finally:
            session.close()
    
    def get_trades_by_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """Get all trades for a specific symbol."""
        if not self.SessionLocal:
            logger.error("Database not connected")
            return []
            
        session = self.get_session()
        try:
            trades = session.query(Trade).filter(
                Trade.symbol == symbol
            ).order_by(Trade.filled_at.desc()).all()
            
            result = []
            for trade in trades:
                result.append({
                    'id': trade.id,
                    'symbol': trade.symbol,
                    'status': trade.status,
                    'filled_at': trade.filled_at,
                    'filled_qty': trade.filled_qty,
                    'filled_price': trade.filled_price,
                    'side': trade.side,
                    'entry_order_id': trade.entry_order_id,
                    'take_profit': trade.take_profit,
                    'stop_loss': trade.stop_loss,
                    'take_profit_order_id': trade.take_profit_order_id,
                    'stop_loss_order_id': trade.stop_loss_order_id,
                    'exit_at': trade.exit_at,
                    'exit_qty': trade.exit_qty,
                    'exit_price': trade.exit_price,
                    'exit_reason': trade.exit_reason,
                    'exit_order_id': trade.exit_order_id,
                    'created_at': trade.created_at,
                    'updated_at': trade.updated_at
                })
            
            logger.info(f"Retrieved {len(result)} trades for {symbol}")
            return result
            
        except SQLAlchemyError as e:
            logger.error(f"Error getting trades by symbol: {e}")
            return []
        finally:
            session.close()
    
    def update_trade_status(self, trade_id: int, status: str) -> bool:
        """Update trade status."""
        return self.update_trade(trade_id, {'status': status})
    
    def update_take_profit_order_id(self, trade_id: int, take_profit_order_id: str) -> bool:
        """Update take profit order ID for a trade."""
        return self.update_trade(trade_id, {'take_profit_order_id': take_profit_order_id})
    
    def update_stop_loss_order_id(self, trade_id: int, stop_loss_order_id: str) -> bool:
        """Update stop loss order ID for a trade."""
        return self.update_trade(trade_id, {'stop_loss_order_id': stop_loss_order_id})
    
    def update_take_profit_price(self, trade_id: int, take_profit: float) -> bool:
        """Update take profit price for a trade."""
        if not self.SessionLocal:
            logger.error("Database not connected")
            return False
            
        session = self.get_session()
        try:
            trade = session.query(Trade).filter(Trade.id == trade_id).first()
            if not trade:
                logger.warning(f"Trade with ID {trade_id} not found")
                return False
            
            trade.take_profit = take_profit
            session.commit()
            
            logger.info(f"Take profit price updated for trade {trade_id}: {take_profit}")
            return True
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error updating take profit price: {e}")
            return False
        finally:
            session.close()
    
    def update_stop_loss_price(self, trade_id: int, stop_loss: float) -> bool:
        """Update stop loss price for a trade."""
        return self.update_trade(trade_id, {'stop_loss': stop_loss})

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
        if not self.SessionLocal:
            logger.error("Database not connected")
            return False
            
        # Define allowed fields for updates
        allowed_fields = {
            'status', 'take_profit', 'stop_loss', 'take_profit_order_id', 
            'stop_loss_order_id', 'exit_at', 'exit_qty', 'exit_price', 
            'exit_reason', 'exit_order_id'
        }
        
        # Validate that all update fields are allowed
        invalid_fields = set(updates.keys()) - allowed_fields
        if invalid_fields:
            logger.error(f"Invalid fields for update: {invalid_fields}")
            return False
            
        session = self.get_session()
        try:
            trade = session.query(Trade).filter(Trade.id == trade_id).first()
            if not trade:
                logger.warning(f"Trade with ID {trade_id} not found")
                return False
            
            # Apply updates
            for field, value in updates.items():
                if hasattr(trade, field):
                    setattr(trade, field, value)
                else:
                    logger.error(f"Field {field} does not exist on Trade model")
                    return False
            
            session.commit()
            
            logger.info(f"Trade {trade_id} updated with fields: {list(updates.keys())}")
            return True
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error updating trade: {e}")
            return False
        finally:
            session.close()
    
    def delete_trade(self, trade_id: int) -> bool:
        """Delete a trade from the database."""
        if not self.SessionLocal:
            logger.error("Database not connected")
            return False
            
        session = self.get_session()
        try:
            trade = session.query(Trade).filter(Trade.id == trade_id).first()
            if not trade:
                logger.warning(f"Trade with ID {trade_id} not found")
                return False
            
            session.delete(trade)
            session.commit()
            
            logger.info(f"Trade deleted: {trade.symbol}")
            return True
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error deleting trade: {e}")
            return False
        finally:
            session.close()
    
    def get_trade_statistics(self) -> Dict[str, Any]:
        """Get trade statistics."""
        if not self.SessionLocal:
            logger.error("Database not connected")
            return {}
            
        session = self.get_session()
        try:
            total_trades = session.query(Trade).count()
            open_trades = session.query(Trade).filter(Trade.status == 'open').count()
            closed_trades = session.query(Trade).filter(Trade.status == 'closed').count()
            
            # Calculate total P&L for closed trades
            closed_trades_data = session.query(Trade).filter(Trade.status == 'closed').all()
            total_pnl = 0.0
            winning_trades = 0
            losing_trades = 0
            
            for trade in closed_trades_data:
                if trade.exit_price and trade.filled_price:
                    pnl = (trade.exit_price - trade.filled_price) * trade.filled_qty
                    if trade.side == 'sell':  # Short position
                        pnl = -pnl
                    total_pnl += pnl
                    
                    if pnl > 0:
                        winning_trades += 1
                    else:
                        losing_trades += 1
            
            win_rate = (winning_trades / closed_trades * 100) if closed_trades > 0 else 0
            
            return {
                'total_trades': total_trades,
                'open_trades': open_trades,
                'closed_trades': closed_trades,
                'total_pnl': total_pnl,
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'win_rate': win_rate
            }
            
        except SQLAlchemyError as e:
            logger.error(f"Error getting trade statistics: {e}")
            return {}
        finally:
            session.close()

    def add_trades_from_dataframe(self, df: pd.DataFrame) -> List[int]:
        """
        Add multiple trades from a pandas DataFrame.
        
        Args:
            df: DataFrame with trade data. Column names should match Trade model fields.
                 Non-NaN values will be used for each trade.
        
        Returns:
            List of trade IDs that were successfully added
            
        Expected DataFrame columns (all optional except basic trade info):
            - symbol: Stock symbol
            - filled_at: Fill datetime
            - filled_qty: Quantity filled
            - filled_price: Price filled
            - side: Trade side (buy/sell)
            - entry_order_id: Entry order ID
            - status: Trade status (default: 'open')
            - take_profit: Take profit price
            - stop_loss: Stop loss price
            - take_profit_order_id: Take profit order ID
            - stop_loss_order_id: Stop loss order ID
            - exit_at: Exit datetime
            - exit_qty: Exit quantity
            - exit_price: Exit price
            - exit_reason: Exit reason
            - exit_order_id: Exit order ID
        """
        if not self.SessionLocal:
            logger.error("Database not connected")
            return []
        
        # Define allowed fields and their types
        allowed_fields = {
            'symbol': str,
            'status': str,
            'filled_at': datetime,
            'filled_qty': float,
            'filled_price': float,
            'side': str,
            'entry_order_id': str,
            'take_profit': float,
            'stop_loss': float,
            'take_profit_order_id': str,
            'stop_loss_order_id': str,
            'exit_at': datetime,
            'exit_qty': float,
            'exit_price': float,
            'exit_reason': str,
            'exit_order_id': str
        }
        
        # Validate DataFrame columns
        invalid_columns = set(df.columns) - set(allowed_fields.keys())
        if invalid_columns:
            logger.warning(f"Invalid columns in DataFrame: {invalid_columns}")
            # Remove invalid columns
            df = df.drop(columns=list(invalid_columns))
        
        # Check if symbol is in columns or index
        symbol_from_index = False
        if 'symbol' not in df.columns:
            if df.index.name == 'symbol' or (isinstance(df.index, pd.Index) and df.index.dtype == 'object'):
                symbol_from_index = True
                logger.info("Using DataFrame index as symbol column")
            else:
                logger.error("Missing required column 'symbol' and index is not suitable for symbol")
                return []
        
        # Ensure other required columns exist
        required_columns = ['filled_at', 'filled_qty', 'filled_price', 'side', 'entry_order_id']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return []
        
        session = self.get_session()
        trade_ids = []
        
        try:
            for index, row in df.iterrows():
                try:
                    # Create trade data dictionary with non-NaN values
                    trade_data = {}
                    
                    # Handle symbol from index or column
                    if symbol_from_index:
                        trade_data['symbol'] = str(index)
                    else:
                        if 'symbol' in df.columns and pd.notna(row['symbol']):
                            trade_data['symbol'] = str(row['symbol'])
                        else:
                            logger.warning(f"Missing symbol for row {index}")
                            continue
                    
                    # Process other fields
                    for field, field_type in allowed_fields.items():
                        if field == 'symbol':  # Skip symbol as it's already handled
                            continue
                        if field in df.columns and pd.notna(row[field]):
                            value = row[field]
                            
                            # Type conversion and validation
                            if field_type == datetime:
                                if isinstance(value, str):
                                    try:
                                        value = pd.to_datetime(value)
                                    except:
                                        logger.warning(f"Invalid datetime format for {field}: {value}")
                                        continue
                                elif isinstance(value, pd.Timestamp):
                                    value = value.to_pydatetime()
                            elif field_type == float:
                                try:
                                    value = float(value)
                                except:
                                    logger.warning(f"Invalid float value for {field}: {value}")
                                    continue
                            elif field_type == str:
                                value = str(value)
                            
                            trade_data[field] = value
                    
                    # Set default status if not provided
                    if 'status' not in trade_data:
                        trade_data['status'] = 'open'
                    
                    # Create Trade object
                    trade = Trade(**trade_data)
                    session.add(trade)
                    session.flush()  # Get the ID without committing
                    
                    trade_ids.append(trade.id)
                    logger.debug(f"Added trade {trade.id} for {trade.symbol}")
                    
                except Exception as e:
                    logger.error(f"Error adding trade from row {index}: {e}")
                    continue
            
            session.commit()
            logger.info(f"Successfully added {len(trade_ids)} trades from DataFrame")
            return trade_ids
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error adding trades from DataFrame: {e}")
            return []
        finally:
            session.close()

    def add_trades_from_csv(self, csv_file_path: str) -> List[int]:
        """
        Add multiple trades from a CSV file.
        
        Args:
            csv_file_path: Path to the CSV file containing trade data
            
        Returns:
            List of trade IDs that were successfully added
        """
        try:
            df = pd.read_csv(csv_file_path)
            return self.add_trades_from_dataframe(df)
        except Exception as e:
            logger.error(f"Error reading CSV file {csv_file_path}: {e}")
            return []
