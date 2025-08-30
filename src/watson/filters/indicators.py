"""
Trading filters for earnings calendar and sector diversification.
"""
from typing import Dict, List, Union
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from enum import Enum
import talib
import pandas as pd

from watson.logger import get_logger
from .filters import FilterError

logger = get_logger(__name__)


class PriceType(Enum):
    CLOSE = "CLOSE"
    OPEN = "OPEN"
    HIGH = "HIGH"
    LOW = "LOW"


class IndicatorFilter:
    """Wrapper for indicator comparisons that can be applied as filters"""
    
    def __init__(self, indicator, comparison_func, value):
        self.indicator = indicator
        self.comparison_func = comparison_func
        self.value = value
        self.name = f"{indicator.name}{comparison_func.__name__}{value}".upper()
        self.initialized = False
        
    async def initialize(self, universe: List[str], market_data: Dict[str, pd.DataFrame]):
        await self.indicator.initialize(universe, market_data)
        self.initialized = True
        
    async def apply(self) -> Dict[str, bool]:
        if not self.initialized:
            raise FilterError(f"Indicator comparison {self.name} not initialized")
            
        indicator_values = await self.indicator.apply()
        result = {}
        
        for symbol, value in indicator_values.items():
            if pd.isna(value):
                logger.warning(f"Indicator {self.name} is NaN for {symbol}")
                result[symbol] = False
            else:
                result[symbol] = self.comparison_func(value, self.value)
                
        return result

class IndicatorComparison:
    """Mixin class that provides comparison methods for indicators"""

    def gt(self, value: float) -> IndicatorFilter:
        """Greater than comparison"""
        f = lambda x, y: x > y
        f.__name__ = ">"
        return IndicatorFilter(self, f, value)
    
    def lt(self, value: float) -> IndicatorFilter:
        """Less than comparison"""
        f = lambda x, y: x < y
        f.__name__ = "<"
        return IndicatorFilter(self, f, value)
    
    def gte(self, value: float) -> IndicatorFilter:
        """Greater than or equal comparison"""
        f = lambda x, y: x >= y
        f.__name__ = ">="
        return IndicatorFilter(self, f, value)
    
    def lte(self, value: float) -> IndicatorFilter:
        """Less than or equal comparison"""
        f = lambda x, y: x <= y
        f.__name__ = "<="
        return IndicatorFilter(self, f, value)
    
    def eq(self, value: float) -> IndicatorFilter:
        """Equal comparison"""
        f = lambda x, y: x == y
        f.__name__ = "="
        return IndicatorFilter(self, f, value)

class TechnicalIndicator(IndicatorComparison):
    """Base class for technical indicators that reduces code duplication"""
    
    def __init__(self, name: str, period: int = 20, price: Union[PriceType, List[PriceType]] = PriceType.CLOSE):
        self.initialized = False
        self.name = name
        self.period = period
        self.price = price
        self.universe: List[str] = None
        self.market_data = {}

    async def initialize(self, universe: List[str], market_data: Dict[str, pd.DataFrame]):
        self.universe = universe

        for symbol in universe:
            if isinstance(self.price, PriceType):
                self.market_data[symbol] = market_data[self.price.value][symbol]
            elif isinstance(self.price, list):
                    self.market_data[symbol] = {price.value: market_data[price.value][symbol] for price in self.price}
            else:
                raise ValueError(f"Invalid price type: {type(self.price)}")

        self.initialized = True

    @abstractmethod
    def calculate_indicator(self, symbol_data: pd.DataFrame) -> float:
        """Calculate the indicator value for a single symbol"""
        raise NotImplementedError("Subclasses must implement this method")

    async def apply(self) -> Dict[str, float]:
        if not self.initialized:
            raise FilterError(f"Indicator {self.name} not initialized")
        
        values = {}
        for symbol in self.universe:
            if symbol in self.market_data:
                try:
                    values[symbol] = self.calculate_indicator(self.market_data[symbol])
                except Exception as e:
                    logger.warning(f"Error calculating {self.name} for {symbol}: {e}")
                    values[symbol] = float('nan')
            else:
                logger.warning(f"No market data for {symbol} available")
                values[symbol] = float('nan')
        return values

class Roc(TechnicalIndicator):
    """Rate of Change indicator"""

    def __init__(self, period: int = 20, price: PriceType = PriceType.CLOSE):
        super().__init__(f"ROC{period}", period, price)

    def calculate_indicator(self, symbol_data: pd.DataFrame) -> float:
        return talib.ROC(symbol_data, timeperiod=self.period).iat[-1]

class Adx(TechnicalIndicator):
    """Average Directional Index indicator"""

    def __init__(self, period: int = 14, price: List[PriceType] = [PriceType.HIGH, PriceType.LOW, PriceType.CLOSE]):
        super().__init__(f"ADX{period}", period, price)

    def calculate_indicator(self, symbol_data: pd.DataFrame) -> float:
        return talib.ADX(
            symbol_data[PriceType.HIGH.value], 
            symbol_data[PriceType.LOW.value], 
            symbol_data[PriceType.CLOSE.value], 
            timeperiod=self.period
        ).iat[-1]

class Rsi(TechnicalIndicator):
    """Relative Strength Index indicator"""

    def __init__(self, period: int = 14, price: PriceType = PriceType.CLOSE):
        super().__init__(f"RSI{period}", period, price)

    def calculate_indicator(self, symbol_data: pd.DataFrame) -> float:
        return talib.RSI(symbol_data, timeperiod=self.period).iat[-1]

class Sma(TechnicalIndicator):
    """Simple Moving Average indicator"""

    def __init__(self, period: int = 20, price: PriceType = PriceType.CLOSE):
        super().__init__(f"SMA{period}", period, price)

    def calculate_indicator(self, symbol_data: pd.DataFrame) -> float:
        return talib.SMA(symbol_data, timeperiod=self.period).iat[-1]

class Ema(TechnicalIndicator):
    """Exponential Moving Average indicator"""

    def __init__(self, period: int = 20, price: PriceType = PriceType.CLOSE):
        super().__init__(f"EMA{period}", period, price)

    def calculate_indicator(self, symbol_data: pd.DataFrame) -> float:
        return talib.EMA(symbol_data, timeperiod=self.period).iat[-1]

class Macd(TechnicalIndicator):
    """MACD indicator"""

    def __init__(self, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9):
        super().__init__(f"MACD{fast_period}_{slow_period}_{signal_period}")
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period

    def calculate_indicator(self, symbol_data: pd.DataFrame) -> float:
        # Return the MACD line (not the signal or histogram)
        return talib.MACD(
            symbol_data["close"], 
            fastperiod=self.fast_period, 
            slowperiod=self.slow_period, 
            signalperiod=self.signal_period
        )[0].iat[-1]  # MACD line
