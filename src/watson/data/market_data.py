import asyncio
from typing import Dict, List, Any
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from enum import Enum

from watson.settings import API_KEYS
from .symbols import Universe
from watson.logger import get_logger

logger = get_logger(__name__)

class DataProvider(Enum):
    YAHOO = "yahoo"

class MarketDataManager:
    
    def __init__(self, universe: str = Universe.NASDAQ100, provider: DataProvider = DataProvider.YAHOO):
        self.universe = universe
        self.provider = provider
        self.data_cache: Dict[str, pd.DataFrame] = {}
        self.cache_expiry: Dict[str, datetime] = {}
        self.cache_duration = timedelta(hours=1)  # Cache for 1 hour

    def _is_cache_valid(self, cache_key: str) -> bool:
        if cache_key not in self.cache_expiry:
            return False
            
        return datetime.now() < self.cache_expiry[cache_key]
        
    def _cache_data(self, cache_key: str, data: pd.DataFrame):
        self.data_cache[cache_key] = data
        self.cache_expiry[cache_key] = datetime.now() + self.cache_duration
        
    def clear_cache(self):
        self.data_cache.clear()
        self.cache_expiry.clear()
        logger.info("Market data cache cleared")
        
    def get_cache_info(self) -> Dict[str, Any]:
        return {
            "cached_symbols": len(self.data_cache),
            "cache_duration_hours": self.cache_duration.total_seconds() / 3600,
            "expired_entries": len([k for k, v in self.cache_expiry.items() 
                                  if datetime.now() >= v])
        }
        
    async def get_multiple_symbols_data(self, symbols: List[str], period: str = "300d") -> Dict[str, pd.DataFrame]:

        match self.provider:

            case DataProvider.YAHOO:
                tickers = yf.Tickers(symbols)
                history = tickers.history(period=period)
                history.rename(columns=str.upper, inplace=True)
            case _:
                logger.error(f"Unsupported provider: {self.provider}")
                return {}
        
        logger.info(f"Fetched data for {len(symbols)} symbols")
        self.data_cache = history
        return history
        
if __name__ == '__main__':

    mgm = MarketDataManager(provider=DataProvider.YAHOO)

    data = asyncio.run(mgm._get_historical_data('AAPL', '250d'))
    print(data)