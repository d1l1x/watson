from typing import List
import pandas as pd

from watson.logger import get_logger
from watson.data.symbols import Universe, get_symbols
from watson.data.market_data import MarketDataManager
from watson.filters.filters import Filter
from watson.filters.indicators import IndicatorFilter, TechnicalIndicator

logger = get_logger(__name__)

class Screener:

    def __init__(self, universe: str = Universe.NASDAQ100):
        self.initialized = False
        self.universe = universe

        self.market_data_manager = MarketDataManager(universe=universe)

        self.symbols: pd.DataFrame = None
        self.filters: List[Filter] = []

        self.candidates: pd.DataFrame = None

        self.use = None


    async def initialize(self):
        logger.info(f"Initializing screener with universe: {self.universe}")

        self.symbols = await get_symbols(self.universe)
        self.candidates = self.symbols.copy()

        market_data = await self.market_data_manager.get_multiple_symbols_data(self.symbols.index.tolist())

        if self.filters:
            for filter in self.filters:
                if isinstance(filter, Filter):
                    await filter.initialize(self.symbols.index.tolist())
                elif isinstance(filter, (TechnicalIndicator, IndicatorFilter)):
                    await filter.initialize(self.symbols.index.tolist(), market_data)

        self.initialized = True

    def add_filter(self, filter: Filter):
        if self.filters is None:
            self.filters = []
        self.filters.append(filter)

    def apply_filters(self) -> pd.DataFrame:
        if self.use:
            f = True
            for filter in self.use:
                f = f & (self.candidates[filter] == True)
            return self.candidates[f]
        else:
            return self.candidates

    async def run(self):
        for filter in self.filters:
            if isinstance(filter, IndicatorFilter):
                # Handle indicator comparisons that return boolean results
                filter_results = await filter.apply()
                self.candidates[filter.name] = filter_results
            else:
                # Handle regular filters
                self.candidates[filter.name] = await filter.apply()

    def view(self):
        print(self.candidates)

    def screen(self, data: pd.DataFrame) -> pd.DataFrame:
        pass