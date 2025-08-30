from typing import Dict, List
from datetime import datetime, timedelta
import pandas as pd

from watson.logger import get_logger
from ..data.earnings_calendar import EarningsCalendar
from .filters import Filter, FilterError

logger = get_logger(__name__)


class Earnings(Filter):
    def __init__(self, lookahead: int, name: str = "Earnings"):
        self.initialized = False
        self.name = name
        self.lookahead = lookahead
        self.dates: Dict[str, List[datetime]] = None

        period = datetime.now() + timedelta(days=lookahead)
        # Add 1 day offset to avoid missing earnings dates
        self.earnings_calendar = EarningsCalendar(period=period + timedelta(days=1))

        self.universe: List[str] = None

    async def initialize(self, universe: List[str]):
        self.universe = universe
        self.dates = await self.earnings_calendar.get_multiple_earnings_dates(universe)
        self.initialized = True

    async def apply(self) -> Dict[str, bool]:
        if not self.initialized:
            raise FilterError(f"Filter {self.name} not initialized")

        today = datetime.now()

        result = {}

        for symbol in self.universe:
            if symbol not in self.dates:
                logger.debug(f"No earnings data for {symbol} in the next {self.lookahead} days")
                result[symbol] = True
            else:
                for date in self.dates[symbol]:
                    if date - today <= timedelta(days=self.lookahead):
                        result[symbol] = False
                        break

        return result
        