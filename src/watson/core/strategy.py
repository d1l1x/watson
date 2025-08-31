import asyncio
from abc import ABC, abstractmethod
import pandas as pd
from watson.core.portfolio import PortfolioError
from watson.logger import get_logger

logger = get_logger(__name__)

class Strategy(ABC):

    def __init__(self):
        self.broker = None
        self.mm = None
        self.pm = None
        self.screener = None
        self.scheduler = None

    @abstractmethod
    async def check_entry_conditions(self, open_positions: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    async def check_exit_conditions(self, open_positions: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement this method")

    async def initialize(self):
        if self.broker is not None:
            await self.broker.initialize()
        if self.mm is not None:
            await self.mm.initialize()
        if self.pm is not None:
            await self.pm.initialize()

        if self.screener is not None:
            await self.screener.initialize()

        if self.scheduler is not None:
            await self.scheduler.initialize(self.loop)

    async def loop(self):
        logger.info("Starting strategy loop")
        if self.pm is None or self.mm is None or self.broker is None:
            raise ValueError("Portfolio manager, money management and broker must be set")

        open_positions = await self.pm.update()

        exit_signals = await self.check_exit_conditions(open_positions)

        if exit_signals.empty and len(open_positions) >= self.mm.max_positions:
            logger.info(f"Max positions reached ({self.mm.max_positions}), exiting...")
            return

        if not exit_signals.empty:
            logger.info(f"Exit signals found: {exit_signals.index.tolist()}")
            try:
                await self.pm.close_trades(exit_signals)
            except PortfolioError as e:
                logger.error(f"Error closing trades: {e}")
                return

        buy_signals = await self.check_entry_conditions(open_positions)

        if buy_signals.empty:
            logger.info(f"There are no entry signals")
            return

        try:
            await self.pm.open_trades(buy_signals)
        except PortfolioError as e:
            logger.error(f"Error opening trades: {e}")
            return
        logger.info("Strategy loop finished")

    async def run(self):
        if self.scheduler:
            await self.scheduler.start()
            while True:
                await asyncio.sleep(10)
        else:
            await self.loop()