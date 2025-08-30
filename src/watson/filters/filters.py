from abc import ABC, abstractmethod
from enum import Enum
import pandas as pd


class FilterError(Exception):
    pass


class Filter(ABC):

    @abstractmethod
    async def initialize(self, data: pd.DataFrame) -> pd.DataFrame:
        pass

    @abstractmethod
    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        pass