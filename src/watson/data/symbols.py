import pandas as pd
import requests
from io import StringIO
from enum import Enum

from watson.logger import get_logger

logger = get_logger(__name__)

URL_SLICKCHARTS = 'https://www.slickcharts.com/'

USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/111.0'

class Universe(Enum):
    NASDAQ100 = 'nasdaq100'
    SP500 = 'sp500'

async def get_symbols(universe: Universe) -> pd.DataFrame:
    try:
        match universe:
            # Ref: https://stackoverflow.com/a/75846060/
            case Universe.NASDAQ100:
                response = requests.get(f'{URL_SLICKCHARTS}/nasdaq100', headers={'User-Agent': USER_AGENT}, timeout=30)
            case Universe.SP500:
                response = requests.get(f'{URL_SLICKCHARTS}/sp500', headers={'User-Agent': USER_AGENT}, timeout=30)
            case _:
                raise ValueError(f"Unsupported universe: {universe}")
        
        response.raise_for_status()
        
        # Check if response has content
        if not response.text.strip():
            raise ValueError(f"Empty response received for {universe.value}")
        
        try:
            dfs = pd.read_html(StringIO(response.text), match='Symbol', index_col='Symbol')
            if not dfs:
                raise ValueError(f"No tables found with 'Symbol' column for {universe.value}")
            
            df = dfs[0][['Company']]
            if df.empty:
                raise ValueError(f"Empty dataframe returned for {universe.value}")
            
            return df
            
        except Exception as parse_error:
            logger.error(f"Failed to parse HTML for {universe.value}: {parse_error}")
            raise ValueError(f"Failed to parse symbols data for {universe.value}: {parse_error}")
            
    except Exception as e:
        logger.error(f"Unexpected error while fetching {universe.value} symbols: {e}")
        raise RuntimeError(f"Unexpected error while fetching {universe.value} symbols: {e}")


if __name__ == "__main__":
    import asyncio
    symbols = asyncio.run(get_symbols(Universe.NASDAQ100))
    print(symbols)