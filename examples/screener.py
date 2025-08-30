import asyncio
from typing import List
import pandas as pd

from watson.core.screener import Screener
from watson.filters.earnings import Earnings
from watson.data.symbols import Universe, get_symbols
from watson.filters.indicators import Roc, Adx, Rsi, PriceType

if __name__ == "__main__":

    async def main():
        screener = Screener(universe=Universe.NASDAQ100)
        screener.add_filter(Earnings(lookahead=11))

        screener.add_filter(Roc(period=120, price=PriceType.CLOSE).gt(0))
        screener.add_filter(Adx(period=24, price=[PriceType.HIGH, PriceType.LOW, PriceType.CLOSE]).gt(20))
        screener.add_filter(Rsi(period=14, price=PriceType.CLOSE).lt(10))

        screener.add_filter(Roc(period=60, price=PriceType.CLOSE))

        await screener.initialize()
        await screener.run()

        candidates = screener.candidates[
            (screener.candidates["Earnings"] == True) 
            & (screener.candidates["ROC120>0"] == True)
            & (screener.candidates["ADX24>20"] == True)
            & (screener.candidates["RSI14<10"] == True)
            ].sort_values(by=['ROC60'], ascending=False)

        print(candidates)


    asyncio.run(main())
