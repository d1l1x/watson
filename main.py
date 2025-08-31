import os
import argparse
import pathlib
import asyncio

from watson.core.loader import load_strategy_from_path
from watson.core.banner import welcome
# from watson.core.strategy.base import BaseStrategy
from watson.logger import get_logger

logger = get_logger(__name__)

def command_line_arguments():
    logger.info("Parsing command line arguments")
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--strategies", type=str, required=False, 
                       default=os.getenv("STRATEGIES_PATH", "./strategies"))
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    welcome()
    
    args = command_line_arguments()

    # Filter out files that begin with underscore
    strategies = [f for f in pathlib.Path(args.strategies).glob("*.py") 
                  if not f.name.startswith('_')]

    strategy_classes = [load_strategy_from_path(str(strategy)) for strategy in strategies]

    async def main():
        for strategy in strategy_classes:
            await strategy.initialize()
            await strategy.run()

    asyncio.run(main())
