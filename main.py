import os
import argparse

from watson.core.loader import load_strategy_from_path
# from watson.core.strategy.base import BaseStrategy


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--strategies", type=str, required=False, 
                       default=os.getenv("STRATEGIES_PATH", "./strategies"))
    args = parser.parse_args()

    strategies = [f for f in os.listdir(args.strategies) if f.endswith(".py")]

    for strategy in strategies:
        strategy = load_strategy_from_path(strategy, 'Strategy')