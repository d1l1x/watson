from watson.broker.broker import Broker

class MoneyManagement:

    def __init__(self, broker: Broker, 
                 max_positions: int = None,
                 pct_per_position: float = 0.1,
                 pct_net_asset_value: float = 0.5):
        self.broker = broker
        self.max_positions = max_positions
        self.pct_per_position = pct_per_position
        self.pct_net_asset_value = pct_net_asset_value

        self.net_asset_value = None

    async def initialize(self):
        await self.update_account_info()

    async def update_account_info(self):
        # TODO: Ensure that the update happens once per day
        account_info = await self.broker.get_account_info()
        self.net_asset_value = float(account_info.equity)

    async def get_entry_qty(self, symbol: str) -> int:
        if self.net_asset_value is None:
            await self.update_account_info()

        quote = await self.broker.get_stock_latest_bar(symbol)
        return int(self.net_asset_value * self.pct_net_asset_value * self.pct_per_position / quote[symbol].close)