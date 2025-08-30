from typing import Dict, List
from datetime import datetime, timedelta
import finnhub

from watson.settings import API_KEYS
from watson.logger import get_logger

logger = get_logger(__name__)


class EarningsCalendar:
    
    def __init__(self, period: datetime):
        self.earnings_data: Dict[str, List[Dict]] = {}
        self.cache_expiry: Dict[str, datetime] = {}
        self.cache_duration = timedelta(days=1) 

        self.finnhub = finnhub.Client(api_key=API_KEYS["finnhub"])

        self.period = period
        
            
    async def get_multiple_earnings_dates(self, symbols: List[str]) -> Dict[str, List[datetime]]:
        """
        Get earnings dates for multiple symbols.
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            Dictionary of symbol -> earnings dates
        """
        earnings_dict = {}
        
        dates = self.finnhub.earnings_calendar(_from = datetime.now().strftime("%Y-%m-%d"), to=self.period.strftime("%Y-%m-%d"), symbol = '', international=False)

        for date in dates['earningsCalendar']:
            if date['symbol'] in symbols:
                logger.debug(f"Fetched earnings date for {date['symbol']}: {date['date']}")
                earnings_dict[date['symbol']] = [datetime.strptime(date['date'], "%Y-%m-%d")]
                
        logger.info(f"Fetched earnings dates for {len(earnings_dict)} symbols")
        return earnings_dict