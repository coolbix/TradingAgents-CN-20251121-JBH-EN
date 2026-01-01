"""
Data source manager that orchestrates multiple adapters with priority and optional consistency checks
"""
from typing import List, Optional, Tuple, Dict
import logging
from datetime import datetime, timedelta
import pandas as pd

from .base import DataSourceAdapter
from .tushare_adapter import TushareAdapter
from .akshare_adapter import AKShareAdapter
from .baostock_adapter import BaoStockAdapter

logger = logging.getLogger(__name__)


class DataSourceManager:
    """
    NOTE: there is another DataSourceManager in tradingagents/dataflows/data_source_manager.py
    NOTE: consider unifying them in the future
    Data Source Manager
    - Manage multiple adapters based on priority ranking
    - Offering capacity to fallback
    - Optional: Consistency check (if dependent)
    """

    def __init__(self):
        self.adapters: List[DataSourceAdapter] = [
            TushareAdapter(),
            AKShareAdapter(),
            BaoStockAdapter(),
        ]

        #Load priority configuration from database
        self._load_priority_from_database()

        #Sort in order of priority (the larger the number, the higher the priority, the lower the order)
        self.adapters.sort(key=lambda x: x.priority, reverse=True)

        try:
            from .data_consistency_checker import DataConsistencyChecker  # type: ignore
            self.consistency_checker = DataConsistencyChecker()
        except Exception:
            logger.warning("Data Consistency Checker Not Available")
            self.consistency_checker = None

    def _load_priority_from_database(self):
        """Data source priority configuration from database load (read A stock market priority from data groupings)"""
        try:
            from app.core.database import get_mongo_db_synchronous
            db = get_mongo_db_synchronous()
            groupings_collection = db.datasource_groupings

            #Query data source grouping for Unit A market
            groupings = list(groupings_collection.find({
                "market_category_id": "a_shares",
                "enabled": True
            }))

            if groupings:
                #Create a map with a name to priority (data source name needs to be converted to lowercase)
                priority_map = {}
                for grouping in groupings:
                    data_source_name = grouping.get('data_source_name', '').lower()
                    priority = grouping.get('priority')
                    if data_source_name and priority is not None:
                        priority_map[data_source_name] = priority
                        logger.info(f"Read from the database{data_source_name}In the A stock market priorities:{priority}")

                #Update priority for each Adapter
                for adapter in self.adapters:
                    if adapter.name in priority_map:
                        #Dynamic setting priorities
                        adapter._priority = priority_map[adapter.name]
                        logger.info(f"Settings{adapter.name}Priority:{adapter._priority}")
                    else:
                        #Use default priority
                        adapter._priority = adapter._get_default_priority()
                        logger.info(f"Not found in database ⚠️{adapter.name}, use the default priority:{adapter._priority}")
            else:
                logger.info("No data source configuration for the A share market was found in ⚠️ database, using default priority")
                #Use default priority
                for adapter in self.adapters:
                    adapter._priority = adapter._get_default_priority()
        except Exception as e:
            logger.warning(f"Could not close temporary folder: %s{e}, using default priority")
            import traceback
            logger.warning(f"Stack tracking: \n{traceback.format_exc()}")
            #Use default priority
            for adapter in self.adapters:
                adapter._priority = adapter._get_default_priority()

    def get_available_adapters(self) -> List[DataSourceAdapter]:
        available: List[DataSourceAdapter] = []
        for adapter in self.adapters:
            if adapter.is_available():
                available.append(adapter)
                logger.info(
                    f"Data source {adapter.name} is available (priority: {adapter.priority})"
                )
            else:
                logger.warning(f"Data source {adapter.name} is not available")
        return available

    def get_stock_list_with_fallback(self, preferred_sources: Optional[List[str]] = None) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Get a list of shares to support the designation of priority data sources

Args:
Prefered sources: list of preferred data sources, such as ['akshare', 'baostock']
If None, in the default priority order

Returns:
(DataFrame, source name) or (None, None)
"""
        available_adapters = self.get_available_adapters()

        #Reorder if priority data sources are specified
        if preferred_sources:
            logger.info(f"Using preferred data sources: {preferred_sources}")
            #Create Priority Map
            priority_map = {name: idx for idx, name in enumerate(preferred_sources)}
            #Line the specified data sources ahead, and keep the rest in order
            preferred = [a for a in available_adapters if a.name in priority_map]
            others = [a for a in available_adapters if a.name not in priority_map]
            #Sort in order of prefered sources
            preferred.sort(key=lambda a: priority_map.get(a.name, 999))
            available_adapters = preferred + others
            logger.info(f"Reordered adapters: {[a.name for a in available_adapters]}")

        for adapter in available_adapters:
            try:
                logger.info(f"Trying to fetch stock list from {adapter.name}")
                df = adapter.get_stock_list()
                if df is not None and not df.empty:
                    return df, adapter.name
            except Exception as e:
                logger.error(f"Failed to fetch stock list from {adapter.name}: {e}")
                continue
        return None, None

    def get_daily_basic_with_fallback(self, trade_date: str, preferred_sources: Optional[List[str]] = None) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Access to daily basic data to support the designation of priority data sources

Args:
trade date: transaction date
Prefered sources: Priority list of data sources

Returns:
(DataFrame, source name) or (None, None)
"""
        available_adapters = self.get_available_adapters()

        #Reorder if priority data sources are specified
        if preferred_sources:
            priority_map = {name: idx for idx, name in enumerate(preferred_sources)}
            preferred = [a for a in available_adapters if a.name in priority_map]
            others = [a for a in available_adapters if a.name not in priority_map]
            preferred.sort(key=lambda a: priority_map.get(a.name, 999))
            available_adapters = preferred + others

        for adapter in available_adapters:
            try:
                logger.info(f"Trying to fetch daily basic data from {adapter.name}")
                df = adapter.get_daily_basic(trade_date)
                if df is not None and not df.empty:
                    return df, adapter.name
            except Exception as e:
                logger.error(f"Failed to fetch daily basic data from {adapter.name}: {e}")
                continue
        return None, None

    def find_latest_trade_date_with_fallback(self, preferred_sources: Optional[List[str]] = None) -> Optional[str]:
        """Find the latest transaction date and support the designation of priority data Source

Args:
Prefered sources: Priority list of data sources

Returns:
Transaction Date String (YYYMMDD format) or None
"""
        available_adapters = self.get_available_adapters()

        #Reorder if priority data sources are specified
        if preferred_sources:
            priority_map = {name: idx for idx, name in enumerate(preferred_sources)}
            preferred = [a for a in available_adapters if a.name in priority_map]
            others = [a for a in available_adapters if a.name not in priority_map]
            preferred.sort(key=lambda a: priority_map.get(a.name, 999))
            available_adapters = preferred + others

        for adapter in available_adapters:
            try:
                trade_date = adapter.find_latest_trade_date()
                if trade_date:
                    return trade_date
            except Exception as e:
                logger.error(f"Failed to find trade date from {adapter.name}: {e}")
                continue
        return (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    def get_realtime_quotes_with_fallback(self) -> Tuple[Optional[Dict], Optional[str]]:
        """Get market-wide real-time snapshots, try in order of adaptor priority, and return the first successful result
RETURNS: (quates dict, source name)
Quotes dict forms    FMT 0,...}
"""
        available_adapters = self.get_available_adapters()
        for adapter in available_adapters:
            try:
                logger.info(f"Trying to fetch realtime quotes from {adapter.name}")
                data = adapter.get_realtime_quotes()
                if data:
                    return data, adapter.name
            except Exception as e:
                logger.error(f"Failed to fetch realtime quotes from {adapter.name}: {e}")
                continue
        return None, None


    def get_daily_basic_with_consistency_check(
        self, trade_date: str
    ) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[Dict]]:
        """Access to daily basic data using consistency checks

Returns:
Tuple.
"""
        available_adapters = self.get_available_adapters()
        if len(available_adapters) < 2:
            df, source = self.get_daily_basic_with_fallback(trade_date)
            return df, source, None
        primary_adapter = available_adapters[0]
        secondary_adapter = available_adapters[1]
        try:
            logger.info(
                f"Access to data for consistency check:{primary_adapter.name} vs {secondary_adapter.name}"
            )
            primary_data = primary_adapter.get_daily_basic(trade_date)
            secondary_data = secondary_adapter.get_daily_basic(trade_date)
            if primary_data is None or primary_data.empty:
                logger.warning(f"Main data source{primary_adapter.name}Failed, use fallback")
                df, source = self.get_daily_basic_with_fallback(trade_date)
                return df, source, None
            if secondary_data is None or secondary_data.empty:
                logger.warning(f"Data source{secondary_adapter.name}Failed, use main data source")
                return primary_data, primary_adapter.name, None
            if self.consistency_checker:
                consistency_result = self.consistency_checker.check_daily_basic_consistency(
                    primary_data,
                    secondary_data,
                    primary_adapter.name,
                    secondary_adapter.name,
                )
                final_data, resolution_strategy = self.consistency_checker.resolve_data_conflicts(
                    primary_data, secondary_data, consistency_result
                )
                consistency_report = {
                    'is_consistent': consistency_result.is_consistent,
                    'confidence_score': consistency_result.confidence_score,
                    'recommended_action': consistency_result.recommended_action,
                    'resolution_strategy': resolution_strategy,
                    'differences': consistency_result.differences,
                    'primary_source': primary_adapter.name,
                    'secondary_source': secondary_adapter.name,
                }
                logger.info(
                    f"Data consistency check complete: confidence ={consistency_result.confidence_score:.2f},Text={consistency_result.recommended_action}"
                )
                return final_data, primary_adapter.name, consistency_report
            else:
                logger.warning("⚠️ Consistency check not available, using main data source")
                return primary_data, primary_adapter.name, None
        except Exception as e:
            logger.error(f"Consistency check failed:{e}")
            df, source = self.get_daily_basic_with_fallback(trade_date)
            return df, source, None



    def get_kline_with_fallback(self, code: str, period: str = "day", limit: int = 120, adj: Optional[str] = None) -> Tuple[Optional[List[Dict]], Optional[str]]:
        """Try K-line on priority, return (items, source)"""
        available_adapters = self.get_available_adapters()
        for adapter in available_adapters:
            try:
                logger.info(f"Trying to fetch kline from {adapter.name}")
                items = adapter.get_kline(code=code, period=period, limit=limit, adj=adj)
                if items:
                    return items, adapter.name
            except Exception as e:
                logger.error(f"Failed to fetch kline from {adapter.name}: {e}")
                continue
        return None, None

    def get_news_with_fallback(self, code: str, days: int = 2, limit: int = 50, include_announcements: bool = True) -> Tuple[Optional[List[Dict]], Optional[str]]:
        """Try to get news and announcements on priority, return (items, source)"""
        available_adapters = self.get_available_adapters()
        for adapter in available_adapters:
            try:
                logger.info(f"Trying to fetch news from {adapter.name}")
                items = adapter.get_news(code=code, days=days, limit=limit, include_announcements=include_announcements)
                if items:
                    return items, adapter.name
            except Exception as e:
                logger.error(f"Failed to fetch news from {adapter.name}: {e}")
                continue
        return None, None
