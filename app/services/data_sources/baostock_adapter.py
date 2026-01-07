"""
BaoStock data source adapter
"""
from typing import Optional
import logging
from datetime import datetime, timedelta
import pandas as pd

from .base import DataSourceAdapter

logger = logging.getLogger(__name__)


class BaoStockAdapter(DataSourceAdapter):
    """BaoStockdata source adapter"""

    def __init__(self):
        super().__init__()  #Call Parent Initialization
        #JBH FIXME: why not use BaoStockProvider @ tradingagents/dataflows/providers/china/baostock.py ?
        #           implement it with reference to TushareAdapter where TushareProvider is being used.

    @property
    def name(self) -> str:
        return "baostock"

    def _get_default_priority(self) -> int:
        return 1  #(the larger the number, the higher the priority)

    def is_available(self) -> bool:
        try:
            import baostock as bs  # noqa: F401
            return True
        except ImportError:
            return False

    def get_stock_list(self) -> Optional[pd.DataFrame]:
        if not self.is_available():
            return None
        try:
            import baostock as bs
            lg = bs.login()
            if lg.error_code != '0':
                logger.error(f"BaoStock: Login failed: {lg.error_msg}")
                return None
            try:
                logger.info("BaoStock: Querying stock basic info...")
                rs = bs.query_stock_basic()
                if rs.error_code != '0':
                    logger.error(f"BaoStock: Query failed: {rs.error_msg}")
                    return None
                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())
                if not data_list:
                    return None

                df = pd.DataFrame(data_list, columns=rs.fields)
                """                
                           code   code_name             ipoDate     outDate type status
                0     sh.000001      上证综合指数       1991-07-15            2      1
                1     sh.000002      上证A股指数        1992-02-21            2      1
                2     sh.000003      上证B股指数        1992-08-17            2      1
                3     sh.000004     上证工业类指数      1993-05-03            2      1
                4     sh.000005     上证商业类指数      1993-05-03            2      1
                ...         ...         ...               ...          ...  ...    ...
                7170  sz.399994  中证信息安全主题指数   2015-03-12            2      1
                7171  sz.399995    中证基建工程指数     2015-03-12            2      1
                7172  sz.399996    中证智能家居指数     2014-09-17            2      1
                7173  sz.399997      中证白酒指数      2015-01-21            2      1
                7174  sz.399998      中证煤炭指数      2015-02-13            2      1
                
                [7175 rows x 6 columns]
                """

                df = df[df['type'] == '1'] #filter df to only the rows where the type column equals the string '1', and reassigns df to that filtered subset.
                """
                           code code_name     ipoDate     outDate      type status
                742   sh.600000      浦发银行  1999-11-10                1      1
                743   sh.600001      邯郸钢铁  1998-01-22  2009-12-29    1      0
                744   sh.600002      齐鲁石化  1998-04-08  2006-04-24    1      0
                745   sh.600003     ST东北高  1999-08-10   2010-02-26    1      0
                746   sh.600004      白云机场  2003-04-28                1      1
                ...         ...       ...         ...         ...       ...    ...
                6803  sz.301667       纳百川  2025-12-23                 1      1
                6804  sz.301668      昊创瑞通  2025-09-26                1      1
                6805  sz.301678       新恒汇  2025-06-20                 1      1
                6806  sz.301687       新广益  2025-12-31                 1      1
                6807  sz.302132      中航成飞  2010-08-27                1      1
                
                [5501 rows x 6 columns]
                """

                df['symbol'] = df['code'].str.replace(r'^(sh|sz)\.', '', regex=True)
                df['ts_code'] = (
                    df['code'].str.replace('sh.', '').str.replace('sz.', '')
                    + df['code'].str.extract(r'^(sh|sz)\.').iloc[:, 0].str.upper().str.replace('SH', '.SH').str.replace('SZ', '.SZ')
                )
                df['name'] = df['code_name']
                df['area'] = ''

                #Access to industry information
                logger.info("BaoStock: Querying stock industry info...")
                industry_rs = bs.query_stock_industry()
                if industry_rs.error_code == '0':
                    industry_list = []
                    while (industry_rs.error_code == '0') & industry_rs.next():
                        industry_list.append(industry_rs.get_row_data())
                    if industry_list:
                        industry_df = pd.DataFrame(industry_list, columns=industry_rs.fields)

                        #Remove industry prefixes.
                        def clean_industry_name(industry_str):
                            if not industry_str or pd.isna(industry_str):
                                return ''
                            #Remove the letters and numeric codes (e. g. I65, C31, etc.) from the front using regular expressions
                            import re
                            cleaned = re.sub(r'^[A-Z]\d+', '', str(industry_str))
                            return cleaned.strip()

                        industry_df['industry_clean'] = industry_df['industry'].apply(clean_industry_name)

                        #Create industry map dictionary   FMT 0   
                        industry_map = dict(zip(industry_df['code'], industry_df['industry_clean']))
                        #Merge industry information with main DataFrame
                        df['industry'] = df['code'].map(industry_map).fillna('')
                        logger.info(f"BaoStock: Successfully mapped industry info for {len(industry_map)} stocks")
                    else:
                        df['industry'] = ''
                        logger.warning("BaoStock: No industry data returned")
                else:
                    df['industry'] = ''
                    logger.warning(f"BaoStock: Failed to query industry info: {industry_rs.error_msg}")

                df['market'] = '\u4e3b\u677f'
                df['list_date'] = ''
                logger.info(f"BaoStock: Successfully fetched {len(df)} stocks")
                return df[['symbol', 'name', 'ts_code', 'area', 'industry', 'market', 'list_date']]
            finally:
                bs.logout()
        except Exception as e:
            logger.error(f"BaoStock: Failed to fetch stock list: {e}")
            return None

    def get_daily_basic(self, trade_date: str, max_stocks: int = None) -> Optional[pd.DataFrame]:
        """Access to daily basic data (including PE, PB, total market value, etc.)

        Args:
            trade date: transaction date (YYYYMMDD)
            Max stocks: Max. Number of processed equities.
        """
        if not self.is_available():
            return None
        try:
            import baostock as bs
            logger.info(f"BaoStock: Attempting to get valuation data for {trade_date}")
            lg = bs.login()
            if lg.error_code != '0':
                logger.error(f"BaoStock: Login failed: {lg.error_msg}")
                return None
            try:
                logger.info("BaoStock: Querying stock basic info...")
                rs = bs.query_stock_basic()
                if rs.error_code != '0':
                    logger.error(f"BaoStock: Query stock list failed: {rs.error_msg}")
                    return None
                stock_list = []
                while (rs.error_code == '0') & rs.next():
                    stock_list.append(rs.get_row_data())
                if not stock_list:
                    logger.warning("BaoStock: No stocks found")
                    return None

                total_stocks = len([s for s in stock_list if len(s) > 5 and s[4] == '1' and s[5] == '1'])
                logger.info(f"BaoStock:{total_stocks}Only active stocks. Start processing.{'All' if max_stocks is None else f'Front{max_stocks}Only'}...")

                basic_data = []
                processed_count = 0
                failed_count = 0
                for stock in stock_list:
                    if max_stocks and processed_count >= max_stocks:
                        break
                    code = stock[0] if len(stock) > 0 else ''
                    name = stock[1] if len(stock) > 1 else ''
                    stock_type = stock[4] if len(stock) > 4 else '0'
                    status = stock[5] if len(stock) > 5 else '0'
                    if stock_type == '1' and status == '1':
                        try:
                            formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
                            #🔥 Access to valuation data and gross equity
                            rs_valuation = bs.query_history_k_data_plus(
                                code,
                                "date,code,close,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                                start_date=formatted_date,
                                end_date=formatted_date,
                                frequency="d",
                                adjustflag="3",
                            )
                            if rs_valuation.error_code == '0':
                                valuation_data = []
                                while (rs_valuation.error_code == '0') & rs_valuation.next():
                                    valuation_data.append(rs_valuation.get_row_data())
                                if valuation_data:
                                    row = valuation_data[0]
                                    symbol = code.replace('sh.', '').replace('sz.', '')
                                    ts_code = f"{symbol}.SH" if code.startswith('sh.') else f"{symbol}.SZ"
                                    pe_ttm = self._safe_float(row[3]) if len(row) > 3 else None
                                    pb_mrq = self._safe_float(row[4]) if len(row) > 4 else None
                                    ps_ttm = self._safe_float(row[5]) if len(row) > 5 else None
                                    pcf_ttm = self._safe_float(row[6]) if len(row) > 6 else None
                                    close_price = self._safe_float(row[2]) if len(row) > 2 else None

                                    #BaoStock does not directly provide total market value and gross equity
                                    #In order to avoid synchronized overtime, no additional API is called here for total equity.
                                    #total mv left blank, followed by additional data sources
                                    total_mv = None

                                    basic_data.append({
                                        'ts_code': ts_code,
                                        'trade_date': trade_date,
                                        'name': name,
                                        'pe': pe_ttm,  #Profits (TTM)
                                        'pb': pb_mrq,  #Net ratio (MRQ)
                                        'ps': ps_ttm,  #Marketing rate
                                        'pcf': pcf_ttm,  #Current rate
                                        'close': close_price,
                                        'total_mv': total_mv,  #BaoStock is not available.
                                        'turnover_rate': None,  #BaoStock not available
                                    })
                                    processed_count += 1

                                    #🔥For every 50 stocks processed to export progress log
                                    if processed_count % 50 == 0:
                                        progress_pct = (processed_count / total_stocks) * 100
                                        logger.info(f"BaoStock Sync Progress:{processed_count}/{total_stocks} ({progress_pct:.1f}Other Organiser{name}({ts_code})")
                                else:
                                    failed_count += 1
                            else:
                                failed_count += 1
                        except Exception as e:
                            failed_count += 1
                            if failed_count % 50 == 0:
                                logger.warning(f"BaoStock:{failed_count}Only stock acquisition failed")
                            logger.debug(f"BaoStock: Failed to get valuation for {code}: {e}")
                            continue
                if basic_data:
                    df = pd.DataFrame(basic_data)
                    logger.info(f"== sync, corrected by elderman =={len(df)}Only, failure.{failed_count}Date only{trade_date}")
                    return df
                else:
                    logger.warning(f"BaoStock: No valuation data obtained (failed){failed_count}Only)")
                    return None
            finally:
                bs.logout()
        except Exception as e:
            logger.error(f"BaoStock: Failed to fetch valuation data for {trade_date}: {e}")
            return None

    def _safe_float(self, value) -> Optional[float]:
        try:
            if value is None or value == '' or value == 'None':
                return None
            return float(value)
        except (ValueError, TypeError):
            return None


    def get_realtime_quotes(self):
        """Placeholder: BaoStock does not provide full-market realtime snapshot in our adapter.
        Return None to allow fallback to higher-priority sources.
        """
        if not self.is_available():
            return None
        return None

    def get_kline(self, code: str, period: str = "day", limit: int = 120, adj: Optional[str] = None):
        """BaoStock not used for K-line here; return None to allow fallback"""
        if not self.is_available():
            return None
        return None

    def get_news(self, code: str, days: int = 2, limit: int = 50, include_announcements: bool = True):
        """BaoStock does not provide news in this adapter; return None"""
        if not self.is_available():
            return None
        return None

        """Placeholder: BaoStock  does not provide full-market realtime snapshot in our adapter.
        Return None to allow fallback to higher-priority sources.
        """

    def find_latest_trade_date(self) -> Optional[str]:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        logger.info(f"BaoStock: Using yesterday as trade date: {yesterday}")
        return yesterday

