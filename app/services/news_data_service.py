"""News data services
Provide unified information data storage, query and management functions
"""
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
from pymongo import ReplaceOne
from pymongo.errors import BulkWriteError
from bson import ObjectId

from app.core.database import get_database

logger = logging.getLogger(__name__)


def convert_objectid_to_str(data: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
    """Convert MongoDB ObjectId to a string to avoid a serialization error by JSON

    Args:
        Data: Single document or list of documents

    Returns:
        Converted Data
    """
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and '_id' in item:
                item['_id'] = str(item['_id'])
        return data
    elif isinstance(data, dict):
        if '_id' in data:
            data['_id'] = str(data['_id'])
        return data
    return data


@dataclass
class NewsQueryParams:
    """News query parameters"""
    symbol: Optional[str] = None
    symbols: Optional[List[str]] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    category: Optional[str] = None
    sentiment: Optional[str] = None
    importance: Optional[str] = None
    data_source: Optional[str] = None
    keywords: Optional[List[str]] = None
    limit: int = 50
    skip: int = 0
    sort_by: str = "publish_time"
    sort_order: int = -1  # -1 for desc, 1 for asc


@dataclass
class NewsStats:
    """News statistics"""
    total_count: int = 0
    positive_count: int = 0
    negative_count: int = 0
    neutral_count: int = 0
    high_importance_count: int = 0
    medium_importance_count: int = 0
    low_importance_count: int = 0
    categories: Dict[str, int] = None
    sources: Dict[str, int] = None
    
    def __post_init__(self):
        if self.categories is None:
            self.categories = {}
        if self.sources is None:
            self.sources = {}


class NewsDataService:
    """News data services"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._db = None
        self._collection = None
        self._indexes_ensured = False

    async def _ensure_indexes(self):
        """Ensure the necessary index exists"""
        if self._indexes_ensured:
            return

        try:
            collection = self._get_collection()
            self.logger.info("Check and create an index of news data...")

            #1. Unique index: Prevention of duplicate news (URL+title + release time)
            await collection.create_index([
                ("url", 1),
                ("title", 1),
                ("publish_time", 1)
            ], unique=True, name="url_title_time_unique", background=True)

            #2. Stock code index (search for information on single stocks)
            await collection.create_index([("symbol", 1)], name="symbol_index", background=True)

            #Multi-stock code index (search for information on multiple stocks)
            await collection.create_index([("symbols", 1)], name="symbols_index", background=True)

            #4. Publication of time index (check by time frame)
            await collection.create_index([("publish_time", -1)], name="publish_time_desc", background=True)

            #5. Composite index: stock code + release time (common query)
            await collection.create_index([
                ("symbol", 1),
                ("publish_time", -1)
            ], name="symbol_time_index", background=True)

            #6. Data source index (screened by data source)
            await collection.create_index([("data_source", 1)], name="data_source_index", background=True)

            #Classification index (screening by news category)
            await collection.create_index([("category", 1)], name="category_index", background=True)

            #8. Emotional index (in emotional screening)
            await collection.create_index([("sentiment", 1)], name="sentiment_index", background=True)

            #9. Index of importance (screened by importance)
            await collection.create_index([("importance", 1)], name="importance_index", background=True)

            #10. Update time index (data maintenance)
            await collection.create_index([("updated_at", -1)], name="updated_at_index", background=True)

            self._indexes_ensured = True
            self.logger.info("News data index check completed")
        except Exception as e:
            #Index creation failure should not prevent service startup
            self.logger.warning(f"Warning (possibly exists) when creating index:{e}")

    def _get_collection(self):
        """Access to news data sets"""
        if self._collection is None:
            self._db = get_database()
            self._collection = self._db.stock_news
        return self._collection
    
    async def save_news_data(
        self,
        news_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        data_source: str,
        market: str = "CN"
    ) -> int:
        """Preservation of news data

        Args:
            News data: News data (single or multiple)
            Data source: Data source identifier
            Market sign

        Returns:
            Number of records kept
        """
        try:
            #🔥 Ensure Index exists (created on first call)
            await self._ensure_indexes()

            collection = self._get_collection()
            now = datetime.utcnow()
            
            #Standardized data
            if isinstance(news_data, dict):
                news_list = [news_data]
            else:
                news_list = news_data
            
            if not news_list:
                return 0
            
            #Prepare batch operations
            operations = []

            for i, news in enumerate(news_list):
                #Standardized public information data
                standardized_news = self._standardize_news_data(
                    news, data_source, market, now
                )

                #🔍 Detailed information for recording the first three data
                if i < 3:
                    self.logger.info(f"It's a standardized story.{i+1}:")
                    self.logger.info(f"      symbol: {standardized_news.get('symbol')}")
                    self.logger.info(f"      title: {standardized_news.get('title', '')[:50]}...")
                    self.logger.info(f"      publish_time: {standardized_news.get('publish_time')} (type: {type(standardized_news.get('publish_time'))})")
                    self.logger.info(f"      url: {standardized_news.get('url', '')[:80]}...")

                #Use URL, title and release time as unique identifier
                filter_query = {
                    "url": standardized_news["url"],
                    "title": standardized_news["title"],
                    "publish_time": standardized_news["publish_time"]
                }

                operations.append(
                    ReplaceOne(
                        filter_query,
                        standardized_news,
                        upsert=True
                    )
                )
            
            #Execute Batch Operations
            if operations:
                result = await collection.bulk_write(operations)
                saved_count = result.upserted_count + result.modified_count
                
                self.logger.info(f"News data saved:{saved_count}Article record (data source:{data_source})")
                return saved_count
            
            return 0
            
        except BulkWriteError as e:
            #Process batch writing error but not completely failed
            write_errors = e.details.get('writeErrors', [])
            error_count = len(write_errors)
            self.logger.warning(f"Some news data storage failed:{error_count}Error")

            #Record detailed error information
            for i, error in enumerate(write_errors[:3], 1):  #Only the first three errors were recorded.
                error_msg = error.get('errmsg', 'Unknown error')
                error_code = error.get('code', 'N/A')
                self.logger.warning(f"Error{i}: [Code {error_code}] {error_msg}")

            #Calculate the number of successfully saved
            success_count = len(operations) - error_count
            if success_count > 0:
                self.logger.info(f"Save successfully{success_count}Press data")

            return success_count
            
        except Exception as e:
            self.logger.error(f"Can not get folder: %s: %s{e}")
            return 0

    def save_news_data_sync(
        self,
        news_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        data_source: str,
        market: str = "CN"
    ) -> int:
        """Save news data (sync version)
        Use a synchronized PyMongo client for non-show context

        Args:
            News data: News data (single or multiple)
            Data source: Data source identifier
            Market sign

        Returns:
            Number of records kept
        """
        try:
            from app.core.database import get_mongo_db_synchronous

            #Get Sync database connections
            db = get_mongo_db_synchronous()
            collection = db.stock_news
            now = datetime.utcnow()

            #Standardized data
            if isinstance(news_data, dict):
                news_list = [news_data]
            else:
                news_list = news_data

            if not news_list:
                return 0

            #Prepare batch operations
            operations = []

            self.logger.info(f"Standardize.{len(news_list)}News data...")

            for i, news in enumerate(news_list, 1):
                #Standardized public information data
                standardized_news = self._standardize_news_data(news, data_source, market, now)

                #Recording details of the first three stories
                if i <= 3:
                    self.logger.info(f"It's a standardized story.{i}:")
                    self.logger.info(f"      symbol: {standardized_news.get('symbol')}")
                    self.logger.info(f"      title: {standardized_news.get('title', '')[:50]}...")
                    publish_time = standardized_news.get('publish_time')
                    self.logger.info(f"      publish_time: {publish_time} (type: {type(publish_time)})")
                    self.logger.info(f"      url: {standardized_news.get('url', '')[:60]}...")

                #Use URL+title+ time for publication only
                filter_query = {
                    "url": standardized_news.get("url"),
                    "title": standardized_news.get("title"),
                    "publish_time": standardized_news.get("publish_time")
                }

                operations.append(
                    ReplaceOne(
                        filter_query,
                        standardized_news,
                        upsert=True
                    )
                )

            #Perform batch operations (sync)
            if operations:
                result = collection.bulk_write(operations)
                saved_count = result.upserted_count + result.modified_count

                self.logger.info(f"News data saved:{saved_count}Article record (data source:{data_source})")
                return saved_count

            return 0

        except BulkWriteError as e:
            #Process batch writing error but not completely failed
            write_errors = e.details.get('writeErrors', [])
            error_count = len(write_errors)
            self.logger.warning(f"Some news data storage failed:{error_count}Error")

            #Record detailed error information
            for i, error in enumerate(write_errors[:3], 1):  #Only the first three errors were recorded.
                error_msg = error.get('errmsg', 'Unknown error')
                error_code = error.get('code', 'N/A')
                self.logger.warning(f"Error{i}: [Code {error_code}] {error_msg}")

            #Calculate the number of successfully saved
            success_count = len(operations) - error_count
            if success_count > 0:
                self.logger.info(f"Save successfully{success_count}Press data")

            return success_count

        except Exception as e:
            self.logger.error(f"Can not get folder: %s: %s{e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return 0

    def _standardize_news_data(
        self,
        news_data: Dict[str, Any],
        data_source: str,
        market: str,
        now: datetime
    ) -> Dict[str, Any]:
        """Standardized public information data"""
        
        #Extract Basic Information
        symbol = news_data.get("symbol")
        symbols = news_data.get("symbols", [])
        
        #Add to symbols if key stock codes are available but symbols are empty
        if symbol and symbol not in symbols:
            symbols = [symbol] + symbols
        
        #Standardized data structure
        standardized = {
            #Basic information
            "symbol": symbol,
            "full_symbol": self._get_full_symbol(symbol, market) if symbol else None,
            "market": market,
            "symbols": symbols,
            
            #Public information content
            "title": news_data.get("title", ""),
            "content": news_data.get("content", ""),
            "summary": news_data.get("summary", ""),
            "url": news_data.get("url", ""),
            "source": news_data.get("source", ""),
            "author": news_data.get("author", ""),
            
            #Time Information
            "publish_time": self._parse_datetime(news_data.get("publish_time")),
            
            #Classification and labelling
            "category": news_data.get("category", "general"),
            "sentiment": news_data.get("sentiment", "neutral"),
            "sentiment_score": self._safe_float(news_data.get("sentiment_score")),
            "keywords": news_data.get("keywords", []),
            "importance": news_data.get("importance", "medium"),
            #Note: does not contain alanguage field, avoiding conflict with the MongoDB text index

            #Metadata
            "data_source": data_source,
            "created_at": now,
            "updated_at": now,
            "version": 1
        }
        
        return standardized
    
    def _get_full_symbol(self, symbol: str, market: str) -> str:
        """Get the full stock code"""
        if not symbol:
            return None
        
        if market == "CN":
            if len(symbol) == 6:
                if symbol.startswith(('60', '68')):
                    return f"{symbol}.SH"
                elif symbol.startswith(('00', '30')):
                    return f"{symbol}.SZ"
        
        return symbol
    
    def _parse_datetime(self, dt_value) -> Optional[datetime]:
        """Other Organiser"""
        if dt_value is None:
            return None
        
        if isinstance(dt_value, datetime):
            return dt_value
        
        if isinstance(dt_value, str):
            try:
                #Try multiple date formats
                formats = [
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%d",
                ]
                
                for fmt in formats:
                    try:
                        return datetime.strptime(dt_value, fmt)
                    except ValueError:
                        continue
                
                #If they fail, return to the current time.
                self.logger.warning(f"Could not close temporary folder: %s{dt_value}")
                return datetime.utcnow()
                
            except Exception:
                return datetime.utcnow()
        
        return datetime.utcnow()
    
    def _safe_float(self, value) -> Optional[float]:
        """Convert safe to floating point"""
        if value is None:
            return None
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    async def query_news(self, params: NewsQueryParams) -> List[Dict[str, Any]]:
        """Query news data

        Args:
            Params: query parameters

        Returns:
            News Data List
        """
        try:
            collection = self._get_collection()

            self.logger.info(f"[Query news]")
            self.logger.info(f"Parameter: symbol={params.symbol}, start_time={params.start_time}, end_time={params.end_time}, limit={params.limit}")

            #Build query conditions
            query = {}

            if params.symbol:
                query["symbol"] = params.symbol
                self.logger.info(f"Add query condition: symbol={params.symbol}")

            if params.symbols:
                query["symbols"] = {"$in": params.symbols}
                self.logger.info(f"Add query condition: symbols in{params.symbols}")

            if params.start_time or params.end_time:
                time_query = {}
                if params.start_time:
                    time_query["$gte"] = params.start_time
                if params.end_time:
                    time_query["$lte"] = params.end_time
                query["publish_time"] = time_query
                self.logger.info(f"Add query condition: public time between{params.start_time} and {params.end_time}")

            if params.category:
                query["category"] = params.category
                self.logger.info(f"Add Query Conditions:{params.category}")

            if params.sentiment:
                query["sentiment"] = params.sentiment
                self.logger.info(f"Adding Query Conditions:{params.sentiment}")

            if params.importance:
                query["importance"] = params.importance
                self.logger.info(f"Add query condition:{params.importance}")

            if params.data_source:
                query["data_source"] = params.data_source
                self.logger.info(f"Add Query Conditions: Data source={params.data_source}")

            if params.keywords:
                #Text Search
                query["$text"] = {"$search": " ".join(params.keywords)}
                self.logger.info(f"Add query condition: text search={params.keywords}")

            self.logger.info(f"Final search condition:{query}")

            #Total first count
            total_count = await collection.count_documents(query)
            self.logger.info(f"Total records eligible in database:{total_count}")

            #Execute queries
            cursor = collection.find(query)

            #Sort
            cursor = cursor.sort(params.sort_by, params.sort_order)
            self.logger.info(f"Sort:{params.sort_by} ({params.sort_order})")

            #Page Break
            cursor = cursor.skip(params.skip).limit(params.limit)
            self.logger.info(f"Page Break: Skip={params.skip}, limit={params.limit}")

            #Get results
            results = await cursor.to_list(length=None)
            self.logger.info(f"Query returns:{len(results)}Notes")

            #Convert ObjectId to a string to avoid serialization errors in JSON
            results = convert_objectid_to_str(results)

            if results:
                self.logger.info(f"The first 3 previews:")
                for i, r in enumerate(results[:3], 1):
                    self.logger.info(f"      {i}. symbol={r.get('symbol')}, title={r.get('title', 'N/A')[:50]}..., publish_time={r.get('publish_time')}")
            else:
                self.logger.warning(f"Other Organiser")

            self.logger.info(f"[Query news] Query completed, return{len(results)}Notes")
            return results

        except Exception as e:
            self.logger.error(f"The search for news data failed:{e}", exc_info=True)
            return []
    
    async def get_latest_news(
        self,
        symbol: str = None,
        limit: int = 10,
        hours_back: int = 24
    ) -> List[Dict[str, Any]]:
        """Get the latest news.

        Args:
            symbol: stock code, empty for all news
            Limited number of returns
            Hours back: backtrace hours

        Returns:
            Newslist Update
        """
        start_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        params = NewsQueryParams(
            symbol=symbol,
            start_time=start_time,
            limit=limit,
            sort_by="publish_time",
            sort_order=-1
        )
        
        return await self.query_news(params)
    
    async def get_news_statistics(
        self,
        symbol: str = None,
        start_time: datetime = None,
        end_time: datetime = None
    ) -> NewsStats:
        """Access to news statistics

        Args:
            symbol: stock code
            Start time: start time
            End time: End time

        Returns:
            News statistics
        """
        try:
            collection = self._get_collection()
            
            #Build Match Conditions
            match_stage = {}
            
            if symbol:
                match_stage["symbol"] = symbol
            
            if start_time or end_time:
                time_query = {}
                if start_time:
                    time_query["$gte"] = start_time
                if end_time:
                    time_query["$lte"] = end_time
                match_stage["publish_time"] = time_query
            
            #Aggregation Conduit
            pipeline = []
            
            if match_stage:
                pipeline.append({"$match": match_stage})
            
            pipeline.extend([
                {
                    "$group": {
                        "_id": None,
                        "total_count": {"$sum": 1},
                        "positive_count": {
                            "$sum": {"$cond": [{"$eq": ["$sentiment", "positive"]}, 1, 0]}
                        },
                        "negative_count": {
                            "$sum": {"$cond": [{"$eq": ["$sentiment", "negative"]}, 1, 0]}
                        },
                        "neutral_count": {
                            "$sum": {"$cond": [{"$eq": ["$sentiment", "neutral"]}, 1, 0]}
                        },
                        "high_importance_count": {
                            "$sum": {"$cond": [{"$eq": ["$importance", "high"]}, 1, 0]}
                        },
                        "medium_importance_count": {
                            "$sum": {"$cond": [{"$eq": ["$importance", "medium"]}, 1, 0]}
                        },
                        "low_importance_count": {
                            "$sum": {"$cond": [{"$eq": ["$importance", "low"]}, 1, 0]}
                        },
                        "categories": {"$push": "$category"},
                        "sources": {"$push": "$data_source"}
                    }
                }
            ])
            
            #Execute Convergence
            result = await collection.aggregate(pipeline).to_list(length=1)
            
            if result:
                data = result[0]
                
                #Statistical classifications and sources
                categories = {}
                for cat in data.get("categories", []):
                    categories[cat] = categories.get(cat, 0) + 1
                
                sources = {}
                for src in data.get("sources", []):
                    sources[src] = sources.get(src, 0) + 1
                
                return NewsStats(
                    total_count=data.get("total_count", 0),
                    positive_count=data.get("positive_count", 0),
                    negative_count=data.get("negative_count", 0),
                    neutral_count=data.get("neutral_count", 0),
                    high_importance_count=data.get("high_importance_count", 0),
                    medium_importance_count=data.get("medium_importance_count", 0),
                    low_importance_count=data.get("low_importance_count", 0),
                    categories=categories,
                    sources=sources
                )
            
            return NewsStats()
            
        except Exception as e:
            self.logger.error(f"Access to news statistics failed:{e}")
            return NewsStats()
    
    async def delete_old_news(self, days_to_keep: int = 90) -> int:
        """Delete Expired News

        Args:
            days to keep: Keep days

        Returns:
            Number of records removed
        """
        try:
            collection = self._get_collection()
            
            cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
            
            result = await collection.delete_many({
                "publish_time": {"$lt": cutoff_date}
            })
            
            deleted_count = result.deleted_count
            self.logger.info(f"Delete the expired news:{deleted_count}Notes")
            
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"@❌ > Delete expired news failed:{e}")
            return 0

    async def search_news(
        self,
        query_text: str,
        symbol: str = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Full text search news

        Args:
            Query text: Search text
            symbol: stock code filter
            Limited number of returns

        Returns:
            Search result list
        """
        try:
            collection = self._get_collection()

            #Build query conditions
            query = {"$text": {"$search": query_text}}

            if symbol:
                query["symbol"] = symbol

            #Execute search, sort by relevance
            cursor = collection.find(
                query,
                {"score": {"$meta": "textScore"}}
            ).sort([("score", {"$meta": "textScore"})])

            cursor = cursor.limit(limit)
            results = await cursor.to_list(length=None)

            #Convert ObjectId to a string to avoid serialization errors in JSON
            results = convert_objectid_to_str(results)

            self.logger.info(f"Full text search returned{len(results)}Outcome")
            return results

        except Exception as e:
            self.logger.error(f"Full text search failed:{e}")
            return []


#Examples of global services
_service_instance = None

async def get_news_data_service() -> NewsDataService:
    """Examples of access to news data services"""
    global _service_instance
    if _service_instance is None:
        _service_instance = NewsDataService()
        logger.info("✅News data service initiated successfully")
    return _service_instance
