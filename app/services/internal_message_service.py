"""Internal Message Data Service
Provide harmonized internal message storage, query and management functions
"""
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
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
class InternalMessageQueryParams:
    """Internal message query parameters"""
    symbol: Optional[str] = None
    symbols: Optional[List[str]] = None
    message_type: Optional[str] = None  # research_report/insider_info/analyst_note/meeting_minutes/internal_analysis
    category: Optional[str] = None  # fundamental_analysis/technical_analysis/market_sentiment/risk_assessment
    source_type: Optional[str] = None  # internal_research/insider/analyst/meeting/system_analysis
    department: Optional[str] = None
    author: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    importance: Optional[str] = None
    access_level: Optional[str] = None  # public/internal/restricted/confidential
    min_confidence: Optional[float] = None
    rating: Optional[str] = None  # strong_buy/buy/hold/sell/strong_sell
    keywords: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    limit: int = 50
    skip: int = 0
    sort_by: str = "created_time"
    sort_order: int = -1  # -1 for desc, 1 for asc


@dataclass
class InternalMessageStats:
    """Internal Message Statistics"""
    total_count: int = 0
    message_types: Dict[str, int] = field(default_factory=dict)
    categories: Dict[str, int] = field(default_factory=dict)
    departments: Dict[str, int] = field(default_factory=dict)
    importance_levels: Dict[str, int] = field(default_factory=dict)
    ratings: Dict[str, int] = field(default_factory=dict)
    avg_confidence: float = 0.0
    recent_count: int = 0  #Last 24 hours.


class InternalMessageService:
    """Internal Message Data Service"""
    
    def __init__(self):
        self.db = None
        self.collection = None
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def initialize(self):
        """Initialization services"""
        try:
            self.db = get_database()
            self.collection = self.db.internal_messages
            self.logger.info("✅ Internal message data service initialised successfully")
        except Exception as e:
            self.logger.error(f"The initialization of the internal message data service failed:{e}")
            raise
    
    async def _get_collection(self):
        """Get Collective Examples"""
        if self.collection is None:
            await self.initialize()
        return self.collection
    
    async def save_internal_messages(
        self, 
        messages: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """Batch Save Internal Messages

Args:
Messages: Internal Message List

Returns:
Save statistical information
"""
        if not messages:
            return {"saved": 0, "failed": 0}
        
        try:
            collection = await self._get_collection()
            
            #Prepare batch operations
            operations = []
            for message in messages:
                #Add Timetamp
                message["created_at"] = datetime.utcnow()
                message["updated_at"] = datetime.utcnow()
                
                #Use message id as unique identifier
                filter_dict = {
                    "message_id": message.get("message_id")
                }
                
                operations.append(ReplaceOne(filter_dict, message, upsert=True))
            
            #Execute Batch Operations
            result = await collection.bulk_write(operations, ordered=False)
            
            saved_count = result.upserted_count + result.modified_count
            self.logger.info(f"Can not open message{saved_count}/{len(messages)}")
            
            return {
                "saved": saved_count,
                "failed": len(messages) - saved_count,
                "upserted": result.upserted_count,
                "modified": result.modified_count
            }
            
        except BulkWriteError as e:
            self.logger.error(f"The internal message saver failed:{e.details}")
            return {
                "saved": e.details.get("nUpserted", 0) + e.details.get("nModified", 0),
                "failed": len(e.details.get("writeErrors", [])),
                "errors": e.details.get("writeErrors", [])
            }
        except Exception as e:
            self.logger.error(f"Can not open message{e}")
            return {"saved": 0, "failed": len(messages), "error": str(e)}
    
    async def query_internal_messages(
        self, 
        params: InternalMessageQueryParams
    ) -> List[Dict[str, Any]]:
        """Query Internal Message

Args:
Params: query parameters

Returns:
Internal Message List
"""
        try:
            collection = await self._get_collection()
            
            #Build query conditions
            query = {}
            
            if params.symbol:
                query["symbol"] = params.symbol
            elif params.symbols:
                query["symbol"] = {"$in": params.symbols}
            
            if params.message_type:
                query["message_type"] = params.message_type
            
            if params.category:
                query["category"] = params.category
            
            if params.source_type:
                query["source.type"] = params.source_type
            
            if params.department:
                query["source.department"] = params.department
            
            if params.author:
                query["source.author"] = params.author
            
            if params.start_time or params.end_time:
                time_query = {}
                if params.start_time:
                    time_query["$gte"] = params.start_time
                if params.end_time:
                    time_query["$lte"] = params.end_time
                query["created_time"] = time_query
            
            if params.importance:
                query["importance"] = params.importance
            
            if params.access_level:
                query["access_level"] = params.access_level
            
            if params.min_confidence:
                query["confidence_level"] = {"$gte": params.min_confidence}
            
            if params.rating:
                query["related_data.rating"] = params.rating
            
            if params.keywords:
                query["keywords"] = {"$in": params.keywords}
            
            if params.tags:
                query["tags"] = {"$in": params.tags}
            
            #Execute queries
            cursor = collection.find(query)
            
            #Sort
            cursor = cursor.sort(params.sort_by, params.sort_order)
            
            #Page Break
            cursor = cursor.skip(params.skip).limit(params.limit)
            
            #Get results
            messages = await cursor.to_list(length=params.limit)

            #Convert ObjectId to a string to avoid serialization errors in JSON
            messages = convert_objectid_to_str(messages)

            self.logger.debug(f"Other Organiser{len(messages)}Internal Message")
            return messages
            
        except Exception as e:
            self.logger.error(f"Internal message query failed:{e}")
            return []
    
    async def get_latest_messages(
        self, 
        symbol: str = None, 
        message_type: str = None,
        access_level: str = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get an update on the inside."""
        params = InternalMessageQueryParams(
            symbol=symbol,
            message_type=message_type,
            access_level=access_level,
            limit=limit,
            sort_by="created_time",
            sort_order=-1
        )
        return await self.query_internal_messages(params)
    
    async def search_messages(
        self, 
        query: str, 
        symbol: str = None,
        access_level: str = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Other Organiser"""
        try:
            collection = await self._get_collection()
            
            #Build search conditions
            search_query = {
                "$text": {"$search": query}
            }
            
            if symbol:
                search_query["symbol"] = symbol
            
            if access_level:
                search_query["access_level"] = access_level
            
            #Execute Search
            cursor = collection.find(
                search_query,
                {"score": {"$meta": "textScore"}}
            ).sort([("score", {"$meta": "textScore"})])
            
            messages = await cursor.limit(limit).to_list(length=limit)
            
            self.logger.debug(f"Other Organiser{len(messages)}Can not open message")
            return messages
            
        except Exception as e:
            self.logger.error(f"Internal message search failed:{e}")
            return []
    
    async def get_research_reports(
        self, 
        symbol: str = None,
        department: str = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Access to studies"""
        params = InternalMessageQueryParams(
            symbol=symbol,
            message_type="research_report",
            department=department,
            limit=limit,
            sort_by="created_time",
            sort_order=-1
        )
        return await self.query_internal_messages(params)
    
    async def get_analyst_notes(
        self, 
        symbol: str = None,
        author: str = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get the analyst's notes."""
        params = InternalMessageQueryParams(
            symbol=symbol,
            message_type="analyst_note",
            author=author,
            limit=limit,
            sort_by="created_time",
            sort_order=-1
        )
        return await self.query_internal_messages(params)
    
    async def get_internal_statistics(
        self, 
        symbol: str = None,
        start_time: datetime = None,
        end_time: datetime = None
    ) -> InternalMessageStats:
        """Get internal news statistics"""
        try:
            collection = await self._get_collection()
            
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
                match_stage["created_time"] = time_query
            
            #Aggregation Conduit
            pipeline = []
            if match_stage:
                pipeline.append({"$match": match_stage})
            
            pipeline.extend([
                {
                    "$group": {
                        "_id": None,
                        "total_count": {"$sum": 1},
                        "avg_confidence": {"$avg": "$confidence_level"},
                        "message_types": {"$push": "$message_type"},
                        "categories": {"$push": "$category"},
                        "departments": {"$push": "$source.department"},
                        "importance_levels": {"$push": "$importance"},
                        "ratings": {"$push": "$related_data.rating"}
                    }
                }
            ])
            
            #Execute Convergence
            result = await collection.aggregate(pipeline).to_list(length=1)
            
            if result:
                stats_data = result[0]
                
                #Number of statistical categories
                def count_items(items):
                    counts = {}
                    for item in items:
                        if item:
                            counts[item] = counts.get(item, 0) + 1
                    return counts
                
                return InternalMessageStats(
                    total_count=stats_data.get("total_count", 0),
                    message_types=count_items(stats_data.get("message_types", [])),
                    categories=count_items(stats_data.get("categories", [])),
                    departments=count_items(stats_data.get("departments", [])),
                    importance_levels=count_items(stats_data.get("importance_levels", [])),
                    ratings=count_items(stats_data.get("ratings", [])),
                    avg_confidence=stats_data.get("avg_confidence", 0.0)
                )
            else:
                return InternalMessageStats()
                
        except Exception as e:
            self.logger.error(f"Internal news count failed:{e}")
            return InternalMessageStats()


#Examples of global services
_internal_message_service = None

async def get_internal_message_service() -> InternalMessageService:
    """Examples of accessing internal message data services"""
    global _internal_message_service
    if _internal_message_service is None:
        _internal_message_service = InternalMessageService()
        await _internal_message_service.initialize()
    return _internal_message_service
