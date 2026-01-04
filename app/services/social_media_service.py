"""Social Media Data Service
Provide unified media message storage, query and analysis functions
"""
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import logging
from pymongo import ReplaceOne
from pymongo.errors import BulkWriteError

from app.core.database import get_database

logger = logging.getLogger(__name__)


@dataclass
class SocialMediaQueryParams:
    """Social Message Query Parameters"""
    symbol: Optional[str] = None
    symbols: Optional[List[str]] = None
    platform: Optional[str] = None  # weibo/wechat/douyin/xiaohongshu/zhihu/twitter/reddit
    message_type: Optional[str] = None  # post/comment/repost/reply
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    sentiment: Optional[str] = None
    importance: Optional[str] = None
    min_influence_score: Optional[float] = None
    min_engagement_rate: Optional[float] = None
    verified_only: bool = False
    keywords: Optional[List[str]] = None
    hashtags: Optional[List[str]] = None
    limit: int = 50
    skip: int = 0
    sort_by: str = "publish_time"
    sort_order: int = -1  # -1 for desc, 1 for asc


@dataclass
class SocialMediaStats:
    """Social media statistics"""
    total_count: int = 0
    positive_count: int = 0
    negative_count: int = 0
    neutral_count: int = 0
    platforms: Dict[str, int] = field(default_factory=dict)
    message_types: Dict[str, int] = field(default_factory=dict)
    top_hashtags: List[Dict[str, Any]] = field(default_factory=list)
    avg_engagement_rate: float = 0.0
    total_views: int = 0
    total_likes: int = 0
    total_shares: int = 0
    total_comments: int = 0


class SocialMediaService:
    """Social Media Data Service"""
    
    def __init__(self):
        self.db = None
        self.collection = None
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def initialize(self):
        """Initialization services"""
        try:
            self.db = get_database()
            self.collection = self.db.social_media_messages
            self.logger.info("The social media data service was successfully initiated")
        except Exception as e:
            self.logger.error(f"The initialization of the social media data service failed:{e}")
            raise
    
    async def _get_collection(self):
        """Get Collective Examples"""
        if self.collection is None:
            await self.initialize()
        return self.collection
    
    async def save_social_media_messages(
        self, 
        messages: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """Batch Save Media Messages

        Args:
            Messages: Social Message List

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
                
                #Use message id and platform as unique identifiers
                filter_dict = {
                    "message_id": message.get("message_id"),
                    "platform": message.get("platform")
                }
                
                operations.append(ReplaceOne(filter_dict, message, upsert=True))
            
            #Execute Batch Operations
            result = await collection.bulk_write(operations, ordered=False)
            
            saved_count = result.upserted_count + result.modified_count
            self.logger.info(f"Media message batch saved:{saved_count}/{len(messages)}")
            
            return {
                "saved": saved_count,
                "failed": len(messages) - saved_count,
                "upserted": result.upserted_count,
                "modified": result.modified_count
            }
            
        except BulkWriteError as e:
            self.logger.error(f"The mass saving of media messages failed:{e.details}")
            return {
                "saved": e.details.get("nUpserted", 0) + e.details.get("nModified", 0),
                "failed": len(e.details.get("writeErrors", [])),
                "errors": e.details.get("writeErrors", [])
            }
        except Exception as e:
            self.logger.error(f"Media message storage failed:{e}")
            return {"saved": 0, "failed": len(messages), "error": str(e)}
    
    async def query_social_media_messages(
        self, 
        params: SocialMediaQueryParams
    ) -> List[Dict[str, Any]]:
        """Search for media messages

        Args:
            Params: query parameters

        Returns:
            Social Message List
        """
        try:
            collection = await self._get_collection()
            
            #Build query conditions
            query = {}
            
            if params.symbol:
                query["symbol"] = params.symbol
            elif params.symbols:
                query["symbol"] = {"$in": params.symbols}
            
            if params.platform:
                query["platform"] = params.platform
            
            if params.message_type:
                query["message_type"] = params.message_type
            
            if params.start_time or params.end_time:
                time_query = {}
                if params.start_time:
                    time_query["$gte"] = params.start_time
                if params.end_time:
                    time_query["$lte"] = params.end_time
                query["publish_time"] = time_query
            
            if params.sentiment:
                query["sentiment"] = params.sentiment
            
            if params.importance:
                query["importance"] = params.importance
            
            if params.min_influence_score:
                query["author.influence_score"] = {"$gte": params.min_influence_score}
            
            if params.min_engagement_rate:
                query["engagement.engagement_rate"] = {"$gte": params.min_engagement_rate}
            
            if params.verified_only:
                query["author.verified"] = True
            
            if params.keywords:
                query["keywords"] = {"$in": params.keywords}
            
            if params.hashtags:
                query["hashtags"] = {"$in": params.hashtags}
            
            #Execute queries
            cursor = collection.find(query)
            
            #Sort
            cursor = cursor.sort(params.sort_by, params.sort_order)
            
            #Page Break
            cursor = cursor.skip(params.skip).limit(params.limit)
            
            #Get results
            messages = await cursor.to_list(length=params.limit)
            
            self.logger.debug(f"Other Organiser{len(messages)}A news agency.")
            return messages
            
        except Exception as e:
            self.logger.error(f"Media query failed:{e}")
            return []
    
    async def get_latest_messages(
        self, 
        symbol: str = None, 
        platform: str = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get an update on the media."""
        params = SocialMediaQueryParams(
            symbol=symbol,
            platform=platform,
            limit=limit,
            sort_by="publish_time",
            sort_order=-1
        )
        return await self.query_social_media_messages(params)
    
    async def search_messages(
        self, 
        query: str, 
        symbol: str = None,
        platform: str = None,
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
            
            if platform:
                search_query["platform"] = platform
            
            #Execute Search
            cursor = collection.find(
                search_query,
                {"score": {"$meta": "textScore"}}
            ).sort([("score", {"$meta": "textScore"})])
            
            messages = await cursor.limit(limit).to_list(length=limit)
            
            self.logger.debug(f"Other Organiser{len(messages)}Can not open message")
            return messages
            
        except Exception as e:
            self.logger.error(f"The media search failed:{e}")
            return []
    
    async def get_social_media_statistics(
        self, 
        symbol: str = None,
        start_time: datetime = None,
        end_time: datetime = None
    ) -> SocialMediaStats:
        """Access to media statistics"""
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
                        "total_views": {"$sum": "$engagement.views"},
                        "total_likes": {"$sum": "$engagement.likes"},
                        "total_shares": {"$sum": "$engagement.shares"},
                        "total_comments": {"$sum": "$engagement.comments"},
                        "avg_engagement_rate": {"$avg": "$engagement.engagement_rate"}
                    }
                }
            ])
            
            #Execute Convergence
            result = await collection.aggregate(pipeline).to_list(length=1)
            
            if result:
                stats_data = result[0]
                return SocialMediaStats(
                    total_count=stats_data.get("total_count", 0),
                    positive_count=stats_data.get("positive_count", 0),
                    negative_count=stats_data.get("negative_count", 0),
                    neutral_count=stats_data.get("neutral_count", 0),
                    total_views=stats_data.get("total_views", 0),
                    total_likes=stats_data.get("total_likes", 0),
                    total_shares=stats_data.get("total_shares", 0),
                    total_comments=stats_data.get("total_comments", 0),
                    avg_engagement_rate=stats_data.get("avg_engagement_rate", 0.0)
                )
            else:
                return SocialMediaStats()
                
        except Exception as e:
            self.logger.error(f"The media has failed:{e}")
            return SocialMediaStats()


#Examples of global services
_social_media_service = None

async def get_social_media_service() -> SocialMediaService:
    """Examples of accessing media data services"""
    global _social_media_service
    if _social_media_service is None:
        _social_media_service = SocialMediaService()
        await _social_media_service.initialize()
    return _social_media_service
