"""Use of statistical services
Management model usage records and cost statistics
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from collections import defaultdict

from app.core.database import get_mongo_db_async
from app.models.config import UsageRecord, UsageStatistics

logger = logging.getLogger("app.services.usage_statistics_service")


class UsageStatisticsService:
    """Use of statistical services"""
    
    def __init__(self):
        #Use the group names of tradencies
        self.collection_name = "token_usage"
    
    async def add_usage_record(self, record: UsageRecord) -> bool:
        """Add Usage Record"""
        try:
            db = get_mongo_db_async()
            collection = db[self.collection_name]

            record_dict = record.model_dump(exclude={"id"})
            result = await collection.insert_one(record_dict)

            logger.info(f"Use record successful:{record.provider}/{record.model_name}")
            return True
        except Exception as e:
            logger.error(f"Could not close temporary folder: %s{e}")
            return False
    
    async def get_usage_records(
        self,
        provider: Optional[str] = None,
        model_name: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100
    ) -> List[UsageRecord]:
        """Access to usage records"""
        try:
            db = get_mongo_db_async()
            collection = db[self.collection_name]
            
            #Build query conditions
            query = {}
            if provider:
                query["provider"] = provider
            if model_name:
                query["model_name"] = model_name
            if start_date or end_date:
                query["timestamp"] = {}
                if start_date:
                    query["timestamp"]["$gte"] = start_date.isoformat()
                if end_date:
                    query["timestamp"]["$lte"] = end_date.isoformat()
            
            #Query records
            cursor = collection.find(query).sort("timestamp", -1).limit(limit)
            records = []
            
            async for doc in cursor:
                doc["id"] = str(doc.pop("_id"))
                records.append(UsageRecord(**doc))
            
            logger.info(f"Successful access to records:{len(records)}Article")
            return records
        except Exception as e:
            logger.error(f"Can not get folder: %s: %s{e}")
            return []
    
    async def get_usage_statistics(
        self,
        days: int = 7,
        provider: Optional[str] = None,
        model_name: Optional[str] = None
    ) -> UsageStatistics:
        """Access to usage statistics"""
        try:
            db = get_mongo_db_async()
            collection = db[self.collection_name]
            
            #Calculate the time frame
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            #Build query conditions
            query = {
                "timestamp": {
                    "$gte": start_date.isoformat(),
                    "$lte": end_date.isoformat()
                }
            }
            if provider:
                query["provider"] = provider
            if model_name:
                query["model_name"] = model_name
            
            #Get all records
            cursor = collection.find(query)
            records = []
            async for doc in cursor:
                records.append(doc)
            
            #Statistics
            stats = UsageStatistics()
            stats.total_requests = len(records)

            #Cost by currency
            cost_by_currency = defaultdict(float)

            by_provider = defaultdict(lambda: {
                "requests": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "cost": 0.0,
                "cost_by_currency": defaultdict(float)
            })
            by_model = defaultdict(lambda: {
                "requests": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "cost": 0.0,
                "cost_by_currency": defaultdict(float)
            })
            by_date = defaultdict(lambda: {
                "requests": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "cost": 0.0,
                "cost_by_currency": defaultdict(float)
            })

            for record in records:
                cost = record.get("cost", 0.0)
                currency = record.get("currency", "CNY")

                #Total
                stats.total_input_tokens += record.get("input_tokens", 0)
                stats.total_output_tokens += record.get("output_tokens", 0)
                stats.total_cost += cost  #Keep backward compatibility
                cost_by_currency[currency] += cost

                #By supplier
                provider_key = record.get("provider", "unknown")
                by_provider[provider_key]["requests"] += 1
                by_provider[provider_key]["input_tokens"] += record.get("input_tokens", 0)
                by_provider[provider_key]["output_tokens"] += record.get("output_tokens", 0)
                by_provider[provider_key]["cost"] += cost
                by_provider[provider_key]["cost_by_currency"][currency] += cost

                #By Model
                model_key = f"{record.get('provider', 'unknown')}/{record.get('model_name', 'unknown')}"
                by_model[model_key]["requests"] += 1
                by_model[model_key]["input_tokens"] += record.get("input_tokens", 0)
                by_model[model_key]["output_tokens"] += record.get("output_tokens", 0)
                by_model[model_key]["cost"] += cost
                by_model[model_key]["cost_by_currency"][currency] += cost

                #By date
                timestamp = record.get("timestamp", "")
                if timestamp:
                    date_key = timestamp[:10]  # YYYY-MM-DD
                    by_date[date_key]["requests"] += 1
                    by_date[date_key]["input_tokens"] += record.get("input_tokens", 0)
                    by_date[date_key]["output_tokens"] += record.get("output_tokens", 0)
                    by_date[date_key]["cost"] += cost
                    by_date[date_key]["cost_by_currency"][currency] += cost

            #Convert default to normal dict (including embedded host by currence)
            stats.cost_by_currency = dict(cost_by_currency)
            stats.by_provider = {k: {**v, "cost_by_currency": dict(v["cost_by_currency"])} for k, v in by_provider.items()}
            stats.by_model = {k: {**v, "cost_by_currency": dict(v["cost_by_currency"])} for k, v in by_model.items()}
            stats.by_date = {k: {**v, "cost_by_currency": dict(v["cost_by_currency"])} for k, v in by_date.items()}
            
            logger.info(f"✅ for statistical success:{stats.total_requests}Notes")
            return stats
        except Exception as e:
            logger.error(f"Access to statistics failed:{e}")
            return UsageStatistics()
    
    async def get_cost_by_provider(self, days: int = 7) -> Dict[str, float]:
        """Access to cost statistics by supplier"""
        stats = await self.get_usage_statistics(days=days)
        return {
            provider: data["cost"]
            for provider, data in stats.by_provider.items()
        }
    
    async def get_cost_by_model(self, days: int = 7) -> Dict[str, float]:
        """Access to cost statistics by model"""
        stats = await self.get_usage_statistics(days=days)
        return {
            model: data["cost"]
            for model, data in stats.by_model.items()
        }
    
    async def get_daily_cost(self, days: int = 7) -> Dict[str, float]:
        """Access to daily cost statistics"""
        stats = await self.get_usage_statistics(days=days)
        return {
            date: data["cost"]
            for date, data in stats.by_date.items()
        }
    
    async def delete_old_records(self, days: int = 90) -> int:
        """Remove old record"""
        try:
            db = get_mongo_db_async()
            collection = db[self.collection_name]
            
            #Calculating cut-off date
            cutoff_date = datetime.now() - timedelta(days=days)
            
            #Remove old record
            result = await collection.delete_many({
                "timestamp": {"$lt": cutoff_date.isoformat()}
            })
            
            deleted_count = result.deleted_count
            logger.info(f"Delete the old record successfully:{deleted_count}Article")
            return deleted_count
        except Exception as e:
            logger.error(f"The old record has failed:{e}")
            return 0


#Create global instance
usage_statistics_service = UsageStatisticsService()

