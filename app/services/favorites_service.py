"""Self-selected Unit Services
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from bson import ObjectId

from app.core.database import get_mongo_db
from app.models.user import FavoriteStock
from app.services.quotes_service import get_quotes_service


class FavoritesService:
    """Self-selected Unit Services Group"""
    
    def __init__(self):
        self.db = None
    
    async def _get_db(self):
        """Get database connections"""
        if self.db is None:
            self.db = get_mongo_db()
        return self.db

    def _is_valid_object_id(self, user_id: str) -> bool:
        """Check if it's a valid ObjectId format
Note: Only the format is checked here, which does not represent the type of objectId actually stored in the database.
For compatibility, we use the user favorites pool to store ourselves. Unit
"""
        #Force return to False, united with user favorites
        return False

    def _format_favorite(self, favorite: Dict[str, Any]) -> Dict[str, Any]:
        """Formats the collection entry (basic information only, not real-time lines).
Lines will be enriched in bulk in get user favorites.
"""
        added_at = favorite.get("added_at")
        if isinstance(added_at, datetime):
            added_at = added_at.isoformat()
        return {
            "stock_code": favorite.get("stock_code"),
            "stock_name": favorite.get("stock_name"),
            "market": favorite.get("market", "A股"),
            "added_at": added_at,
            "tags": favorite.get("tags", []),
            "notes": favorite.get("notes", ""),
            "alert_price_high": favorite.get("alert_price_high"),
            "alert_price_low": favorite.get("alert_price_low"),
            #I'll fill it later.
            "current_price": None,
            "change_percent": None,
            "volume": None,
        }

    async def get_user_favorites(self, user_id: str) -> List[Dict[str, Any]]:
        """Retrieving a list of user-selected shares, and drawing a mass of real-time lines for enrichment (compatible string ID and ObjectId)."""
        db = await self._get_db()

        favorites: List[Dict[str, Any]] = []
        if self._is_valid_object_id(user_id):
            #Try using ObjectId query first
            user = await db.users.find_one({"_id": ObjectId(user_id)})
            #Try using a string query if ObjectiveId query failed
            if user is None:
                user = await db.users.find_one({"_id": user_id})
            favorites = (user or {}).get("favorite_stocks", [])
        else:
            doc = await db.user_favorites.find_one({"user_id": user_id})
            favorites = (doc or {}).get("favorites", [])

        #Format base fields first
        items = [self._format_favorite(fav) for fav in favorites]

        #Batch access to basic stock information (boards, etc.)
        codes = [it.get("stock_code") for it in items if it.get("stock_code")]
        if codes:
            try:
                #Access source priority configuration
                from app.core.unified_config import UnifiedConfigManager
                config = UnifiedConfigManager()
                data_source_configs = await config.get_data_source_configs_async()

                #Extract enabled data sources in order of priority
                enabled_sources = [
                    ds.type.lower() for ds in data_source_configs
                    if ds.enabled and ds.type.lower() in ['tushare', 'akshare', 'baostock']
                ]

                if not enabled_sources:
                    enabled_sources = ['tushare', 'akshare', 'baostock']

                preferred_source = enabled_sources[0] if enabled_sources else 'tushare'

                #Retrieving plate information from stock basic info (Querying highest priority data sources only)
                basic_info_coll = db["stock_basic_info"]
                cursor = basic_info_coll.find(
                    {"code": {"$in": codes}, "source": preferred_source},  #Add Data Source Filter
                    {"code": 1, "sse": 1, "market": 1, "_id": 0}
                )
                basic_docs = await cursor.to_list(length=None)
                basic_map = {str(d.get("code")).zfill(6): d for d in (basic_docs or [])}

                for it in items:
                    code = it.get("stock_code")
                    basic = basic_map.get(code)
                    if basic:
                        #Market field indicates a plate (main board, entrepreneurship board, science board, etc.)
                        it["board"] = basic.get("market", "-")
                        #sse field indicates an exchange (Shanghai Stock Exchange, Shenzhen Stock Exchange, etc.)
                        it["exchange"] = basic.get("sse", "-")
                    else:
                        it["board"] = "-"
                        it["exchange"] = "-"
            except Exception as e:
                #Set default when query failed
                for it in items:
                    it["board"] = "-"
                    it["exchange"] = "-"

        #Batch to get lines (prior to database market quotes, 30 seconds update)
        if codes:
            try:
                coll = db["market_quotes"]
                cursor = coll.find({"code": {"$in": codes}}, {"code": 1, "close": 1, "pct_chg": 1, "amount": 1})
                docs = await cursor.to_list(length=None)
                quotes_map = {str(d.get("code")).zfill(6): d for d in (docs or [])}
                for it in items:
                    code = it.get("stock_code")
                    q = quotes_map.get(code)
                    if q:
                        it["current_price"] = q.get("close")
                        it["change_percent"] = q.get("pct_chg")
                #Bottom: use an online source to complete the undead code (optional)
                missing = [c for c in codes if c not in quotes_map]
                if missing:
                    try:
                        quotes_online = await get_quotes_service().get_quotes(missing)
                        for it in items:
                            code = it.get("stock_code")
                            if it.get("current_price") is None:
                                q2 = quotes_online.get(code, {}) if quotes_online else {}
                                it["current_price"] = q2.get("close")
                                it["change_percent"] = q2.get("pct_chg")
                    except Exception:
                        pass
            except Exception:
                #Noone to avoid affecting basic functions when searching failed
                pass

        return items

    async def add_favorite(
        self,
        user_id: str,
        stock_code: str,
        stock_name: str,
        market: str = "A股",
        tags: List[str] = None,
        notes: str = "",
        alert_price_high: Optional[float] = None,
        alert_price_low: Optional[float] = None
    ) -> bool:
        """Add stocks to selected shares (compatible string ID and objectId)"""
        import logging
        logger = logging.getLogger("webapi")

        try:
            logger.info(f"[add favorite]{user_id}, stock_code={stock_code}")

            db = await self._get_db()
            logger.info(f"[add favorite] Database connection successfully accessed")

            favorite_stock = {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "market": market,
                "added_at": datetime.utcnow(),
                "tags": tags or [],
                "notes": notes,
                "alert_price_high": alert_price_high,
                "alert_price_low": alert_price_low
            }

            logger.info(f"[add favorite] Self-selected unit data complete:{favorite_stock}")

            is_oid = self._is_valid_object_id(user_id)
            logger.info(f"[add favorite]{is_oid}")

            if is_oid:
                logger.info(f"🔧 [add favorite] Add to usrs collection using ObjectiveId")

                #Try using ObjectId query first
                result = await db.users.update_one(
                    {"_id": ObjectId(user_id)},
                    {
                        "$push": {"favorite_stocks": favorite_stock},
                        "$setOnInsert": {"favorite_stocks": []}
                    }
                )
                logger.info(f"[add favorite]{result.matched_count}, modified_count={result.modified_count}")

                #Try using a string query if ObjectiveId query failed
                if result.matched_count == 0:
                    logger.info(f"🔧 [add favorite] ObjectId query failed, trying to use string ID query")
                    result = await db.users.update_one(
                        {"_id": user_id},
                        {
                            "$push": {"favorite_stocks": favorite_stock}
                        }
                    )
                    logger.info(f"[add favorite] stringID query result: made count={result.matched_count}, modified_count={result.modified_count}")

                success = result.matched_count > 0
                logger.info(f"[add favorite]{success}")
                return success
            else:
                logger.info(f"🔧 [add favorite] Add by string ID to user favorites")
                result = await db.user_favorites.update_one(
                    {"user_id": user_id},
                    {
                        "$setOnInsert": {"user_id": user_id, "created_at": datetime.utcnow()},
                        "$push": {"favorites": favorite_stock},
                        "$set": {"updated_at": datetime.utcnow()}
                    },
                    upsert=True
                )
                logger.info(f"== sync, corrected by elderman =={result.matched_count}, modified_count={result.modified_count}, upserted_id={result.upserted_id}")
                logger.info(f"[add favorite]")
                return True
        except Exception as e:
            logger.error(f"[add favorite] Add the selected share anomaly:{type(e).__name__}: {str(e)}", exc_info=True)
            raise

    async def remove_favorite(self, user_id: str, stock_code: str) -> bool:
        """Remove shares from selected shares (compatible string ID with ObjectId)"""
        db = await self._get_db()

        if self._is_valid_object_id(user_id):
            #Try using ObjectId query first
            result = await db.users.update_one(
                {"_id": ObjectId(user_id)},
                {"$pull": {"favorite_stocks": {"stock_code": stock_code}}}
            )
            #Try using a string query if ObjectiveId query failed
            if result.matched_count == 0:
                result = await db.users.update_one(
                    {"_id": user_id},
                    {"$pull": {"favorite_stocks": {"stock_code": stock_code}}}
                )
            return result.modified_count > 0
        else:
            result = await db.user_favorites.update_one(
                {"user_id": user_id},
                {
                    "$pull": {"favorites": {"stock_code": stock_code}},
                    "$set": {"updated_at": datetime.utcnow()}
                }
            )
            return result.modified_count > 0

    async def update_favorite(
        self,
        user_id: str,
        stock_code: str,
        tags: Optional[List[str]] = None,
        notes: Optional[str] = None,
        alert_price_high: Optional[float] = None,
        alert_price_low: Optional[float] = None
    ) -> bool:
        """Update self-selected unit information (compatible string ID with objectId)"""
        db = await self._get_db()

        #Unifiedly build updated fields (prefixes according to the path of different collections)
        is_oid = self._is_valid_object_id(user_id)
        prefix = "favorite_stocks.$." if is_oid else "favorites.$."
        update_fields: Dict[str, Any] = {}
        if tags is not None:
            update_fields[prefix + "tags"] = tags
        if notes is not None:
            update_fields[prefix + "notes"] = notes
        if alert_price_high is not None:
            update_fields[prefix + "alert_price_high"] = alert_price_high
        if alert_price_low is not None:
            update_fields[prefix + "alert_price_low"] = alert_price_low

        if not update_fields:
            return True

        if is_oid:
            result = await db.users.update_one(
                {
                    "_id": ObjectId(user_id),
                    "favorite_stocks.stock_code": stock_code
                },
                {"$set": update_fields}
            )
            return result.modified_count > 0
        else:
            result = await db.user_favorites.update_one(
                {
                    "user_id": user_id,
                    "favorites.stock_code": stock_code
                },
                {
                    "$set": {
                        **update_fields,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            return result.modified_count > 0

    async def is_favorite(self, user_id: str, stock_code: str) -> bool:
        """Check if shares are in the selected stock (compatible string ID with objectId)"""
        import logging
        logger = logging.getLogger("webapi")

        try:
            logger.info(f"[is favorite]{user_id}, stock_code={stock_code}")

            db = await self._get_db()

            is_oid = self._is_valid_object_id(user_id)
            logger.info(f"[is favorite] User ID type: is valid Object id={is_oid}")

            if is_oid:
                #Try using ObjectId query first
                user = await db.users.find_one(
                    {
                        "_id": ObjectId(user_id),
                        "favorite_stocks.stock_code": stock_code
                    }
                )

                #Try using a string query if ObjectiveId query failed
                if user is None:
                    logger.info(f"🔧 [is favorite] ObjectId query not found, trying to use string ID query")
                    user = await db.users.find_one(
                        {
                            "_id": user_id,
                            "favorite_stocks.stock_code": stock_code
                        }
                    )

                result = user is not None
                logger.info(f"[is favorite]{result}")
                return result
            else:
                doc = await db.user_favorites.find_one(
                    {
                        "user_id": user_id,
                        "favorites.stock_code": stock_code
                    }
                )
                result = doc is not None
                logger.info(f"[is favorite] StringID query results:{result}")
                return result
        except Exception as e:
            logger.error(f"[is favorite]{type(e).__name__}: {str(e)}", exc_info=True)
            raise

    async def get_user_tags(self, user_id: str) -> List[str]:
        """Fetch all labels used by users (compatible string ID and objectId)"""
        db = await self._get_db()

        if self._is_valid_object_id(user_id):
            pipeline = [
                {"$match": {"_id": ObjectId(user_id)}},
                {"$unwind": "$favorite_stocks"},
                {"$unwind": "$favorite_stocks.tags"},
                {"$group": {"_id": "$favorite_stocks.tags"}},
                {"$sort": {"_id": 1}}
            ]
            result = await db.users.aggregate(pipeline).to_list(None)
        else:
            pipeline = [
                {"$match": {"user_id": user_id}},
                {"$unwind": "$favorites"},
                {"$unwind": "$favorites.tags"},
                {"$group": {"_id": "$favorites.tags"}},
                {"$sort": {"_id": 1}}
            ]
            result = await db.user_favorites.aggregate(pipeline).to_list(None)

        return [item["_id"] for item in result if item.get("_id")]

    def _get_mock_price(self, stock_code: str) -> float:
        """Get mock equity prices"""
        #Generate analogue prices based on stock code
        base_price = hash(stock_code) % 100 + 10
        return round(base_price + (hash(stock_code) % 1000) / 100, 2)
    
    def _get_mock_change(self, stock_code: str) -> float:
        """Get simulated rise and fall"""
        #Modelled up and down based on stock code Fan
        change = (hash(stock_code) % 2000 - 1000) / 100
        return round(change, 2)
    
    def _get_mock_volume(self, stock_code: str) -> int:
        """Get Simulated Exchange"""
        #Generate simulations based on stock code
        return (hash(stock_code) % 10000 + 1000) * 100


#Create global instance
favorites_service = FavoritesService()
