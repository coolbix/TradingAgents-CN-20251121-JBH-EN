"""Enhanced stock screening services
Provide efficient stock screening in conjunction with database optimization and traditional selection methods
"""

import logging
import time
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from app.models.screening import ScreeningCondition, FieldType, BASIC_FIELDS_INFO
from app.services.database_screening_service import get_database_screening_service
from app.services.screening_service import ScreeningService, ScreeningParams

logger = logging.getLogger(__name__)

from app.services.enhanced_screening.utils import (
    analyze_conditions as _analyze_conditions_util,
    convert_conditions_to_traditional_format as _convert_to_traditional_util,
)
from app.core.database import get_mongo_db_async


class EnhancedScreeningService:
    """Enhanced stock screening services"""

    def __init__(self):
        self.db_service = get_database_screening_service()
        self.traditional_service = ScreeningService()

        #Fields supporting database optimization
        self.db_supported_fields = set(BASIC_FIELDS_INFO.keys())

    async def screen_stocks(
        self,
        conditions: List[ScreeningCondition],
        market: str = "CN",
        date: Optional[str] = None,
        adj: str = "qfq",
        limit: int = 50,
        offset: int = 0,
        order_by: Optional[List[Dict[str, str]]] = None,
        use_database_optimization: bool = True
    ) -> Dict[str, Any]:
        """Smart stock filter

        Args:
            Conditions: Filter Condition List
            Market:
            date: transaction date
            Adj: By way of reinstatement
            Limited number of returns
            offset: offset
            order by: Sort Conditions
            use database optimisation: using database optimization

        Returns:
            Dict: Filter Results
        """
        start_time = time.time()

        try:
            #Analyse filter conditions
            analysis = self._analyze_conditions(conditions)

            #Decision on which selection method to use
            if (use_database_optimization and
                analysis["can_use_database"] and
                not analysis["needs_technical_indicators"]):

                #Use database optimization filter
                result = await self._screen_with_database(
                    conditions, limit, offset, order_by
                )
                optimization_used = "database"
                source = "mongodb"

            else:
                #Use traditional selection methods
                result = await self._screen_with_traditional_method(
                    conditions, market, date, adj, limit, offset, order_by
                )
                optimization_used = "traditional"
                source = "api"

            #extract
            items = result[0] if isinstance(result, tuple) else result.get("items", [])
            total = result[1] if isinstance(result, tuple) else result.get("total", 0)

            #If you use the database to optimize the path, enrich from the database profile (avoid external calls when requested)
            if source == "mongodb" and items:
                try:
                    db = get_mongo_db_async()
                    coll = db["market_quotes"]
                    codes = [str(it.get("code")).zfill(6) for it in items if it.get("code")]
                    if codes:
                        cursor = coll.find(
                            {"code": {"$in": codes}},
                            projection={"_id": 0, "code": 1, "close": 1, "pct_chg": 1, "amount": 1},
                        )
                        quotes_list = await cursor.to_list(length=len(codes))
                        quotes_map = {str(d.get("code")).zfill(6): d for d in quotes_list}
                        for it in items:
                            key = str(it.get("code")).zfill(6)
                            q = quotes_map.get(key)
                            if not q:
                                continue
                            if q.get("close") is not None:
                                it["close"] = q.get("close")
                            if q.get("pct_chg") is not None:
                                it["pct_chg"] = q.get("pct_chg")
                            if q.get("amount") is not None:
                                it["amount"] = q.get("amount")
                except Exception as enrich_err:
                    logger.warning(f"Real-time venture wealth failed (neglected):{enrich_err}")

            #Add real time PE/PB to filter results
            if items:
                try:
                    items = await self._enrich_results_with_realtime_metrics(items)
                except Exception as enrich_err:
                    logger.warning(f"Real-time PE/PB enrichment failed (neglected):{enrich_err}")

            #Time-consuming calculation
            took_ms = int((time.time() - start_time) * 1000)

            #Return Result
            return {
                "total": total,
                "items": items,
                "took_ms": took_ms,
                "optimization_used": optimization_used,
                "source": source,
                "analysis": analysis
            }

        except Exception as e:
            logger.error(f"The selection failed:{e}")
            took_ms = int((time.time() - start_time) * 1000)

            return {
                "total": 0,
                "items": [],
                "took_ms": took_ms,
                "optimization_used": "none",
                "source": "error",
                "error": str(e)
            }

    def _analyze_conditions(self, conditions: List[ScreeningCondition]) -> Dict[str, Any]:
        """Delegate condition analysis to utils."""
        analysis = _analyze_conditions_util(conditions)
        logger.info(f"Screening condition analysis:{analysis}")
        return analysis

    async def _screen_with_database(
        self,
        conditions: List[ScreeningCondition],
        limit: int,
        offset: int,
        order_by: Optional[List[Dict[str, str]]]
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Use database optimization filter"""
        logger.info("🚀 Use database optimization filter")

        return await self.db_service.screen_stocks(
            conditions=conditions,
            limit=limit,
            offset=offset,
            order_by=order_by
        )

    async def _screen_with_traditional_method(
        self,
        conditions: List[ScreeningCondition],
        market: str,
        date: Optional[str],
        adj: str,
        limit: int,
        offset: int,
        order_by: Optional[List[Dict[str, str]]]
    ) -> Dict[str, Any]:
        """Use of traditional screening methods"""
        logger.info("Using traditional screening methods")

        #Convert conditional format to traditional service support format
        traditional_conditions = self._convert_conditions_to_traditional_format(conditions)

        #Create filter parameters
        params = ScreeningParams(
            market=market,
            date=date,
            adj=adj,
            limit=limit,
            offset=offset,
            order_by=order_by
        )

        #Implement traditional filtering
        result = self.traditional_service.run(traditional_conditions, params)

        return result

    def _convert_conditions_to_traditional_format(
        self,
        conditions: List[ScreeningCondition]
    ) -> Dict[str, Any]:
        """Delegate condition conversion to utils."""
        return _convert_to_traditional_util(conditions)

    async def _enrich_results_with_realtime_metrics(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Add PE/PB to filter results (use static data to avoid performance problems)

        Args:
            Organisation

        Returns:
            List [Dict]: List of results after enrichment
        """
        # Stock screening scene: Directly using static PE/PB in stock basic info
        #Reason: Bulk calculation dynamics PE can cause serious performance problems (each stock is asked for multiple pools)
        #Static PE based on the closing price of the last trading date, is accurate enough for the screening scene.

        logger.info(f" [Screen Results Enrichment] Using Static PE/PB (avoiding performance problems),{len(items)}Only stocks")

        #Note: PE/PB of the items has come from stock basic info, which does not require additional processing
        #If real-time PE is needed in the future, it can be calculated separately on the individual stock details page

        return items

    async def get_field_info(self, field: str) -> Optional[Dict[str, Any]]:
        """Get Field Information

        Args:
            Field: First Name

        Returns:
            Dict: Field Information
        """
        if field in BASIC_FIELDS_INFO:
            field_info = BASIC_FIELDS_INFO[field]

            #Access to statistical information
            stats = await self.db_service.get_field_statistics(field)

            #Get an optional value (for an itemized type field)
            available_values = None
            if field_info.data_type == "string":
                available_values = await self.db_service.get_available_values(field)

            return {
                "name": field_info.name,
                "display_name": field_info.display_name,
                "field_type": field_info.field_type.value,
                "data_type": field_info.data_type,
                "description": field_info.description,
                "unit": field_info.unit,
                "supported_operators": [op.value for op in field_info.supported_operators],
                "statistics": stats,
                "available_values": available_values
            }

        return None

    async def get_all_supported_fields(self) -> List[Dict[str, Any]]:
        """Get all supported field information"""
        fields = []

        for field_name in BASIC_FIELDS_INFO.keys():
            field_info = await self.get_field_info(field_name)
            if field_info:
                fields.append(field_info)

        return fields

    async def validate_conditions(self, conditions: List[ScreeningCondition]) -> Dict[str, Any]:
        """Verify Filter Conditions

        Args:
            Conditions: Filter Condition List

        Returns:
            Dict: Verify Results
        """
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": []
        }

        for i, condition in enumerate(conditions):
            field = condition.field
            operator = condition.operator
            value = condition.value

            #Checks if fields support
            if field not in BASIC_FIELDS_INFO:
                validation_result["errors"].append(
                    f"条件 {i+1}: 不支持的字段 '{field}'"
                )
                validation_result["valid"] = False
                continue

            field_info = BASIC_FIELDS_INFO[field]

            #Check if operator supports
            if operator not in [op.value for op in field_info.supported_operators]:
                validation_result["errors"].append(
                    f"条件 {i+1}: 字段 '{field}' 不支持操作符 '{operator}'"
                )
                validation_result["valid"] = False

            #Type and range of checked values
            if field_info.data_type == "number":
                if operator == "between":
                    if not isinstance(value, list) or len(value) != 2:
                        validation_result["errors"].append(
                            f"条件 {i+1}: between操作符需要两个数值"
                        )
                        validation_result["valid"] = False
                    elif not all(isinstance(v, (int, float)) for v in value):
                        validation_result["errors"].append(
                            f"条件 {i+1}: between操作符的值必须是数字"
                        )
                        validation_result["valid"] = False
                elif not isinstance(value, (int, float)):
                    validation_result["errors"].append(
                        f"条件 {i+1}: 数值字段 '{field}' 的值必须是数字"
                    )
                    validation_result["valid"] = False

        return validation_result


#Examples of global services
_enhanced_screening_service: Optional[EnhancedScreeningService] = None


def get_enhanced_screening_service() -> EnhancedScreeningService:
    """Example of accessing enhanced screening services"""
    global _enhanced_screening_service
    if _enhanced_screening_service is None:
        _enhanced_screening_service = EnhancedScreeningService()
    return _enhanced_screening_service
