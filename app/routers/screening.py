
import logging
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from app.routers.auth_db import get_current_user

from app.services.screening_service import ScreeningService, ScreeningParams
from app.services.enhanced_screening_service import get_enhanced_screening_service
from app.models.screening import (
    ScreeningCondition, ScreeningRequest as NewScreeningRequest,
    ScreeningResponse as NewScreeningResponse, FieldInfo, BASIC_FIELDS_INFO
)

router = APIRouter(tags=["screening"])
logger = logging.getLogger("webapi")

#Filter field configuration response model
class FieldConfigResponse(BaseModel):
    """Filter field configuration response"""
    fields: Dict[str, FieldInfo]
    categories: Dict[str, List[str]]

#Traditional request/response model (maintaining backward compatibility)
class OrderByItem(BaseModel):
    field: str
    direction: str = Field("desc", pattern=r"^(?i)(asc|desc)$")

class ScreeningRequest(BaseModel):
    market: str = Field("CN", description="市场：CN")
    date: Optional[str] = Field(None, description="交易日YYYY-MM-DD，缺省为最新")
    adj: str = Field("qfq", description="复权口径：qfq/hfq/none（P0占位）")
    conditions: Dict[str, Any] = Field(default_factory=dict)
    order_by: Optional[List[OrderByItem]] = None
    limit: int = Field(50, ge=1, le=500)
    offset: int = Field(0, ge=0)

class ScreeningResponse(BaseModel):
    total: int
    items: List[dict]

#Examples of services
svc = ScreeningService()
enhanced_svc = get_enhanced_screening_service()


@router.get("/fields", response_model=FieldConfigResponse)
async def get_screening_fields(user: dict = Depends(get_current_user)):
    """Get Filter Field Configuration
    Returns all available filter fields and their configuration information
    """
    try:
        #Field Classification
        categories = {
            "basic": ["code", "name", "industry", "area", "market"],
            "market_value": ["total_mv", "circ_mv"],
            "financial": ["pe", "pb", "pe_ttm", "pb_mrq", "roe"],
            "trading": ["turnover_rate", "volume_ratio"],
            "price": ["close", "pct_chg", "amount"],
            "technical": ["ma20", "rsi14", "kdj_k", "kdj_d", "kdj_j", "dif", "dea", "macd_hist"]
        }

        return FieldConfigResponse(
            fields=BASIC_FIELDS_INFO,
            categories=categories
        )

    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


def _convert_legacy_conditions_to_new_format(legacy_conditions: Dict[str, Any]) -> List[ScreeningCondition]:
    """Convert filter conditions in traditional format to new format

    Example of traditional format:
    FMT 0 

    ♪ I'm sorry ♪

    New format:
    [ Chuckles ]
    ScreeningCondition (field = "total mv", operator = "between", value = [50, 900771992547])

    """
    conditions = []

    #Field name map (old field name - > unified backend field name that may be used at the front end)
    field_mapping = {
        "market_cap": "total_mv",      #Market value (old field compatible)
        "pe_ratio": "pe",              #Market share (compatible with old field names)
        "pb_ratio": "pb",              #Net market ratio (compatible with old field names)
        "turnover": "turnover_rate",   #Exchange rate (compatible with old field names)
        "change_percent": "pct_chg",   #Increase or decrease (compatible old field names)
        "price": "close",              #Price (compatible with old field names)
    }

    #Operator Map
    operator_mapping = {
        "between": "between",
        "gt": ">",
        "lt": "<",
        "gte": ">=",
        "lte": "<=",
        "eq": "==",
        "ne": "!=",
        "in": "in",
        "contains": "contains"
    }

    if isinstance(legacy_conditions, dict):
        children = legacy_conditions.get("children", [])

        for child in children:
            if isinstance(child, dict):
                field = child.get("field")
                op = child.get("op")
                value = child.get("value")

                if field and op and value is not None:
                    #Map field name
                    mapped_field = field_mapping.get(field, field)

                    #Map Operator
                    mapped_op = operator_mapping.get(op, op)

                    #Processing of market value unit conversions (millions in front end and billions in database)
                    if mapped_field == "total_mv" and isinstance(value, list):
                        #Convert millions to billions
                        converted_value = [v / 10000 for v in value if isinstance(v, (int, float))]
                        logger.info(f"[screening]{value}A million dollars.{converted_value}Billions.")
                        value = converted_value
                    elif mapped_field == "total_mv" and isinstance(value, (int, float)):
                        value = value / 10000
                        logger.info(f"[screening]{child.get('value')}A million dollars.{value}Billions.")

                    #Create Filter Condition
                    condition = ScreeningCondition(
                        field=mapped_field,
                        operator=mapped_op,
                        value=value
                    )
                    conditions.append(condition)

                    logger.info(f"[screening]{field}({op}) -> {mapped_field}({mapped_op}) value:{value}")

    return conditions


#Traditional screening interfaces (backward compatibility but use of enhanced services)
@router.post("/run", response_model=ScreeningResponse)
async def run_screening(req: ScreeningRequest, user: dict = Depends(get_current_user)):
    try:
        logger.info(f"[screening] Request terms:{req.conditions}")
        logger.info(f"[screening] Sort with page break: order by={req.order_by}, limit={req.limit}, offset={req.offset}")

        #The condition for converting the traditional format is new
        conditions = _convert_legacy_conditions_to_new_format(req.conditions)
        logger.info(f"[screening]{conditions}")

        #Use enhanced screening services
        result = await enhanced_svc.screen_stocks(
            conditions=conditions,
            market=req.market,
            date=req.date,
            adj=req.adj,
            limit=req.limit,
            offset=req.offset,
            order_by=[{"field": o.field, "direction": o.direction} for o in (req.order_by or [])],
            use_database_optimization=True
        )

        logger.info(f"[screening] Screening complete: total={result.get('total')}, "
                   f"took={result.get('took_ms')}ms, optimization={result.get('optimization_used')}")

        if result.get('items'):
            sample = result['items'][:3]
            logger.info(f"[screening] Returns sample (first 3):{sample}")

        return ScreeningResponse(total=result["total"], items=result["items"])

    except Exception as e:
        logger.error(f"[screening] Process failed:{e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


#New Optimised Filter Interface
@router.post("/enhanced", response_model=NewScreeningResponse)
async def enhanced_screening(req: NewScreeningRequest, user: dict = Depends(get_current_user)):
    """Enhanced stock filter interface
    - Support for richer filtering conditions formats
    - Automatically select the best selection strategy (database optimization vs traditional method)
    - Provide detailed performance statistics
    """
    try:
        logger.info(f"[enhanced screening]{len(req.conditions)}individual")
        logger.info(f"[enhanced screening] Sort with page break: order by={req.order_by}, limit={req.limit}, offset={req.offset}")

        #Execute Enhanced Filter
        result = await enhanced_svc.screen_stocks(
            conditions=req.conditions,
            market=req.market,
            date=req.date,
            adj=req.adj,
            limit=req.limit,
            offset=req.offset,
            order_by=req.order_by,
            use_database_optimization=req.use_database_optimization
        )

        logger.info(f"[enhanced screening] Screening complete: total={result.get('total')}, "
                   f"took={result.get('took_ms')}ms, optimization={result.get('optimization_used')}")

        return NewScreeningResponse(
            total=result["total"],
            items=result["items"],
            took_ms=result.get("took_ms"),
            optimization_used=result.get("optimization_used"),
            source=result.get("source")
        )

    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"增强筛选失败: {str(e)}")


#Get Supported Field Information
@router.get("/fields", response_model=List[Dict[str, Any]])
async def get_supported_fields(user: dict = Depends(get_current_user)):
    """Get all supported filter field information"""
    try:
        fields = await enhanced_svc.get_all_supported_fields()
        return fields
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"获取字段信息失败: {str(e)}")


#Fetch details for individual fields
@router.get("/fields/{field_name}", response_model=Dict[str, Any])
async def get_field_info(field_name: str, user: dict = Depends(get_current_user)):
    """Get details of the specified field"""
    try:
        field_info = await enhanced_svc.get_field_info(field_name)
        if not field_info:
            raise HTTPException(status_code=404, detail=f"字段 '{field_name}' 不存在")
        return field_info
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"获取字段信息失败: {str(e)}")


#Verify Filter Conditions
@router.post("/validate", response_model=Dict[str, Any])
async def validate_conditions(conditions: List[ScreeningCondition], user: dict = Depends(get_current_user)):
    """Validation of filter conditions"""
    try:
        validation_result = await enhanced_svc.validate_conditions(conditions)
        return validation_result
    except Exception as e:
        logger.error(f"Could not close temporary folder: %s{e}")
        raise HTTPException(status_code=500, detail=f"验证条件失败: {str(e)}")

#Repeat defined old peer removal (retain version with log)


@router.get("/industries")
async def get_industries(user: dict = Depends(get_current_user)):
    """Get a list of all available industries in the database
    Obtain industry-specific data from the highest-priority data sources according to the data source priorities configured by the system
    Returns the list of industries by stock count
    """
    try:
        from app.core.database import get_mongo_db
        from app.core.unified_config import UnifiedConfigManager

        db = get_mongo_db()
        collection = db["stock_basic_info"]

        #🔥 Access to data source priority configuration (using the uniform configuration manager's walk method)
        config = UnifiedConfigManager()
        data_source_configs = await config.get_data_source_configs_async()

        #Extract enabled data sources in order of priority (ordered)
        enabled_sources = [
            ds.type.lower() for ds in data_source_configs
            if ds.enabled and ds.type.lower() in ['tushare', 'akshare', 'baostock']
        ]

        if not enabled_sources:
            #Use default order if no configuration
            enabled_sources = ['tushare', 'akshare', 'baostock']

        logger.info(f"[get industries] Data source priority:{enabled_sources}")

        #🔥 Priority query: Prioritize the highest priority data sources
        preferred_source = enabled_sources[0] if enabled_sources else 'tushare'

        #Aggregation queries: grouping by industry and counting the number of shares (see only specified data sources)
        pipeline = [
            {
                "$match": {
                    "source": preferred_source,  #🔥 Only query the highest priority data source
                    "industry": {"$ne": None, "$ne": ""}  #Filter Empty Industry
                }
            },
            {
                "$group": {
                    "_id": "$industry",
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"count": -1}},  #Sort by stock volume decrease
            {
                "$project": {
                    "industry": "$_id",
                    "count": 1,
                    "_id": 0
                }
            }
        ]

        industries = []
        async for doc in collection.aggregate(pipeline):
            #Purge fields to avoid NAN/Inf resulting in the serialization of JSON
            raw_industry = doc.get("industry")
            safe_industry = ""
            try:
                if raw_industry is None:
                    safe_industry = ""
                elif isinstance(raw_industry, float):
                    if raw_industry != raw_industry or raw_industry in (float("inf"), float("-inf")):
                        safe_industry = ""
                    else:
                        safe_industry = str(raw_industry)
                else:
                    safe_industry = str(raw_industry)
            except Exception:
                safe_industry = ""

            raw_count = doc.get("count", 0)
            safe_count = 0
            try:
                if isinstance(raw_count, float):
                    if raw_count != raw_count or raw_count in (float("inf"), float("-inf")):
                        safe_count = 0
                    else:
                        safe_count = int(raw_count)
                else:
                    safe_count = int(raw_count)
            except Exception:
                safe_count = 0

            industries.append({
                "value": safe_industry,
                "label": safe_industry,
                "count": safe_count,
            })

        logger.info(f"[get industries] from data sources{preferred_source}Back{len(industries)}Industry")

        return {
            "industries": industries,
            "total": len(industries),
            "source": preferred_source  #Return data source
        }

    except Exception as e:
        logger.error(f"[get industries] Failed to get industry list:{e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))