from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field
from typing import Literal, Optional, Dict, Any, List, Tuple
from datetime import datetime
import logging
import re

from app.routers.auth_db import get_current_user
from app.core.database import get_mongo_db
from app.core.response import ok

router = APIRouter(prefix="/paper", tags=["paper"])
logger = logging.getLogger("webapi")


#Initial funding allocation per market
INITIAL_CASH_BY_MARKET = {
    "CNY": 1_000_000.0,   #A share: 1 million yuan
    "HKD": 1_000_000.0,   #Port shares: HK$ 1 million
    "USD": 100_000.0      #United States share: $100,000
}


class PlaceOrderRequest(BaseModel):
    code: str = Field(..., description="股票代码（支持A股/港股/美股）")
    side: Literal["buy", "sell"]
    quantity: int = Field(..., gt=0)
    market: Optional[str] = Field(None, description="市场类型 (CN/HK/US)，不传则自动识别")
    #Optional: Associated Analysis ID, to facilitate tracking from the analysis page down to the list
    analysis_id: Optional[str] = None


def _detect_market_and_code(code: str) -> Tuple[str, str]:
    """Test market type of stock code and standardize code

    Returns:
        (market, standardized code): Market type and standardized code
        - CN: Unit A (6-digit)
        - HK: Port Unit (4-5 digit or HK suffix)
        - US: United States shares (letter code)
    """
    code = code.strip().upper()

    #Port Unit: with .HK suffix
    if code.endswith('.HK'):
        return ('HK', code[:-3].zfill(5))

    #United States share: pure letters
    if re.match(r'^[A-Z]+$', code):
        return ('US', code)

    #Port Unit: 4-5 figures
    if re.match(r'^\d{4,5}$', code):
        return ('HK', code.zfill(5))

    #Unit A: 6 figures
    if re.match(r'^\d{6}$', code):
        return ('CN', code)

    #Default as unit A, complete 6 places
    return ('CN', code.zfill(6))


async def _get_or_create_account(user_id: str) -> Dict[str, Any]:
    """Acquisition or creation of accounts (multi-currency)"""
    db = get_mongo_db()
    acc = await db["paper_accounts"].find_one({"user_id": user_id})
    if not acc:
        now = datetime.utcnow().isoformat()
        acc = {
            "user_id": user_id,
            #Multi-currency cash accounts
            "cash": {
                "CNY": INITIAL_CASH_BY_MARKET["CNY"],
                "HKD": INITIAL_CASH_BY_MARKET["HKD"],
                "USD": INITIAL_CASH_BY_MARKET["USD"]
            },
            #Multi-currency realized gains and losses
            "realized_pnl": {
                "CNY": 0.0,
                "HKD": 0.0,
                "USD": 0.0
            },
            #Account Settings
            "settings": {
                "auto_currency_conversion": False,
                "default_market": "CN"
            },
            "created_at": now,
            "updated_at": now,
        }
        await db["paper_accounts"].insert_one(acc)
    else:
        #Compatible old account structure: migrate to a multi-currency object if the cash or realized pnl remain the target
        updates: Dict[str, Any] = {}
        try:
            cash_val = acc.get("cash")
            if not isinstance(cash_val, dict):
                base_cash = float(cash_val or 0.0)
                updates["cash"] = {"CNY": base_cash, "HKD": 0.0, "USD": 0.0}

            pnl_val = acc.get("realized_pnl")
            if not isinstance(pnl_val, dict):
                base_pnl = float(pnl_val or 0.0)
                updates["realized_pnl"] = {"CNY": base_pnl, "HKD": 0.0, "USD": 0.0}

            if updates:
                updates["updated_at"] = datetime.utcnow().isoformat()
                await db["paper_accounts"].update_one({"user_id": user_id}, {"$set": updates})
                #Reread migrated accounts
                acc = await db["paper_accounts"].find_one({"user_id": user_id})
        except Exception as e:
            logger.error(f"The account structure has failed{user_id}: {e}")
    return acc


async def _get_market_rules(market: str) -> Optional[Dict[str, Any]]:
    """Access to market rules configuration"""
    db = get_mongo_db()
    rules_doc = await db["paper_market_rules"].find_one({"market": market})
    if rules_doc:
        return rules_doc.get("rules", {})
    return None


def _calculate_commission(market: str, side: str, amount: float, rules: Dict[str, Any]) -> float:
    """Calculation of fees"""
    if not rules or "commission" not in rules:
        return 0.0

    commission_config = rules["commission"]
    commission = 0.0

    #Commissions
    comm_rate = commission_config.get("rate", 0.0)
    comm_min = commission_config.get("min", 0.0)
    commission += max(amount * comm_rate, comm_min)

    #stamp duty (sale only)
    if side == "sell" and "stamp_duty_rate" in commission_config:
        commission += amount * commission_config["stamp_duty_rate"]

    #Other costs (Port Unit)
    if market == "HK":
        if "transaction_levy_rate" in commission_config:
            commission += amount * commission_config["transaction_levy_rate"]
        if "trading_fee_rate" in commission_config:
            commission += amount * commission_config["trading_fee_rate"]
        if "settlement_fee_rate" in commission_config:
            commission += amount * commission_config["settlement_fee_rate"]

    #SEC costs (United States share, sold only)
    if market == "US" and side == "sell" and "sec_fee_rate" in commission_config:
        commission += amount * commission_config["sec_fee_rate"]

    return round(commission, 2)


async def _get_available_quantity(user_id: str, code: str, market: str) -> int:
    """Access to available quantities (considering T+1 limits)"""
    db = get_mongo_db()
    pos = await db["paper_positions"].find_one({"user_id": user_id, "code": code})

    if not pos:
        return 0

    total_qty = pos.get("quantity", 0)

    #A shares T+1: not sold today
    if market == "CN":
        #Market access rules
        rules = await _get_market_rules(market)
        if rules and rules.get("t_plus", 0) > 0:
            #Query number of purchases today
            today = datetime.utcnow().date().isoformat()
            pipeline = [
                {"$match": {
                    "user_id": user_id,
                    "code": code,
                    "side": "buy",
                    "timestamp": {"$gte": today}
                }},
                {"$group": {"_id": None, "total": {"$sum": "$quantity"}}}
            ]
            today_buy = await db["paper_trades"].aggregate(pipeline).to_list(1)
            today_buy_qty = today_buy[0]["total"] if today_buy else 0
            return max(0, total_qty - today_buy_qty)

    #Port Unit/United States Unit T+0: All available
    return total_qty


async def _get_last_price(code: str, market: str) -> Optional[float]:
    """Up-to-date prices for equities (support to multiple markets)

    Args:
        code: stock code
        Market type (CN/HK/US)

    Returns:
        Latest price, notone if you fail to get back
    """
    db = get_mongo_db()

    #Unit A: Access to database
    if market == "CN":
        #1. Attempt to retrieve from market quotes
        q = await db["market_quotes"].find_one(
            {"$or": [{"code": code}, {"symbol": code}]},
            {"_id": 0, "close": 1}
        )
        if q and q.get("close") is not None:
            try:
                price = float(q["close"])
                if price > 0:
                    logger.debug(f"Get prices from market quotes:{code} = {price}")
                    return price
            except Exception as e:
                logger.warning(f"The price conversion failed.{code}: {e}")

        #Back to stock basic info
        basic_info = await db["stock_basic_info"].find_one(
            {"$or": [{"code": code}, {"symbol": code}]},
            {"_id": 0, "current_price": 1}
        )
        if basic_info and basic_info.get("current_price") is not None:
            try:
                price = float(basic_info["current_price"])
                if price > 0:
                    logger.debug(f"Get the price from stock basic info:{code} = {price}")
                    return price
            except Exception as e:
                logger.warning(f"The price conversion failed.{code}: {e}")

        logger.error(f"No A share price can be obtained from the database:{code}")
        return None

    #Port Unit/United States Unit: Use ForestStockService
    elif market in ['HK', 'US']:
        try:
            from app.services.foreign_stock_service import ForeignStockService
            db = get_mongo_db()
            service = ForeignStockService(db=db)

            quote = await service.get_quote(market, code, force_refresh=False)

            if quote:
                #Try multiple possible price fields
                price = quote.get("price") or quote.get("current_price") or quote.get("close")
                if price and float(price) > 0:
                    logger.debug(f"From ForestStockService{market}Price:{code} = {price}")
                    return float(price)
        except Exception as e:
            logger.error(f"Access{market}Stock price failed{code}: {e}")
            return None

    logger.error(f"No stock prices available:{code} (market={market})")
    return None


def _zfill_code(code: str) -> str:
    s = str(code).strip()
    if len(s) == 6 and s.isdigit():
        return s
    return s.zfill(6)


@router.get("/account", response_model=dict)
async def get_account(current_user: dict = Depends(get_current_user)):
    """Acquisition or creation of paper accounts, return of funds and warehouse valuation summary (support to multi-market)"""
    db = get_mongo_db()
    acc = await _get_or_create_account(current_user["id"])

    #Valuation of polymer hold (by currency)
    positions = await db["paper_positions"].find({"user_id": current_user["id"]}).to_list(None)

    positions_value_by_currency = {
        "CNY": 0.0,
        "HKD": 0.0,
        "USD": 0.0
    }

    detailed_positions: List[Dict[str, Any]] = []
    for p in positions:
        code = p.get("code")
        market = p.get("market", "CN")
        currency = p.get("currency", "CNY")
        qty = int(p.get("quantity", 0))
        avg_cost = float(p.get("avg_cost", 0.0))
        available_qty = p.get("available_qty", qty)

        #Get the latest price.
        last = await _get_last_price(code, market)
        mkt_value = round((last or 0.0) * qty, 2)
        positions_value_by_currency[currency] += mkt_value

        detailed_positions.append({
            "code": code,
            "market": market,
            "currency": currency,
            "quantity": qty,
            "available_qty": available_qty,
            "avg_cost": avg_cost,
            "last_price": last,
            "market_value": mkt_value,
            "unrealized_pnl": None if last is None else round((last - avg_cost) * qty, 2)
        })

    #Total assets calculated (in currency terms)
    cash = acc.get("cash", {})
    realized_pnl = acc.get("realized_pnl", {})

    #Compatibility with old formats (single cash)
    if not isinstance(cash, dict):
        cash = {"CNY": float(cash), "HKD": 0.0, "USD": 0.0}
    if not isinstance(realized_pnl, dict):
        realized_pnl = {"CNY": float(realized_pnl), "HKD": 0.0, "USD": 0.0}

    summary = {
        "cash": {
            "CNY": round(float(cash.get("CNY", 0.0)), 2),
            "HKD": round(float(cash.get("HKD", 0.0)), 2),
            "USD": round(float(cash.get("USD", 0.0)), 2)
        },
        "realized_pnl": {
            "CNY": round(float(realized_pnl.get("CNY", 0.0)), 2),
            "HKD": round(float(realized_pnl.get("HKD", 0.0)), 2),
            "USD": round(float(realized_pnl.get("USD", 0.0)), 2)
        },
        "positions_value": positions_value_by_currency,
        "equity": {
            "CNY": round(float(cash.get("CNY", 0.0)) + positions_value_by_currency["CNY"], 2),
            "HKD": round(float(cash.get("HKD", 0.0)) + positions_value_by_currency["HKD"], 2),
            "USD": round(float(cash.get("USD", 0.0)) + positions_value_by_currency["USD"], 2)
        },
        "updated_at": acc.get("updated_at"),
    }

    return ok({"account": summary, "positions": detailed_positions})


@router.post("/order", response_model=dict)
async def place_order(payload: PlaceOrderRequest, current_user: dict = Depends(get_current_user)):
    """Submission of a market price list, ready for sale at the latest (support to multi-market)"""
    db = get_mongo_db()

    #1. Identification of market types
    if payload.market:
        market = payload.market.upper()
        normalized_code = payload.code
    else:
        market, normalized_code = _detect_market_and_code(payload.code)

    side = payload.side
    qty = int(payload.quantity)
    analysis_id = getattr(payload, "analysis_id", None)

    #2. Currency determination
    currency_map = {
        "CN": "CNY",
        "HK": "HKD",
        "US": "USD"
    }
    currency = currency_map.get(market, "CNY")

    #3. Access to accounts
    acc = await _get_or_create_account(current_user["id"])

    #4. Access to prices
    price = await _get_last_price(normalized_code, market)
    if price is None or price <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"无法获取股票 {normalized_code} ({market}) 的最新价格"
        )

    #5. Calculation of amounts
    notional = round(price * qty, 2)

    #Access to market rules and calculation of fees
    rules = await _get_market_rules(market)
    commission = _calculate_commission(market, side, notional, rules) if rules else 0.0
    total_cost = notional + commission

    #7. Access to holdouts
    pos = await db["paper_positions"].find_one({"user_id": current_user["id"], "code": normalized_code})

    now_iso = datetime.utcnow().isoformat()
    realized_pnl_delta = 0.0

    #8. Implementation of the logic of dealing
    if side == "buy":
        #Funds checks (accounts using corresponding currencies)
        cash = acc.get("cash", {})
        if isinstance(cash, dict):
            available_cash = float(cash.get(currency, 0.0))
        else:
            #Compatible with old format
            available_cash = float(cash) if currency == "CNY" else 0.0

        if available_cash < total_cost:
            raise HTTPException(
                status_code=400,
                detail=f"可用{currency}不足：需要 {total_cost:.2f}，可用 {available_cash:.2f}"
            )

        #Less funds (from corresponding currency accounts)
        new_cash = round(available_cash - total_cost, 2)
        await db["paper_accounts"].update_one(
            {"user_id": current_user["id"]},
            {"$set": {f"cash.{currency}": new_cash, "updated_at": now_iso}}
        )

        #Update/create hold: weighted average cost
        if not pos:
            new_pos = {
                "user_id": current_user["id"],
                "code": normalized_code,
                "market": market,
                "currency": currency,
                "quantity": qty,
                "available_qty": qty if market != "CN" else 0,  #A shares T+1, not available today
                "frozen_qty": 0,
                "avg_cost": price,
                "updated_at": now_iso
            }
            await db["paper_positions"].insert_one(new_pos)
        else:
            old_qty = int(pos.get("quantity", 0))
            old_cost = float(pos.get("avg_cost", 0.0))
            new_qty = old_qty + qty
            new_avg = round((old_cost * old_qty + price * qty) / new_qty, 4) if new_qty > 0 else price

            #Unit A T+1: New acquisitions not available
            if market == "CN":
                new_available = pos.get("available_qty", old_qty)  #Maintenance of available quantities
            else:
                new_available = new_qty  #Port Unit/United States Unit T+0, all available

            await db["paper_positions"].update_one(
                {"_id": pos["_id"]},
                {"$set": {
                    "quantity": new_qty,
                    "available_qty": new_available,
                    "avg_cost": new_avg,
                    "updated_at": now_iso
                }}
            )

    else:  # sell
        #Check available quantity (consider T+1)
        available_qty = await _get_available_quantity(current_user["id"], normalized_code, market)
        if available_qty < qty:
            raise HTTPException(
                status_code=400,
                detail=f"可用持仓不足：需要 {qty}，可用 {available_qty}"
            )

        old_qty = int(pos.get("quantity", 0))
        avg_cost = float(pos.get("avg_cost", 0.0))
        new_qty = old_qty - qty
        pnl = round((price - avg_cost) * qty, 2)
        realized_pnl_delta = pnl

        #Revenue from sales (plus corresponding currency accounts, less handling fees)
        net_proceeds = notional - commission
        await db["paper_accounts"].update_one(
            {"user_id": current_user["id"]},
            {
                "$inc": {
                    f"cash.{currency}": net_proceeds,
                    f"realized_pnl.{currency}": realized_pnl_delta
                },
                "$set": {"updated_at": now_iso}
            }
        )

        #Update holdout
        if new_qty == 0:
            await db["paper_positions"].delete_one({"_id": pos["_id"]})
        else:
            new_available = max(0, pos.get("available_qty", old_qty) - qty)
            await db["paper_positions"].update_one(
                {"_id": pos["_id"]},
                {"$set": {
                    "quantity": new_qty,
                    "available_qty": new_available,
                    "updated_at": now_iso
                }}
            )

    #Recording of orders and transactions (i.e., yes)
    order_doc = {
        "user_id": current_user["id"],
        "code": normalized_code,
        "market": market,
        "currency": currency,
        "side": side,
        "quantity": qty,
        "price": price,
        "amount": notional,
        "commission": commission,
        "status": "filled",
        "created_at": now_iso,
        "filled_at": now_iso,
    }
    if analysis_id:
        order_doc["analysis_id"] = analysis_id
    await db["paper_orders"].insert_one(order_doc)

    trade_doc = {
        "user_id": current_user["id"],
        "code": normalized_code,
        "market": market,
        "currency": currency,
        "side": side,
        "quantity": qty,
        "price": price,
        "amount": notional,
        "commission": commission,
        "pnl": realized_pnl_delta if side == "sell" else 0.0,
        "timestamp": now_iso,
    }
    if analysis_id:
        trade_doc["analysis_id"] = analysis_id
    await db["paper_trades"].insert_one(trade_doc)

    return ok({"order": {k: v for k, v in order_doc.items() if k != "_id"}})


@router.get("/positions", response_model=dict)
async def list_positions(current_user: dict = Depends(get_current_user)):
    """Get hold list (support multi-market)"""
    db = get_mongo_db()
    items = await db["paper_positions"].find({"user_id": current_user["id"]}).to_list(None)
    enriched: List[Dict[str, Any]] = []
    for p in items:
        code = p.get("code")
        market = p.get("market", "CN")
        currency = p.get("currency", "CNY")
        qty = int(p.get("quantity", 0))
        available_qty = p.get("available_qty", qty)
        avg_cost = float(p.get("avg_cost", 0.0))

        last = await _get_last_price(code, market)
        mkt = round((last or 0.0) * qty, 2)
        enriched.append({
            "code": code,
            "market": market,
            "currency": currency,
            "quantity": qty,
            "available_qty": available_qty,
            "avg_cost": avg_cost,
            "last_price": last,
            "market_value": mkt,
            "unrealized_pnl": None if last is None else round((last - avg_cost) * qty, 2)
        })
    return ok({"items": enriched})


@router.get("/orders", response_model=dict)
async def list_orders(limit: int = Query(50, ge=1, le=200), current_user: dict = Depends(get_current_user)):
    db = get_mongo_db()
    cursor = db["paper_orders"].find({"user_id": current_user["id"]}).sort("created_at", -1).limit(limit)
    items = await cursor.to_list(None)
    #Remove  id
    cleaned = [{k: v for k, v in it.items() if k != "_id"} for it in items]
    return ok({"items": cleaned})


@router.post("/reset", response_model=dict)
async def reset_account(confirm: bool = Query(False), current_user: dict = Depends(get_current_user)):
    """Reset Account (Multi-currency support)"""
    if not confirm:
        raise HTTPException(status_code=400, detail="请设置 confirm=true 以确认重置")
    db = get_mongo_db()
    await db["paper_accounts"].delete_many({"user_id": current_user["id"]})
    await db["paper_positions"].delete_many({"user_id": current_user["id"]})
    await db["paper_orders"].delete_many({"user_id": current_user["id"]})
    await db["paper_trades"].delete_many({"user_id": current_user["id"]})
    #Recreate Account
    acc = await _get_or_create_account(current_user["id"])
    return ok({"message": "账户已重置", "cash": acc.get("cash", {})})