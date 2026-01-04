"""Shared document indicator processing function
- add daily basic indicators (market value/valuation/transaction) to the document Medium
"""
from typing import Dict


def add_financial_metrics(doc: Dict, daily_metrics: Dict) -> None:
    """Inclusion of financial and transactional indicators in doc (local change).
    - Market value: total mv/circ mv (converted from 10,000 to hundreds of millions)
    - Valuation: p/pb/pe tm/pb mrq/ps/ps ttm (filtration of NN/None)
    - Transactions: turnover rate/volume ratio (filtration of NN/None)
    - Equity: total share/float share (one million shares, filtered NAN/None)
    """
    #Market value (millions - > billions)
    if "total_mv" in daily_metrics and daily_metrics["total_mv"] is not None:
        doc["total_mv"] = daily_metrics["total_mv"] / 10000
    if "circ_mv" in daily_metrics and daily_metrics["circ_mv"] is not None:
        doc["circ_mv"] = daily_metrics["circ_mv"] / 10000

    #Valuation indicators (🔥) Add ps and ps ttm
    for field in ["pe", "pb", "pe_ttm", "pb_mrq", "ps", "ps_ttm"]:
        if field in daily_metrics and daily_metrics[field] is not None:
            try:
                value = float(daily_metrics[field])
                if not (value != value):  #Filter NAN
                    doc[field] = value
            except (ValueError, TypeError):
                pass

    #Transaction indicators
    for field in ["turnover_rate", "volume_ratio"]:
        if field in daily_metrics and daily_metrics[field] is not None:
            try:
                value = float(daily_metrics[field])
                if not (value != value):  #Filter NAN
                    doc[field] = value
            except (ValueError, TypeError):
                pass

    #🔥 Equity data (one million shares)
    for field in ["total_share", "float_share"]:
        if field in daily_metrics and daily_metrics[field] is not None:
            try:
                value = float(daily_metrics[field])
                if not (value != value):  #Filter NAN
                    doc[field] = value
            except (ValueError, TypeError):
                pass

