"""Decimal trade feature aggregation."""
from decimal import Decimal
def trade_features(events):
    if not events: return {}
    prices=[e.price for e in events]; buys=sum((e.quantity for e in events if e.side=="buy"),Decimal(0)); sells=sum((e.quantity for e in events if e.side=="sell"),Decimal(0))
    quote=sum((e.quote_quantity or e.price*e.quantity for e in events),Decimal(0)); base=sum((e.quantity for e in events),Decimal(0)); denom=buys+sells
    first,last=prices[0],prices[-1]
    return {"price_return": (last-first)/first if first else None, "absolute_return": abs((last-first)/first) if first else None,
      "price_range": max(prices)-min(prices), "trade_count":len(events), "base_volume":base,"quote_volume":quote,
      "buy_volume":buys,"sell_volume":sells,"trade_imbalance":(buys-sells)/denom if denom else None,
      "vwap":quote/base if base else None,"average_trade_size":base/len(events),"largest_trade_size":max(e.quantity for e in events)}
