"""Order-book feature aggregation from normalized snapshots."""
from decimal import Decimal
def orderbook_features(events, previous=None):
    if not events: return {}
    e=events[-1]; out={"best_bid":e.best_bid_price,"best_ask":e.best_ask_price,"mid_price":e.mid_price,"spread":e.spread,
      "spread_bps":e.spread/e.mid_price*Decimal(10000) if e.mid_price else None}
    for n in (1,5,10,20):
        bid=sum((x.quantity for x in e.bids[:n]),Decimal(0)); ask=sum((x.quantity for x in e.asks[:n]),Decimal(0)); d=bid+ask
        out[f"bid_depth_{n}"]=bid; out[f"ask_depth_{n}"]=ask; out[f"orderbook_imbalance_{n}"]=(bid-ask)/d if d else None
    if previous:
        out["spread_change"]=out["spread"]-previous.get("spread",out["spread"])
        out["depth_change"]=(out["bid_depth_20"]+out["ask_depth_20"])-previous.get("bid_depth_20",out["bid_depth_20"])-previous.get("ask_depth_20",out["ask_depth_20"])
    return out
