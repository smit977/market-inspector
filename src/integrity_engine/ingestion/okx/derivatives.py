"""OKX swap public trade and liquidation ingestion."""
from integrity_engine.ingestion.derivatives.base import DerivativesWebsocket
URL="wss://ws.okx.com:8443/ws/v5/public"
def subscription(symbol="BTC-USDT-SWAP"):return {"op":"subscribe","args":[{"channel":"trades","instId":symbol},{"channel":"liquidation-orders","instType":"SWAP","uly":"BTC-USDT"}]}
def classifier(payload):
    if payload.get("event") or not isinstance(payload.get("data"),list) or not payload["data"]:
        return None
    channel=payload.get("arg",{}).get("channel")
    if channel=="trades":return "perpetual_trade"
    if channel=="liquidation-orders":
        rows=payload.get("data",[])
        if rows and rows[0].get("instId")=="BTC-USDT-SWAP":
            details=rows[0].get("details",[])
            if any(str(d.get("bkPx", "0")) not in {"", "0", "0.0"} and str(d.get("sz", "0")) not in {"", "0", "0.0"} for d in details): return "liquidation"
    return None
def ingestor(producer,symbol="BTC-USDT-SWAP"):return DerivativesWebsocket("okx",URL,symbol,subscription(symbol),classifier,producer)
