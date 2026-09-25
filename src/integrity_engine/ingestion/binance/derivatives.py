"""Binance USD-M perpetual public trade and liquidation ingestion."""
from integrity_engine.ingestion.derivatives.base import DerivativesWebsocket

URL="wss://fstream.binance.com/stream"
def subscription(symbol="BTCUSDT"):
    s=symbol.lower();return {"method":"SUBSCRIBE","params":[f"{s}@aggTrade",f"{s}@forceOrder"],"id":1}
def classifier(payload):
    data=payload.get("data",payload)
    if data.get("e")=="aggTrade":return "perpetual_trade"
    if data.get("e")=="forceOrder":return "liquidation"
    return None
def ingestor(producer,symbol="BTCUSDT"):return DerivativesWebsocket("binance",URL,symbol,subscription(symbol),classifier,producer)
