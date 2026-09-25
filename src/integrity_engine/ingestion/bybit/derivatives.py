"""Bybit linear perpetual public trade and liquidation ingestion."""
from integrity_engine.ingestion.derivatives.base import DerivativesWebsocket
URL="wss://stream.bybit.com/v5/public/linear"
def subscription(symbol="BTCUSDT"):return {"op":"subscribe","args":[f"publicTrade.{symbol}",f"allLiquidation.{symbol}"]}
def classifier(payload):
    topic=payload.get("topic","")
    if topic.startswith("publicTrade."):return "perpetual_trade"
    if topic.startswith("allLiquidation."):return "liquidation"
    return None
def ingestor(producer,symbol="BTCUSDT"):return DerivativesWebsocket("bybit",URL,symbol,subscription(symbol),classifier,producer)
