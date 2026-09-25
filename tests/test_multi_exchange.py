"""Offline parsing and normalization tests for Kraken, OKX, and Bybit."""
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock
import pytest
from integrity_engine.normalization.multi import MultiExchangeTradeNormalizer
from integrity_engine.normalization.base import NormalizationError
from integrity_engine.ingestion.kraken.trades import KrakenTradeIngestor, KrakenTradeSettings
from integrity_engine.ingestion.okx.trades import OKXTradeIngestor, OKXTradeSettings
from integrity_engine.ingestion.bybit.trades import BybitTradeIngestor, BybitTradeSettings

def envelope(exchange, symbol, payload):
    return {"schema_version":"1.0","exchange":exchange,"stream":"trades","symbol":symbol,
            "exchange_symbol":symbol,"received_timestamp":"2026-09-19T19:00:00+00:00","payload":payload}

@pytest.fixture
def raws():
    return [
        envelope("kraken","BTC/USD",{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","price":"100000.12345678","qty":"0.00100000","side":"buy","trade_id":123,"timestamp":"2026-09-19T19:00:00.123456Z"}]}),
        envelope("okx","BTC-USDT",{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"456","px":"100000.12345678","sz":"0.00100000","side":"sell","ts":"1789844400123"}]}),
        envelope("bybit","BTCUSDT",{"topic":"publicTrade.BTCUSDT","data":[{"T":1789844400123,"s":"BTCUSDT","S":"Buy","p":"100000.12345678","v":"0.00100000","i":"789"}]}),
    ]

def test_all_new_exchange_normalizers_preserve_precision_and_metadata(raws):
    events=[MultiExchangeTradeNormalizer().normalize(raw) for raw in raws]
    assert [e.exchange for e in events] == ["kraken","okx","bybit"]
    assert all(e.price == Decimal("100000.12345678") and e.quantity == Decimal("0.00100000") for e in events)
    assert [e.side for e in events] == ["buy","sell","buy"]
    assert all(e.base_asset == "BTC" and e.canonical_asset_pair == "BTC-USD" for e in events)
    assert [e.trade_id for e in events] == ["123","456","789"]
    assert all(e.exchange_timestamp.tzinfo == timezone.utc for e in events)

@pytest.mark.parametrize("index", [0,1,2])
def test_missing_fields_are_rejected(raws, index):
    raw=raws[index]; raw["payload"]["data"][0].pop(next(iter(raw["payload"]["data"][0])))
    with pytest.raises(NormalizationError): MultiExchangeTradeNormalizer().normalize(raw)

@pytest.mark.parametrize("kind", ["kraken","okx","bybit"])
async def test_mocked_adapter_publishes_raw(kind):
    messages={
      "kraken": '{"channel":"trade","data":[{"symbol":"BTC/USD","price":"1","qty":"1","side":"buy","trade_id":1,"timestamp":"2026-09-19T19:00:00Z"}]}',
      "okx": '{"arg":{"channel":"trades"},"data":[{"instId":"BTC-USDT","tradeId":"1","px":"1","sz":"1","side":"buy","ts":"1789844400123"}]}',
      "bybit": '{"topic":"publicTrade.BTCUSDT","data":[{"T":1789844400123,"s":"BTCUSDT","S":"Buy","p":"1","v":"1","i":"1"}]}'
    }
    settings={"kraken":KrakenTradeSettings,"okx":OKXTradeSettings,"bybit":BybitTradeSettings}[kind](kafka_bootstrap_servers="broker:9092")
    ingestor={"kraken":KrakenTradeIngestor,"okx":OKXTradeIngestor,"bybit":BybitTradeIngestor}[kind](settings,AsyncMock())
    assert await ingestor.handle_message(messages[kind],datetime.now(timezone.utc))
    ingestor.producer.publish.assert_awaited_once()
