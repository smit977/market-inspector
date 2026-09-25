"""Centralized exchange-symbol mappings for cross-venue comparisons."""

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketMapping:
    exchange: str
    exchange_symbol: str
    symbol: str
    canonical_asset: str
    canonical_quote_family: str
    base_asset: str
    quote_asset: str
    canonical_asset_pair: str


_MAPPINGS = {
    ("binance", "BTCUSDT"): MarketMapping("binance", "BTCUSDT", "BTC-USDT", "BTC", "USD", "BTC", "USDT", "BTC-USD"),
    ("coinbase", "BTC-USD"): MarketMapping("coinbase", "BTC-USD", "BTC-USD", "BTC", "USD", "BTC", "USD", "BTC-USD"),
    ("kraken", "BTC/USD"): MarketMapping("kraken", "BTC/USD", "BTC-USD", "BTC", "USD", "BTC", "USD", "BTC-USD"),
    ("okx", "BTC-USDT"): MarketMapping("okx", "BTC-USDT", "BTC-USDT", "BTC", "USD", "BTC", "USDT", "BTC-USD"),
    ("bybit", "BTCUSDT"): MarketMapping("bybit", "BTCUSDT", "BTC-USDT", "BTC", "USD", "BTC", "USDT", "BTC-USD"),
}


def market_mapping(exchange: str, exchange_symbol: str) -> MarketMapping:
    """Resolve a supported venue symbol without conflating USD and USDT."""
    key = (exchange.strip().lower(), exchange_symbol.strip().upper())
    try:
        return _MAPPINGS[key]
    except KeyError as error:
        raise ValueError(f"Unsupported market mapping: {key[0]} {key[1]}") from error
