"""Event-time cross-venue BTC-USD trade features.

Returns use the first and last trade in each event-time window. A venue is
active when its most recently received trade is newer than the configured
freshness threshold. Volume HHI is the sum of squared venue quote-volume
shares; it is descriptive concentration, not an integrity judgment.
"""

from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from statistics import median
from typing import Any
from uuid import uuid4

import structlog

from integrity_engine.schemas.cross_venue import CrossVenueMetrics
from integrity_engine.schemas.trade import TradeEvent

log = structlog.get_logger(__name__)
VENUES = ("binance", "coinbase", "kraken", "okx", "bybit")
WINDOWS = {"10s": 10, "30s": 30, "1m": 60}


class CrossVenueFeatureEngine:
    def __init__(self, stale_after_ms: int = 5000, minimum_active_venues: int = 3,
                 flat_return_threshold: Decimal = Decimal("0.00005"), move_threshold: Decimal = Decimal("0.0005")) -> None:
        self.stale_after = timedelta(milliseconds=stale_after_ms)
        self.minimum_active_venues = minimum_active_venues
        self.flat_return_threshold = flat_return_threshold
        self.move_threshold = move_threshold
        self.trades: dict[str, deque[TradeEvent]] = {venue: deque() for venue in VENUES}
        self.latest: dict[str, TradeEvent] = {}
        self.latest_received: dict[str, datetime] = {}
        self.latest_timestamp: dict[str, datetime] = {}
        self.processed = self.malformed = self.timestamp_disorder = 0

    def add_trade(self, event: TradeEvent) -> bool:
        if event.exchange not in VENUES or event.canonical_asset_pair != "BTC-USD":
            return False
        venue = event.exchange
        if venue in self.latest_timestamp and event.exchange_timestamp < self.latest_timestamp[venue]:
            self.timestamp_disorder += 1
            log.warning("cross_venue_timestamp_disorder", exchange=venue)
        self.latest_timestamp[venue] = max(self.latest_timestamp.get(venue, event.exchange_timestamp), event.exchange_timestamp)
        if venue not in self.latest or event.exchange_timestamp >= self.latest[venue].exchange_timestamp:
            self.latest[venue] = event
            self.latest_received[venue] = event.received_timestamp
        if len(self.trades[venue]) >= 200000:
            raise RuntimeError("cross-venue rolling state capacity exceeded")
        self.trades[venue].append(event)
        self.processed += 1
        self.expire(event.exchange_timestamp)
        return True

    def expire(self, now: datetime) -> None:
        cutoff = now - timedelta(seconds=max(WINDOWS.values()))
        for venue, values in self.trades.items():
            while values and values[0].exchange_timestamp < cutoff:
                values.popleft()

    def _active(self, now: datetime) -> tuple[list[str], list[str]]:
        active, stale = [], []
        for venue in VENUES:
            received = self.latest_received.get(venue)
            if venue not in self.latest or received is None or not timedelta(0) <= now - received <= self.stale_after or not timedelta(0) <= now - self.latest[venue].exchange_timestamp <= self.stale_after:
                stale.append(venue)
            else:
                active.append(venue)
        return active, stale

    @staticmethod
    def _quote(event: TradeEvent) -> Decimal:
        if event.quote_quantity is not None:
            return event.quote_quantity
        with localcontext() as ctx:
            ctx.prec = max(28, len(event.price.as_tuple().digits) + len(event.quantity.as_tuple().digits))
            return event.price * event.quantity

    def _window_values(self, venue: str, now: datetime, seconds: int) -> list[TradeEvent]:
        start = now - timedelta(seconds=seconds)
        return sorted((event for event in self.trades[venue] if start <= event.exchange_timestamp <= now), key=lambda e:e.exchange_timestamp)

    def _return(self, values: list[TradeEvent]) -> Decimal | None:
        if len(values) < 2 or values[0].price <= 0:
            return None
        return (values[-1].price - values[0].price) / values[0].price

    def _lead_lag(self, now: datetime, active: list[str]) -> tuple[str | None, dict[str, int]]:
        crossings: dict[str, datetime] = {}
        for venue in active:
            values = self._window_values(venue, now, 10)
            if len(values) < 2:
                continue
            move = abs((values[-1].price - values[0].price) / values[0].price)
            if move >= self.move_threshold:
                crossings[venue] = values[0].exchange_timestamp
        if not crossings:
            return None, {}
        first = min(crossings, key=crossings.get)
        origin = crossings[first]
        return first, {venue: max(0, int((timestamp - origin).total_seconds() * 1000)) for venue, timestamp in crossings.items()}

    def calculate(self, window: str, now: datetime | None = None) -> CrossVenueMetrics:
        if window not in WINDOWS:
            raise ValueError(f"Unsupported cross-venue window: {window}")
        now = now or datetime.now(timezone.utc)
        self.expire(now)
        cutoff = now - timedelta(seconds=max(WINDOWS.values()))
        for venue, values in self.trades.items():
            self.trades[venue] = deque(e for e in values if e.exchange_timestamp >= cutoff)
        active, stale = self._active(now)
        prices = {venue: self.latest[venue].price for venue in active}
        flags: list[str] = []
        if len(active) < self.minimum_active_venues: flags.append("insufficient_active_venues")
        if stale: flags.append("stale_venue_data")
        if self.timestamp_disorder: flags.append("timestamp_disorder")
        if any(venue not in prices for venue in VENUES): flags.append("missing_venue_price")
        median_price = Decimal(str(median(prices.values()))) if prices else None
        mean_price = sum(prices.values(), Decimal(0)) / Decimal(len(prices)) if prices else None
        minimum = min(prices.values()) if prices else None
        maximum = max(prices.values()) if prices else None
        dispersion = maximum - minimum if maximum is not None and minimum is not None else None
        dispersion_bps = (dispersion / median_price * Decimal(10000)) if dispersion is not None and median_price else None
        deviations = {venue: (price - median_price) / median_price * Decimal(10000) for venue, price in prices.items()} if median_price else {}
        returns = {venue: self._return(self._window_values(venue, now, WINDOWS[window])) for venue in active}
        valid_returns = [value for value in returns.values() if value is not None]
        up = sum(value > self.flat_return_threshold for value in valid_returns)
        down = sum(value < -self.flat_return_threshold for value in valid_returns)
        flat = len(valid_returns) - up - down
        same = max(up, down) if valid_returns else 0
        ratio = Decimal(same) / Decimal(len(valid_returns)) if valid_returns else None
        if len(valid_returns) < self.minimum_active_venues:
            state = "insufficient_data"
            flags.append("insufficient_venue_returns")
        elif same == 0: state = "neutral"
        elif ratio is not None and ratio >= Decimal("0.8"): state = "broad"
        elif ratio is not None and ratio >= Decimal("0.6"): state = "partial"
        else: state = "fragmented"
        median_return = Decimal(str(median(valid_returns))) if valid_returns else None
        return_dispersion = max(valid_returns) - min(valid_returns) if valid_returns else None
        quote_volumes = {venue: sum((self._quote(e) for e in self._window_values(venue, now, WINDOWS[window])), Decimal(0)) for venue in active}
        total = sum(quote_volumes.values(), Decimal(0))
        shares = {venue: volume / total for venue, volume in quote_volumes.items()} if total else {}
        largest = max(shares.values()) if shares else None
        hhi = sum((share * share for share in shares.values()), Decimal(0)) if shares else None
        first, lags = self._lead_lag(now, active)
        return CrossVenueMetrics(event_id=uuid4(), window=window, calculated_at=now, active_venues=active, stale_venues=stale,
            median_price=median_price, mean_price=mean_price, min_price=minimum, max_price=maximum,
            dispersion=dispersion, dispersion_bps=dispersion_bps, venue_prices=prices, venue_deviation_bps=deviations,
            venue_returns=returns, up_venue_count=up, down_venue_count=down, flat_venue_count=flat,
            same_direction_count=same, confirmation_ratio=ratio, confirmation_state=state,
            median_return=median_return, return_dispersion=return_dispersion, venue_quote_volumes=quote_volumes,
            venue_volume_shares=shares, largest_venue_volume_share=largest, volume_hhi=hhi,
            first_observed_venue=first, follow_lag_ms=lags, data_complete=len(valid_returns) >= self.minimum_active_venues,
            data_quality_flags=flags)
