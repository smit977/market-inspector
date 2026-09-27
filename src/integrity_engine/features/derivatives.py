"""Venue-scoped rolling derivatives, explicit units and freshness."""
import os
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from statistics import median
from uuid import uuid4
from integrity_engine.schemas.derivatives import PerpetualTrade, FundingRate, OpenInterest, LiquidationEvent, DerivativesMetrics
WINDOWS={"10s":10,"30s":30,"1m":60,"5m":300}
D=Decimal

class DerivativesFeatureEngine:
    def __init__(self):
        self.events=deque(); self.spot_events=deque()
        self.latest_oi=self.previous_oi=self.latest_funding=self.previous_funding=None
        self.latest_perpetual=self.latest_spot=None
        self.spot_metrics={}; self.exchange=None
        self.max_age=float(os.getenv("DERIVATIVES_SPOT_MAX_AGE_SECONDS","5"))
        self.max_events=200000

    def add(self,event):
        if self.exchange is not None and event.exchange != self.exchange:
            raise ValueError("cannot combine derivatives from different venues or OI units")
        self.exchange=event.exchange
        if len(self.events)>=self.max_events:
            raise RuntimeError("derivatives rolling state capacity exceeded")
        if isinstance(event,OpenInterest):
            if self.latest_oi and event.quantity_unit != self.latest_oi.quantity_unit:
                raise ValueError("incompatible open-interest units")
            if not self.latest_oi or event.exchange_timestamp>=self.latest_oi.exchange_timestamp:
                self.previous_oi,self.latest_oi=self.latest_oi,event
        elif isinstance(event,FundingRate):
            if not self.latest_funding or event.exchange_timestamp>=self.latest_funding.exchange_timestamp:
                self.previous_funding,self.latest_funding=self.latest_funding,event
        elif isinstance(event,PerpetualTrade):
            if not self.latest_perpetual or event.exchange_timestamp>=self.latest_perpetual.exchange_timestamp:
                self.latest_perpetual=event
        self.events.append(event)

    def add_spot(self,event):
        if len(self.spot_events)>=self.max_events:
            raise RuntimeError("spot rolling state capacity exceeded")
        self.spot_events.append(event); self.latest_spot=event

    def add_spot_metric(self,metric):
        self.spot_metrics[metric.get("window","10s")]=metric

    def expire(self,now):
        cutoff=now-timedelta(seconds=300)
        self.events=deque(e for e in self.events if e.exchange_timestamp>=cutoff)
        self.spot_events=deque(e for e in self.spot_events if e.exchange_timestamp>=cutoff)

    def fresh(self,stamp,now,age=None):
        return stamp is not None and 0 <= (now-stamp).total_seconds() <= (self.max_age if age is None else age)

    def calculate(self,window,now=None,spot_price=None,spot_volume=None,spot_return=None):
        now=now or datetime.now(timezone.utc); self.expire(now)
        start=now-timedelta(seconds=WINDOWS[window])
        items=sorted((e for e in self.events if start<=e.exchange_timestamp<=now),key=lambda e:e.exchange_timestamp)
        spot=sorted((e for e in self.spot_events if start<=e.exchange_timestamp<=now),key=lambda e:e.exchange_timestamp)
        trades=[e for e in items if isinstance(e,PerpetualTrade)]
        liqs=[e for e in items if isinstance(e,LiquidationEvent)]
        flags=[]; refstamp=None
        price=self.latest_perpetual.price if self.latest_perpetual and self.fresh(self.latest_perpetual.exchange_timestamp,now) else None
        if price is None: flags.append("stale_perpetual_price")
        first=trades[0].price if trades else None
        ret=(price-first)/first if price is not None and first else None
        compatible=bool(trades) and all(e.quantity_unit=="base" and e.comparable for e in trades)
        base=sum((e.quantity for e in trades),D(0)) if compatible else None
        quote=sum((e.quote_quantity for e in trades),D(0)) if trades and all(e.quote_quantity is not None and e.comparable for e in trades) else None
        if trades and not compatible: flags.append("venue_specific_contract_units")
        buy=sum((e.quantity for e in trades if e.side=="buy"),D(0));sell=sum((e.quantity for e in trades if e.side=="sell"),D(0))
        imb=(buy-sell)/(buy+sell) if buy+sell else None
        oi=self.latest_oi.open_interest if self.latest_oi and self.fresh(self.latest_oi.received_timestamp,now,10) else None
        oi_change=oi-self.previous_oi.open_interest if oi is not None and self.previous_oi else None
        oi_pct=oi_change/self.previous_oi.open_interest if oi_change is not None and self.previous_oi.open_interest else None
        funding=self.latest_funding.funding_rate if self.latest_funding and self.fresh(self.latest_funding.received_timestamp,now,60) else None
        fund_change=funding-self.previous_funding.funding_rate if funding is not None and self.previous_funding else None
        if not trades:flags.append("insufficient_window_history")
        if oi is None:flags.append("missing_open_interest")
        if funding is None:flags.append("missing_funding")
        if spot and self.fresh(spot[-1].exchange_timestamp,now) and self.fresh(spot[-1].received_timestamp,now):
            refstamp=spot[-1].exchange_timestamp
            spot_price=D(str(median(e.price for e in spot)))
            spot_volume=sum((e.quote_quantity if e.quote_quantity is not None else e.price*e.quantity for e in spot),D(0))
            spot_return=(spot[-1].price-spot[0].price)/spot[0].price if len(spot)>1 else None
        else:
            # Caller-supplied prices without source timestamps cannot establish freshness.
            spot_price=spot_volume=spot_return=None
            fallback=self.spot_metrics.get(window)
            if fallback:
                stamp=fallback.get("calculated_at")
                refstamp=datetime.fromisoformat(stamp.replace("Z","+00:00")) if isinstance(stamp,str) else stamp
                if self.fresh(refstamp,now) and fallback.get("active_venues"):
                    spot_price=fallback.get("median_price")
                    volumes=fallback.get("venue_quote_volumes",{})
                    spot_volume=sum((D(str(v)) for v in volumes.values()),D(0)) if volumes else None
                    spot_return=fallback.get("median_return")
        spot_price=D(str(spot_price)) if spot_price is not None else None
        spot_volume=D(str(spot_volume)) if spot_volume is not None else None
        spot_return=D(str(spot_return)) if spot_return is not None else None
        if spot_price is None:flags.append("stale_spot_reference")
        basis=price-spot_price if price is not None and spot_price is not None else None
        longliq=sum((e.quantity for e in liqs if e.side=="sell"),D(0)) if liqs else None
        shortliq=sum((e.quantity for e in liqs if e.side=="buy"),D(0)) if liqs else None
        liqquote=sum((e.quote_value for e in liqs),D(0)) if liqs and all(e.quote_value is not None and e.comparable for e in liqs) else None
        return DerivativesMetrics(event_id=uuid4(),exchange=self.exchange or "unknown",window=window,calculated_at=now,
            perpetual_price=price,perpetual_return=ret,perpetual_base_volume=base,perpetual_quote_volume=quote,
            perpetual_trade_count=len(trades),perpetual_trade_imbalance=imb,open_interest=oi,
            open_interest_unit=self.latest_oi.quantity_unit if self.latest_oi else None,open_interest_comparable=False,
            open_interest_change=oi_change,open_interest_change_pct=oi_pct,funding_rate=funding,funding_rate_change=fund_change,
            long_liquidation_volume=longliq,short_liquidation_volume=shortliq,total_liquidation_volume=(longliq+shortliq) if liqs else None,
            liquidation_quote_volume=liqquote,liquidation_count=len(liqs),last_liquidation_at=max((e.exchange_timestamp for e in liqs),default=None),volume_unit=trades[-1].quantity_unit if trades else None,
            spot_reference_price=spot_price,spot_reference_timestamp=refstamp,basis=basis,
            basis_bps=basis/spot_price*D(10000) if basis is not None and spot_price else None,spot_quote_volume=spot_volume,
            perpetual_to_spot_volume_ratio=quote/spot_volume if quote is not None and spot_volume else None,
            spot_perpetual_return_difference=ret-spot_return if ret is not None and spot_return is not None else None,
            data_complete=bool(trades and price is not None and spot_price is not None and not flags),data_quality_flags=flags)
