from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4
from integrity_engine.integrity.scoring import score_integrity
from integrity_engine.schemas.anomaly import MarketAnomaly
from integrity_engine.schemas.cross_venue import CrossVenueMetrics
from integrity_engine.schemas.metrics import MarketMetrics
from integrity_engine.schemas.derivatives import DerivativesMetrics

T=datetime(2026,9,19,22,0,tzinfo=timezone.utc)
def anomaly(): return MarketAnomaly(event_id=uuid4(),event_type="rapid_price_move",exchange="binance",symbol="BTC-USDT",detected_at=T,window="10s",severity="high",direction="up",detectors_triggered=["rapid_price_move"],metrics_snapshot={},evidence=[],baseline_ready=True)
def cross(): return CrossVenueMetrics(event_id=uuid4(),window="10s",calculated_at=T,active_venues=["binance","coinbase","kraken","okx","bybit"],median_price=Decimal("100"),mean_price=Decimal("100"),min_price=Decimal("99.99"),max_price=Decimal("100.01"),dispersion=Decimal(".02"),dispersion_bps=Decimal("2"),confirmation_ratio=Decimal("1"),confirmation_state="broad",data_complete=True,venue_quote_volumes={"binance":Decimal("20"),"coinbase":Decimal("20")},venue_volume_shares={"binance":Decimal(".5"),"coinbase":Decimal(".5")},largest_venue_volume_share=Decimal(".5"),volume_hhi=Decimal(".5"))
def spot(): return MarketMetrics(event_id=uuid4(),exchange="binance",symbol="BTC-USDT",window="10s",window_start=T,window_end=T,calculated_at=T,quote_volume=Decimal("100"),trade_count=10,spread_bps=Decimal("0"),depth_change=Decimal("0"),data_complete=True)
def deriv(**kwargs): return DerivativesMetrics(event_id=uuid4(),exchange="okx",window="10s",calculated_at=T,perpetual_quote_volume=kwargs.get("perpetual_quote_volume",Decimal("100")),spot_quote_volume=kwargs.get("spot_quote_volume",Decimal("100")),perpetual_to_spot_volume_ratio=kwargs.get("ratio",Decimal("1")),basis_bps=Decimal("1"),data_complete=True)
def test_broad_move_has_high_confirmation_and_serializes():
    event=score_integrity(anomaly(),spot(),cross(),deriv())
    assert event.integrity_score>Decimal("70")
    assert event.evidence_coverage==Decimal("0.875")
    assert event.classification in {"Organic Trend","Mixed / Unknown"}
    restored=type(event).model_validate_json(event.model_dump_json())
    assert restored.integrity_score==event.integrity_score and restored.classification==event.classification
def test_derivatives_heavy_move_reduces_dependence_score():
    event=score_integrity(anomaly(),spot(),cross(),deriv(perpetual_quote_volume=Decimal("1000"),ratio=Decimal("10")))
    component=next(c for c in event.components if c.name=="Derivatives Dependence")
    assert component.score<50 and event.classification=="Derivatives-Amplified Move"
def test_missing_derivatives_reduces_confidence_not_score_zero():
    event=score_integrity(anomaly(),spot(),cross(),None)
    component=next(c for c in event.components if c.name=="Derivatives Dependence")
    assert component.score is None and component.availability=="unavailable" and component.confidence==0
    assert event.evidence_coverage<1 and event.overall_confidence<1
def test_stale_evidence_is_unavailable():
    old=cross().model_copy(update={"calculated_at":T.replace(hour=21)})
    event=score_integrity(anomaly(),spot(),old,deriv(),max_age_seconds=5)
    assert any("missing_cross_venue_metrics" in c.data_quality_flags for c in event.components)
