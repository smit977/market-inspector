from decimal import Decimal
from datetime import datetime, timezone
from uuid import uuid4
from integrity_engine.schemas.integrity import MarketIntegrityEvent
from integrity_engine.integrity.components import cross,spot,liquidity,volume,derivatives,liquidation

def score_integrity(anomaly, metrics=None, cross_metrics=None, derivatives_metrics=None, max_age_seconds=5):
    now=anomaly.detected_at; quality=[]
    def fresh(value):
        if value is None or not hasattr(value,"calculated_at"): return value
        return value if abs((now-value.calculated_at).total_seconds()) <= max_age_seconds else None
    metrics, cross_metrics, derivatives_metrics = fresh(metrics), fresh(cross_metrics), fresh(derivatives_metrics)
    candidates=[]
    for fn,arg in ((cross,cross_metrics),(spot,metrics),(liquidity,metrics),(volume,metrics),(derivatives,derivatives_metrics),(liquidation,derivatives_metrics)):
        c=fn(arg,cross_metrics,derivatives_metrics) if fn is spot else fn(arg,cross_metrics) if fn is volume else fn(arg)
        candidates.append(c)
    available=[c for c in candidates if c.availability != "unavailable" and c.score is not None]
    denom=sum((c.weight for c in available),Decimal(0))
    score=sum((c.score*c.weight for c in available),Decimal(0))/denom if denom else Decimal(50)
    # Coverage measures intended weighted evidence that is usable; partial data
    # contributes half coverage while remaining eligible for score calculation.
    coverage=sum((c.weight * (Decimal("0.5") if c.availability == "partial" else Decimal(1) if c.availability == "available" else Decimal(0)) for c in candidates),Decimal(0))
    confidence=(sum((c.confidence*c.weight for c in available),Decimal(0))/denom * coverage) if denom else Decimal(0)
    for c in candidates: quality.extend(c.data_quality_flags)
    from integrity_engine.integrity.classification import classify
    classification,classification_conf=classify(candidates)
    return MarketIntegrityEvent(event_id=uuid4(),exchange_scope=anomaly.exchange,canonical_pair="BTC-USD",trigger_event_id=anomaly.event_id,trigger_type=anomaly.event_type,trigger_severity=anomaly.severity,move_direction=anomaly.direction,move_window=anomaly.window,integrity_score=score.quantize(Decimal("0.1")),overall_confidence=confidence.quantize(Decimal("0.001")),evidence_coverage=coverage.quantize(Decimal("0.001")),classification=classification,classification_confidence=classification_conf.quantize(Decimal("0.001")),components=candidates,data_quality_flags=sorted(set(quality)),calculated_at=datetime.now(timezone.utc))
