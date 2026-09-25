"""Independent interpretable detectors and centralized thresholds."""
from dataclasses import dataclass
from uuid import uuid4
from datetime import timezone
from integrity_engine.schemas.anomaly import MarketAnomaly, AnomalyEvidence

@dataclass(frozen=True)
class DetectionConfig:
    z_threshold: float=3.0
    severe_z_threshold: float=5.0
    confirmation_count: int=2
    cooldown_seconds: float=10.0
    baseline_minutes: int=30
    minimum_observations: int=20

def evidence(metric,value,baseline,threshold,reason):
    z=baseline.zscore(value)
    return AnomalyEvidence(metric=metric,current_value=value,baseline_mean=baseline.mean,baseline_std=baseline.std,z_score=z,percentile=baseline.percentile(value),threshold=threshold,reason=reason)

def detect(metrics, baselines, config=DetectionConfig()):
    if metrics.data_quality_flags: return []
    triggered=[]; ev=[]; direction="neutral"
    def check(name, event_type, value, reason, absolute=True):
        if value is None:
            return None
        b=baselines.get(name); z=b.zscore(abs(value) if absolute else value) if b else None
        if z is not None and abs(z)>=config.z_threshold:
            triggered.append(event_type); ev.append(evidence(name,abs(value) if absolute else value,b,config.z_threshold,reason)); return z
        return None
    z=check("absolute_return","rapid_price_move",metrics.absolute_return,"absolute return exceeds baseline")
    if metrics.price_return is not None: direction="up" if metrics.price_return>0 else "down" if metrics.price_return<0 else "flat"
    check("quote_volume","volume_acceleration",metrics.quote_volume,"quote volume acceleration")
    check("trade_count","volume_acceleration",metrics.trade_count,"trade count acceleration")
    check("price_range","volatility_breakout",metrics.price_range,"price range expansion")
    check("spread_bps","liquidity_shock",metrics.spread_bps,"spread expansion")
    check("depth_change","liquidity_shock",-metrics.depth_change if metrics.depth_change is not None else None,"depth loss",True)
    check("orderbook_imbalance_20","orderbook_imbalance_shift",metrics.orderbook_imbalance_20,"order-book imbalance shift")
    flow=check("trade_imbalance","trade_flow_imbalance",metrics.trade_imbalance,"trade-flow imbalance")
    if not triggered:return []
    unique=list(dict.fromkeys(triggered)); confirmed=len(unique)>=config.confirmation_count
    if not confirmed and len(unique)==1 and (ev[0].z_score or 0)<config.severe_z_threshold:return []
    severity="high" if len(unique)>=3 or any(abs(x.z_score or 0)>=config.severe_z_threshold for x in ev) else "moderate" if len(unique)>=2 else "low"
    grouped=[]
    for et in unique:
        grouped.append(et)
    return [MarketAnomaly(event_id=uuid4(),event_type=et,exchange=metrics.exchange,symbol=metrics.symbol,detected_at=metrics.calculated_at,window=metrics.window,severity=severity,direction=direction,detectors_triggered=grouped,metrics_snapshot=metrics.model_dump(mode="json"),evidence=ev,baseline_ready=True,data_quality_flags=[] ) for et in unique]
