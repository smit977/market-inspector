from decimal import Decimal
def classify(components):
    by={c.name:c for c in components}; cross=by["Cross-Venue Confirmation"];spot=by["Spot Participation"];liq=by["Liquidity Stability"];vol=by["Volume Quality"];der=by["Derivatives Dependence"]
    # Availability and competing evidence are explicit: do not let code order
    # turn simultaneous strong signals into an arbitrary single explanation.
    if any(c.availability == "unavailable" for c in (cross,spot,liq,vol,der)):
        return "Mixed / Unknown", Decimal("0.25")
    if liq.status == "stressed":
        if der.status in {"high_dependence","elevated_dependence"} and der.confidence >= Decimal("0.3"):
            return "Mixed / Unknown", min(liq.confidence, der.confidence) * Decimal("0.7")
        return "Liquidity Shock", min(cross.confidence,liq.confidence) * Decimal("0.85")
    if cross.status == "fragmented" and (cross.score or 0) < 50:
        return "Cross-Venue Divergence", min(cross.confidence,vol.confidence) * Decimal("0.85")
    if der.status in {"high_dependence","elevated_dependence"} and spot.status in {"weak","moderate"}:
        return "Derivatives-Amplified Move", min(der.confidence,spot.confidence) * Decimal("0.85")
    if vol.status == "concentrated" and (cross.score or 0) < 70:
        return "Volume Anomaly", min(vol.confidence,cross.confidence) * Decimal("0.8")
    if cross.status in {"broad","partial"} and spot.status in {"strong","moderate"} and liq.status == "stable" and der.status == "balanced":
        return "Organic Trend", min(cross.confidence,spot.confidence,liq.confidence,der.confidence) * Decimal("0.9")
    return "Mixed / Unknown", Decimal("0.35")
