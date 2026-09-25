from decimal import Decimal
from integrity_engine.integrity.components import cross, liquidity, volume
from integrity_engine.integrity.scoring import score_integrity
from tests.test_integrity import anomaly, spot, cross as base_cross, deriv


def comp(event, name):
    return next(c for c in event.components if c.name == name)


def test_component_bounds_and_weight_arithmetic():
    event = score_integrity(anomaly(), spot(), base_cross(), deriv())
    assert sum((c.weight for c in event.components), Decimal(0)) == Decimal(1)
    for c in event.components:
        assert c.score is None or Decimal(0) <= c.score <= Decimal(100)
        assert Decimal(0) <= c.confidence <= Decimal(1)
    assert Decimal(0) <= event.integrity_score <= Decimal(100)
    assert Decimal(0) <= event.overall_confidence <= Decimal(1)
    assert Decimal(0) <= event.evidence_coverage <= Decimal(1)


def test_cross_venue_monotonicity():
    broad = cross(base_cross())
    fragmented = base_cross().model_copy(update={"confirmation_ratio": Decimal("0.2"), "confirmation_state": "fragmented", "dispersion_bps": Decimal("20")})
    assert cross(fragmented).score < broad.score


def test_liquidity_monotonicity():
    stable = spot().model_copy(update={"spread_bps": Decimal("1"), "depth_change": Decimal("0")})
    stressed = stable.model_copy(update={"spread_bps": Decimal("20"), "depth_change": Decimal("-0.8")})
    assert liquidity(stressed).score < liquidity(stable).score


def test_volume_concentration_monotonicity():
    broad = base_cross().model_copy(update={"largest_venue_volume_share": Decimal("0.3")})
    concentrated = broad.model_copy(update={"largest_venue_volume_share": Decimal("0.95"), "volume_hhi": Decimal("0.9")})
    assert volume(None, concentrated).score < volume(None, broad).score


def test_derivatives_dependence_and_missing_coverage():
    balanced = score_integrity(anomaly(), spot(), base_cross(), deriv(ratio=Decimal("1")))
    heavy = score_integrity(anomaly(), spot(), base_cross(), deriv(ratio=Decimal("100")))
    assert comp(heavy, "Derivatives Dependence").score < comp(balanced, "Derivatives Dependence").score
    missing = score_integrity(anomaly(), spot(), base_cross(), None)
    assert missing.evidence_coverage < balanced.evidence_coverage
    assert missing.overall_confidence < balanced.overall_confidence
    assert comp(missing, "Derivatives Dependence").availability == "unavailable"


def test_stale_orderbook_is_partial_not_a_fake_zero_score():
    stale = spot().model_copy(update={"data_quality_flags": ["stale_orderbook"]})
    component = liquidity(stale)
    assert component.availability == "partial"
    assert component.score is not None and component.score > 0
    event = score_integrity(anomaly(), stale, base_cross(), deriv())
    assert event.evidence_coverage < Decimal("1")


def test_liquidation_unavailable_has_no_placeholder_score():
    component = comp(score_integrity(anomaly(), spot(), base_cross(), deriv()), "Liquidation Dependence")
    assert component.score is None
    assert component.availability == "unavailable"


def test_conflicting_liquidity_and_derivatives_evidence_is_mixed():
    stressed = spot().model_copy(update={"spread_bps": Decimal("20"), "depth_change": Decimal("-0.8")})
    event = score_integrity(anomaly(), stressed, base_cross(), deriv(ratio=Decimal("20")))
    assert event.classification == "Mixed / Unknown"
    assert event.classification_confidence < event.overall_confidence
