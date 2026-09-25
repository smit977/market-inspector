"""Exchange-specific derivatives normalization."""

from integrity_engine.normalization.derivatives.binance import normalize_binance
from integrity_engine.normalization.derivatives.bybit import normalize_bybit
from integrity_engine.normalization.derivatives.okx import normalize_okx

def normalize_derivative(raw):
    exchange = raw.get("exchange")
    if exchange == "binance": return normalize_binance(raw)
    if exchange == "bybit": return normalize_bybit(raw)
    if exchange == "okx": return normalize_okx(raw)
    raise ValueError("unsupported derivatives exchange")


def derivative_rows(raw):
    """Yield isolated envelopes in wire order, including nested liquidation details."""
    if not isinstance(raw, dict):
        yield raw
        return
    payload = raw.get("payload")
    # Binance combined-stream wrapper surrounds one event (or an array).
    if raw.get("exchange") == "binance" and isinstance(payload, dict) and "stream" in payload:
        payload = payload.get("data")
    data = payload.get("data") if isinstance(payload, dict) else payload
    rows = data if isinstance(data, list) else [data if isinstance(data, dict) else payload]
    if not rows:
        rows = [None]
    for row in rows:
        details = row.get("details") if isinstance(row, dict) and raw.get("event_type") == "liquidation" else None
        detail_rows = details if isinstance(details, list) and details else [None]
        for detail in detail_rows:
            isolated = dict(row, details=[detail]) if detail is not None else row
            if raw.get("exchange") == "binance":
                new_payload = isolated
            else:
                new_payload = dict(payload or {}, data=[isolated]) if isinstance(payload, dict) else {"data":[isolated]}
            yield dict(raw, payload=new_payload)


def normalize_derivative_batch(raw):
    """Return one result per row; invalid rows never discard valid siblings."""
    import json
    from uuid import NAMESPACE_URL, uuid5
    from integrity_engine.normalization.base import NormalizationError
    for index, row in enumerate(derivative_rows(raw)):
        try:
            event = normalize_derivative(row)
            if event.event_type == "liquidation":
                identity = json.dumps(row.get("payload"), sort_keys=True, separators=(",", ":"))
                event = event.model_copy(update={"event_id":uuid5(NAMESPACE_URL, f"{event.exchange}:{event.exchange_symbol}:liquidation:{index}:{identity}")})
            yield index, row, event, None
        except (ValueError, TypeError, KeyError, IndexError, AttributeError) as error:
            yield index, row, None, error if isinstance(error, NormalizationError) else NormalizationError("invalid derivatives row: " + type(error).__name__)
