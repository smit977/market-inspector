"""Exchange-independent normalized event schemas."""

from integrity_engine.schemas.base import BaseEvent
from integrity_engine.schemas.trade import TradeEvent

__all__ = ["BaseEvent", "TradeEvent"]
