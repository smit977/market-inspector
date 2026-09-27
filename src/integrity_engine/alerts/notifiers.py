"""Bounded optional webhook delivery; safe metadata only, no raw events or URLs in logs."""
import asyncio
import os
import httpx
import structlog
from pydantic import BaseModel, SecretStr, model_validator
from urllib.parse import urlsplit

log=structlog.get_logger(__name__)
class NotificationConfig(BaseModel):
    enabled: bool=False
    url: SecretStr=SecretStr("")
    @model_validator(mode="after")
    def valid(self):
        if self.enabled:
            parsed=urlsplit(self.url.get_secret_value())
            if parsed.scheme!="https" or not parsed.hostname or parsed.username or parsed.password:
                raise ValueError("enabled webhook requires HTTPS without embedded credentials")
        return self
    @classmethod
    def from_env(cls):
        return cls(enabled=os.getenv("ALERT_WEBHOOK_ENABLED","false"),url=os.getenv("ALERT_WEBHOOK_URL",""))

class LoggingNotifier:
    async def notify(self,event):
        log.info("alert_notification",alert_id=event.alert_id,category=event.category,
                 alert_type=event.alert_type,severity=event.severity,status=event.status,summary=event.summary)
        return "logged"

class WebhookNotifier:
    def __init__(self,config,transport=None): self.config,self.transport=config,transport
    async def notify(self,event):
        if event.is_replay: return "replay_suppressed"
        payload={k:getattr(event,k) for k in ("alert_id","category","alert_type","canonical_pair","severity","status","summary")}
        # Only numeric observations and fixed metadata; no arbitrary nested source payloads.
        payload["evidence"]=[{"metric":e.metric,"current_value":e.current_value,"freshness":e.freshness}
            for e in event.evidence if isinstance(e.current_value,(int,float))][:12]
        async with httpx.AsyncClient(timeout=2,follow_redirects=False,transport=self.transport) as client:
            for attempt in range(3):
                try:
                    response=await client.post(self.config.url.get_secret_value(),json=payload)
                    response.raise_for_status()
                    return "sent"
                except Exception as error:
                    log.warning("alert_notification_failed",alert_id=event.alert_id,attempt=attempt+1,error_type=type(error).__name__)
                    if attempt<2: await asyncio.sleep(.1*2**attempt)
        return "failed"
