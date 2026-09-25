"""Environment-driven Fabric exporter configuration."""
from pydantic_settings import BaseSettings, SettingsConfigDict

class FabricSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    fabric_enabled: bool = False
    fabric_kafka_bootstrap_servers: str = ""
    fabric_kafka_username: str = ""
    fabric_kafka_password: str = ""
    fabric_security_protocol: str = "SASL_SSL"
    fabric_sasl_mechanism: str = "PLAIN"
    fabric_topic_metrics: str = ""
    fabric_topic_cross_venue: str = ""
    fabric_topic_derivatives: str = ""
    fabric_topic_anomalies: str = ""
    fabric_topic_integrity: str = ""
    fabric_export_group: str = "fabric-curated-export-v1"
    fabric_max_retries: int = 3
    fabric_retry_backoff_ms: int = 250
    fabric_buffer_limit: int = 1000

    @property
    def routing(self) -> dict[str, str]:
        return {
            "market.metrics": self.fabric_topic_metrics,
            "market.metrics.cross_venue": self.fabric_topic_cross_venue,
            "market.metrics.derivatives": self.fabric_topic_derivatives,
            "market.anomalies": self.fabric_topic_anomalies,
            "market.integrity": self.fabric_topic_integrity,
        }

    def validate_enabled(self) -> None:
        if self.fabric_enabled and not self.fabric_kafka_bootstrap_servers.strip():
            raise ValueError("FABRIC_KAFKA_BOOTSTRAP_SERVERS is required when FABRIC_ENABLED=true")
