"""
shared/settings.py
──────────────────
Single source of truth for all configuration.
Reads from environment variables or a .env file.
All components import from here — nothing reads os.environ directly.
"""
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Kafka ─────────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str
    kafka_ssl_ca_file: str = "./certs/ca.pem"
    kafka_ssl_cert_file: str = "./certs/service.cert"
    kafka_ssl_key_file: str = "./certs/service.key"

    # ── Postgres ──────────────────────────────────────────────────────────────
    postgres_uri: str

    # ── Topics ────────────────────────────────────────────────────────────────
    topic_groups_to_scrape: str = "groups-to-scrape"
    topic_groups_raw: str = "groups-raw"
    topic_events_raw: str = "events-raw"

    # ── Scraping ──────────────────────────────────────────────────────────────
    pro_networks: list[str] = Field(default=["pydata"])
    max_events_per_group: int = 50
    request_delay_seconds: float = 1.5
    playwright_fallback: bool = True

    @classmethod
    def from_env(cls) -> "Settings":
        return cls()  # type: ignore[call-arg]

    # Convenience: build the confluent-kafka producer/consumer config dict
    def kafka_ssl_config(self) -> dict:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "security.protocol": "SSL",
            "ssl.ca.location": self.kafka_ssl_ca_file,
            "ssl.certificate.location": self.kafka_ssl_cert_file,
            "ssl.key.location": self.kafka_ssl_key_file,
        }
