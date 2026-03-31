import os
import tempfile
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Kafka
    kafka_bootstrap_servers: str
    kafka_ssl_ca_file: str = "./certs/ca.pem"
    kafka_ssl_cert_file: str = "./certs/service.cert"
    kafka_ssl_key_file: str = "./certs/service.key"

    # Inline cert contents (used on Fly.io where no filesystem mount exists)
    kafka_ssl_ca: str = ""
    kafka_ssl_cert: str = ""
    kafka_ssl_key: str = ""

    # ... rest of fields unchanged ...

    def kafka_ssl_config(self) -> dict:
        ca = self._resolve_cert(self.kafka_ssl_ca, self.kafka_ssl_ca_file)
        cert = self._resolve_cert(self.kafka_ssl_cert, self.kafka_ssl_cert_file)
        key = self._resolve_cert(self.kafka_ssl_key, self.kafka_ssl_key_file)
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "security.protocol": "SSL",
            "ssl.ca.location": ca,
            "ssl.certificate.location": cert,
            "ssl.key.location": key,
        }

    def _resolve_cert(self, inline: str, filepath: str) -> str:
        if os.path.exists(filepath):
            return filepath
        if inline:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pem", mode="w")
            tmp.write(inline)
            tmp.close()
            return tmp.name
        raise ValueError(f"No cert available: {filepath} not found and no inline value set")