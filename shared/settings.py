from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Cloudflare R2
    r2_endpoint_url: str
    r2_access_key_id: str
    r2_secret_access_key: str

    # Iceberg REST catalog (Cloudflare R2 Data Catalog)
    catalog_uri: str
    catalog_token: str

    # Scraping
    max_events_per_group: int = 50
    request_delay_seconds: float = 1.5

    @classmethod
    def from_env(cls) -> "Settings":
        return cls()
