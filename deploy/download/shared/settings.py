from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Postgres (Aiven) — stores groups/venues/events.
    # Named DATABASE_URL, not POSTGRES_URI: Aiven's Console "Connect service"
    # flow always injects app-credential connections under this fixed name.
    database_url: str

    # Scraping
    max_events_per_group: int = 50
    request_delay_seconds: float = 1.5

    @classmethod
    def from_env(cls) -> "Settings":
        return cls()
