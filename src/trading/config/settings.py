from dotenv import load_dotenv
from pydantic_settings import BaseSettings as PydanticBaseSettings
from pydantic_settings import SettingsConfigDict

# `override=False` (the default) means existing env vars win over .env.
# Lets a deployment orchestrator's injected vars take precedence when both are set.
load_dotenv(override=False)


class Settings(PydanticBaseSettings):
    """Operational settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="",
        frozen=True,
        extra="ignore",
    )

    environment: str = "dev"
    log_level: str = "INFO"
    dashboard_port: int = 8765
    market: str = "futures"
    symbol: str = "btcusdt"


def load_settings() -> Settings:
    settings = Settings()
    _apply_logging(settings)
    return settings


def _apply_logging(settings: Settings) -> None:
    from trading.logging import configure_logging
    configure_logging(level=settings.log_level, json=(settings.environment != "dev"))


__all__ = ["Settings", "load_settings"]
