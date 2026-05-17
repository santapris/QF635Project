from pydantic_settings import BaseSettings as PydanticBaseSettings


class BaseSettings(PydanticBaseSettings):
    environment: str = "dev"
    market: str = "futures"
    symbol: str = "btcusdt"

    api_key: str | None = None
    api_secret: str | None = None
