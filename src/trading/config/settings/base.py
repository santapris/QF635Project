from pydantic_settings import BaseSettings as PydanticBaseSettings


class BaseSettings(PydanticBaseSettings):
    environment: str = "dev"
    market: str = "futures"
    symbol: str = "btcusdt"

    api_key: str | None = None
    api_secret: str | None = None

    log_level: str = "INFO"

    dashboard_port: int = 8765

    spot_rest_base: str
    spot_ws_base: str
    futures_rest_base: str
    futures_ws_base: str
