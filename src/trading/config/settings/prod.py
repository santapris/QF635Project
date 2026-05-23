from pydantic_settings import SettingsConfigDict

from trading.config.settings.base import BaseSettings


class ProdSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="BINANCE_",
        frozen=True,
    )

    api_key: str
    api_secret: str

    spot_rest_base: str = "https://api.binance.com"
    spot_ws_base: str = "wss://stream.binance.com:9443"
    futures_rest_base: str = "https://fapi.binance.com"
    futures_ws_base: str = "wss://fstream.binance.com"
