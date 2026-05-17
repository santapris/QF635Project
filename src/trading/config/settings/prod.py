from pydantic_settings import SettingsConfigDict

from trading.config.settings.base import BaseSettings


class ProdSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="BINANCE_",
        frozen=True,
    )

    api_key: str
    api_secret: str

    rest_base: str = "https://fapi.binance.com"
    ws_base: str = "wss://fstream.binance.com"
