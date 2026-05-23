from pydantic_settings import SettingsConfigDict

from trading.config.settings.base import BaseSettings


class DevSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="BINANCE_",
        frozen=True,
    )

    api_key: str | None = None
    api_secret: str | None = None

    spot_rest_base: str = "https://testnet.binance.vision"
    spot_ws_base: str = "wss://testnet.binance.vision"
    futures_rest_base: str = "https://demo-fapi.binance.com"
    futures_ws_base: str = "wss://fstream.binancefuture.com"
