from pathlib import Path
from pydantic_settings import SettingsConfigDict

from trading.config.settings.base import BaseSettings


def _load_from_vault() -> str | None:
    vault_path = Path.home() / "vault" / "secret_file.txt"
    if not vault_path.exists():
        return None
    try:
        for line in vault_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip().upper() == "API_KEY":
                return v.strip()
    except Exception:
        return None
    return None


def _load_secret_from_vault() -> str | None:
    vault_path = Path.home() / "vault" / "secret_file.txt"
    if not vault_path.exists():
        return None
    try:
        for line in vault_path.read_text().splitlines():
            line = line.strip()
            if not line or not line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip().upper() == "API_SECRET":
                return v.strip()
    except Exception:
        return None
    return None



class DevSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="BINANCE_",
        frozen=True,
    )

    api_key: str | None = None
    api_secret: str | None = None

    rest_base: str = "https://demo-fapi.binance.com"
    ws_base: str = "wss://fstream.binancefuture.com"

    @classmethod
    def create(cls) -> "DevSettings":
        # Resolve env vars and .env via pydantic-settings
        instance = cls()

        # Fallback chain for api_key
        if not instance.api_key:
            vault_key = _load_from_vault()
            if vault_key:
                object.__setattr__(instance, "api_key", vault_key)

        if not instance.api_secret:
            vault_secret = _load_secret_from_vault()
            if vault_secret:
                object.__setattr__(instance, "api_secret", vault_secret)

        return instance
