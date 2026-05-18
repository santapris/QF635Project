import os

from dotenv import load_dotenv

load_dotenv()

ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")

if ENVIRONMENT == "prod":
    from trading.config.settings.prod import ProdSettings as _SettingsCls

    def load_settings():
        settings = _SettingsCls()
        _apply_logging(settings)
        return settings
else:
    from trading.config.settings.dev import DevSettings as _SettingsCls

    def load_settings():
        settings = _SettingsCls.create()
        _apply_logging(settings)
        return settings


def _apply_logging(settings) -> None:
    from trading.logging import configure_logging
    configure_logging(level=settings.log_level, json=(settings.environment == "prod"))


__all__ = ["load_settings"]
