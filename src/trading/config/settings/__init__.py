import os

from dotenv import load_dotenv

load_dotenv()

ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")

if ENVIRONMENT == "prod":
    from trading.config.settings.prod import ProdSettings as _SettingsCls

    def load_settings():
        return _SettingsCls()
else:
    from trading.config.settings.dev import DevSettings as _SettingsCls

    def load_settings():
        return _SettingsCls.create()


__all__ = ["load_settings"]
