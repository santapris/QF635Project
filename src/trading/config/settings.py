import os
from dataclasses import dataclass


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    market: str
    symbol: str
    testnet: bool
    api_key: str | None
    api_secret: str | None

    # Derived endpoints
    rest_base: str
    ws_public_base: str
    ws_user_base: str


def _maybe_load_from_vault() -> tuple[str | None, str | None]:
    """Best-effort load of API key/secret from ~/vault/secret_file.txt used in classes.

    File format:
      API_KEY=...
      API_SECRET=...
    """
    import pathlib

    vault_path = pathlib.Path.home() / "vault" / "secret_file.txt"
    if not vault_path.exists():
        return None, None
    api_key = None
    api_secret = None
    try:
        for line in vault_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip().upper()
            v = v.strip()
            if k == "API_KEY" and not api_key:
                api_key = v
            elif k == "API_SECRET" and not api_secret:
                api_secret = v
    except Exception:
        return None, None
    return api_key, api_secret


def _maybe_load_from_qf_repo() -> tuple[str | None, str | None]:
    """Opt-in dev helper: scan QF635 classes for demo API keys/secrets.

    This is for local validation only. Controlled by DEV_LOAD_QF_KEYS=true.
    """
    import pathlib, re

    try:
        # Find repo root by walking up until we see a 'Modules' dir
        here = pathlib.Path(__file__).resolve()
        root = here
        for _ in range(8):  # walk up a few levels
            if (root / "Modules").exists():
                break
            root = root.parent
        base = root / "Modules" / "Microstructure & QTS" / "QF635-2026a" / "classes"
        if not base.exists():
            return None, None
        api_key = None
        api_secret = None
        key_re = re.compile(r"API_KEY\s*=\s*['\"]([^'\"]+)['\"]")
        sec_re = re.compile(r"api_secret\s*=\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)
        for path in base.rglob("*.py"):
            try:
                text = path.read_text(errors="ignore")
            except Exception:
                continue
            if api_key is None:
                m = key_re.search(text)
                if m:
                    api_key = m.group(1)
            if api_secret is None:
                m = sec_re.search(text)
                if m:
                    api_secret = m.group(1)
            if api_key and api_secret:
                break
        return api_key, api_secret
    except Exception:
        return None, None


def load_settings() -> Settings:
    """Load settings from environment variables.

    For this initial scaffold we focus on Binance Futures (testnet or live).
    Environment variables:
      - BINANCE_MARKET: 'futures' (default) | 'spot' (spot not yet supported here)
      - BINANCE_SYMBOL: e.g. 'btcusdt' (lowercase for WS path consistency)
      - BINANCE_TESTNET: 'true'|'false' (default true)
      - BINANCE_API_KEY / BINANCE_API_SECRET
    """
    market = os.getenv("BINANCE_MARKET", "futures").strip().lower()
    symbol = os.getenv("BINANCE_SYMBOL", "btcusdt").strip().lower()
    testnet = _as_bool(os.getenv("BINANCE_TESTNET"), True)
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        vk, vs = _maybe_load_from_vault()
        api_key = api_key or vk
        api_secret = api_secret or vs

    # Optional dev-only loader (off by default)
    if _as_bool(os.getenv("DEV_LOAD_QF_KEYS"), False) and (not api_key or not api_secret):
        dk, ds = _maybe_load_from_qf_repo()
        api_key = api_key or dk
        api_secret = api_secret or ds

    if market != "futures":
        # Keep scope tight for MVP; spot can be added later.
        raise ValueError("Only BINANCE_MARKET=futures is supported in this scaffold")

    if testnet:
        rest_base = "https://testnet.binancefuture.com"
        ws_public_base = "wss://stream.binancefuture.com"
        ws_user_base = "wss://stream.binancefuture.com/ws"
    else:
        rest_base = "https://fapi.binance.com"
        ws_public_base = "wss://fstream.binance.com"
        ws_user_base = "wss://fstream.binance.com/ws"

    return Settings(
        market=market,
        symbol=symbol,
        testnet=testnet,
        api_key=api_key,
        api_secret=api_secret,
        rest_base=rest_base,
        ws_public_base=ws_public_base,
        ws_user_base=ws_user_base,
    )
