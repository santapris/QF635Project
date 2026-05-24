"""Binance order-gateway plugin.

Owns the Binance-specific config schema, URL defaults, credentials lookup,
and the wiring that constructs the order gateway plus its supporting
services (listen-key manager, user-data stream, balance reconciler).
"""

from __future__ import annotations

import os
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from ...core.exceptions import ConfigError
from ...plugins import gateway_registry
from .config import BinanceConfig, BinanceCredentials
from .listen_key import ListenKeyManager
from .order_gateway import BinanceOrderGateway
from .reconciler import BalanceReconciler
from .rest_client import BinanceRESTClient
from .symbols import SymbolMapper
from .user_data import BinanceUserDataStream


_URLS_TESTNET = {
    "spot_rest_base": "https://testnet.binance.vision",
    "spot_ws_base": "wss://testnet.binance.vision",
    "futures_rest_base": "https://demo-fapi.binance.com",
    "futures_ws_base": "wss://fstream.binancefuture.com",
}
_URLS_LIVE = {
    "spot_rest_base": "https://api.binance.com",
    "spot_ws_base": "wss://stream.binance.com:9443",
    "futures_rest_base": "https://fapi.binance.com",
    "futures_ws_base": "wss://fstream.binance.com",
}


class BinanceParams(BaseModel):
    """Binance gateway parameters.

    ``credentials_env`` names the env-var prefix; the builder reads
    ``{credentials_env}_API_KEY`` and ``{credentials_env}_API_SECRET``.
    URL fields default to testnet or live endpoints based on ``testnet``.
    """

    model_config = ConfigDict(extra="forbid")

    testnet: bool = True
    credentials_env: str = "BINANCE"
    reconcile_interval_seconds: float = 60.0
    mismatch_threshold: str = "0.0001"

    spot_rest_base: str | None = None
    spot_ws_base: str | None = None
    futures_rest_base: str | None = None
    futures_ws_base: str | None = None

    def resolved_urls(self) -> dict[str, str]:
        defaults = _URLS_TESTNET if self.testnet else _URLS_LIVE
        return {
            "spot_rest_base": self.spot_rest_base or defaults["spot_rest_base"],
            "spot_ws_base": self.spot_ws_base or defaults["spot_ws_base"],
            "futures_rest_base": self.futures_rest_base or defaults["futures_rest_base"],
            "futures_ws_base": self.futures_ws_base or defaults["futures_ws_base"],
        }


def _read_credentials(env_prefix: str) -> BinanceCredentials:
    key_var = f"{env_prefix}_API_KEY"
    secret_var = f"{env_prefix}_API_SECRET"
    api_key = os.environ.get(key_var)
    api_secret = os.environ.get(secret_var)
    if not api_key or not api_secret:
        raise ConfigError(
            f"missing Binance API credentials; set {key_var} and {secret_var} "
            "in the environment"
        )
    return BinanceCredentials(api_key=api_key, api_secret=api_secret)


class _BinancePlugin:
    Params = BinanceParams

    def build(self, params: BinanceParams, ctx, *, venue: str):
        urls = params.resolved_urls()
        cfg = BinanceConfig(
            spot_rest_base=urls["spot_rest_base"],
            spot_ws_base=urls["spot_ws_base"],
            futures_rest_base=urls["futures_rest_base"],
            futures_ws_base=urls["futures_ws_base"],
            reconcile_interval_seconds=params.reconcile_interval_seconds,
        )
        creds = _read_credentials(params.credentials_env)

        venue_insts = [i for i in ctx.instruments.values() if i.exchange == venue]
        if not venue_insts:
            raise ConfigError(
                f"no instruments declared for venue {venue!r}; "
                "add [[instruments]] entries with that exchange value",
                venue=venue,
            )
        symbols = SymbolMapper(venue_insts)
        rest = BinanceRESTClient(config=cfg, credentials=creds, clock=ctx.clock)

        gw = BinanceOrderGateway(
            bus=ctx.bus, clock=ctx.clock, config=cfg,
            credentials=creds, symbols=symbols, rest_client=rest,
        )
        lkm = ListenKeyManager(rest=rest, config=cfg)
        uds = BinanceUserDataStream(
            bus=ctx.bus, clock=ctx.clock, config=cfg,
            listen_key_manager=lkm, symbols=symbols,
            strategy_id_lookup=ctx.oms.strategy_id_for_client_order,
        )
        reconciler = BalanceReconciler(
            bus=ctx.bus, clock=ctx.clock, config=cfg, rest=rest,
            position_engine=ctx.position,
            tracked_instruments=venue_insts,
            mismatch_threshold=Decimal(params.mismatch_threshold),
        )
        return gw, [lkm, uds, reconciler]


def register() -> None:
    gateway_registry.register("binance", _BinancePlugin())


register()
