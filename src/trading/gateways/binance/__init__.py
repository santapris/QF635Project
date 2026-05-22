"""Binance Spot adapter."""

from .config import BinanceConfig, BinanceCredentials
from .depth_book import DepthBookManager
from .errors import BinanceErrorResponse, translate_error
from .gateway import BinanceGateway
from .listen_key import ListenKeyManager
from .public_ws import BinancePublicWSConnector
from .reconciler import BalanceReconciler
from .rest_client import BinanceRESTClient
from .symbols import SymbolMapper
from .user_data import BinanceUserDataStream

__all__ = [
    "BalanceReconciler",
    "BinanceConfig",
    "BinanceCredentials",
    "BinanceErrorResponse",
    "BinanceGateway",
    "BinancePublicWSConnector",
    "BinanceRESTClient",
    "BinanceUserDataStream",
    "DepthBookManager",
    "ListenKeyManager",
    "SymbolMapper",
    "translate_error",
]
