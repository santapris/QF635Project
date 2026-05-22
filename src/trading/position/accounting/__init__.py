"""Accounting methods and book factory."""

from __future__ import annotations

from enum import Enum

from .base import AccountingBook
from .lot_book import FIFOBook, LIFOBook
from .wavg import WAVGBook


class AccountingMethod(str, Enum):
    WAVG = "WAVG"
    FIFO = "FIFO"
    LIFO = "LIFO"


def make_book(method: AccountingMethod) -> AccountingBook:
    """Construct an empty book for the given method."""
    if method is AccountingMethod.WAVG:
        return WAVGBook()
    if method is AccountingMethod.FIFO:
        return FIFOBook()
    if method is AccountingMethod.LIFO:
        return LIFOBook()
    raise ValueError(f"unknown accounting method: {method}")


__all__ = [
    "AccountingBook",
    "AccountingMethod",
    "FIFOBook",
    "LIFOBook",
    "WAVGBook",
    "make_book",
]
