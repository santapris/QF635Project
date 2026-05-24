from __future__ import annotations

from typing import Generic, TypeVar

from ..core.exceptions import ConfigError

T = TypeVar("T")


class _Registry(Generic[T]):
    def __init__(self, label: str) -> None:
        self._label = label
        self._items: dict[str, T] = {}

    def register(self, name: str, plugin: T) -> None:
        if name in self._items:
            raise ConfigError(f"{self._label} {name!r} already registered")
        self._items[name] = plugin

    def get(self, name: str) -> T:
        try:
            return self._items[name]
        except KeyError:
            known = ", ".join(sorted(self._items)) or "<none>"
            raise ConfigError(
                f"unknown {self._label} type {name!r}; registered: {known}"
            ) from None

    def __contains__(self, name: str) -> bool:
        return name in self._items

    def names(self) -> list[str]:
        return sorted(self._items)


gateway_registry: _Registry = _Registry("gateway")
strategy_registry: _Registry = _Registry("strategy")
rule_registry: _Registry = _Registry("rule")
