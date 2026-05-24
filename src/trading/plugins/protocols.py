from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from pydantic import BaseModel

if TYPE_CHECKING:
    from .context import BuildContext


class GatewayPlugin(Protocol):
    Params: type[BaseModel]

    def build(self, params: BaseModel, ctx: "BuildContext", *, venue: str) -> tuple[Any, list[Any]]:
        """Return (order_gateway, [extra_services])."""
        ...


class StrategyPlugin(Protocol):
    Params: type[BaseModel]

    def build(
        self,
        params: BaseModel,
        ctx: "BuildContext",
        *,
        strategy_id: str,
        instruments: list[Any],
    ) -> Any:
        ...


class RulePlugin(Protocol):
    Params: type[BaseModel]

    def build(self, params: BaseModel) -> Any:
        ...
