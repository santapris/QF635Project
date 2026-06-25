"""Unit tests for analytics.realtime_risk."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from trading.analytics.realtime_risk import (
    HISTORICAL_SCENARIOS,
    MarketRiskEngine,
    MarketRiskMetrics,
    StressPosition,
    StressTester,
    StressTestResult,
)


# ════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════

def _returns_df(symbol: str = "BTC", n: int = 260, seed: int = 42) -> pd.DataFrame:
    """Deterministic daily returns series for one symbol."""
    rng = np.random.default_rng(seed)
    returns = rng.normal(0.001, 0.03, size=n)
    return pd.DataFrame({symbol: returns})


def _long_btc(notional: float = 50_000) -> StressPosition:
    return StressPosition(symbol="BTC-USDT", asset_class="crypto", notional=notional, is_long=True)


def _short_btc(notional: float = 50_000) -> StressPosition:
    return StressPosition(symbol="BTC-USDT", asset_class="crypto", notional=notional, is_long=False)


# ════════════════════════════════════════════════════════════════════
# MarketRiskEngine
# ════════════════════════════════════════════════════════════════════

class TestMarketRiskEngine:
    def test_historical_var_positive(self):
        engine = MarketRiskEngine()
        engine.load_returns(_returns_df("BTC"))
        var = engine.historical_var("BTC", 10_000)
        assert var > 0

    def test_historical_var_unknown_symbol(self):
        engine = MarketRiskEngine()
        engine.load_returns(_returns_df("BTC"))
        assert engine.historical_var("ETH", 10_000) == 0.0

    def test_historical_var_99_greater_than_95(self):
        engine = MarketRiskEngine()
        engine.load_returns(_returns_df("BTC", seed=7))
        var_95 = engine.historical_var("BTC", 10_000, 0.95)
        var_99 = engine.historical_var("BTC", 10_000, 0.99)
        assert var_99 >= var_95

    def test_historical_cvar_exceeds_var(self):
        engine = MarketRiskEngine()
        engine.load_returns(_returns_df("BTC", seed=7))
        var = engine.historical_var("BTC", 10_000)
        cvar = engine.historical_cvar("BTC", 10_000)
        assert cvar >= var

    def test_annualised_vol_positive(self):
        engine = MarketRiskEngine()
        engine.load_returns(_returns_df("BTC"))
        assert engine.annualised_volatility("BTC") > 0

    def test_portfolio_var_matrix_single_asset(self):
        """With a single asset, portfolio VaR ≈ historical VaR (within rounding)."""
        engine = MarketRiskEngine()
        engine.load_returns(_returns_df("BTC", seed=1))
        port_var, _ = engine.portfolio_var_matrix({"BTC": 10_000})
        assert port_var > 0

    def test_portfolio_var_matrix_empty_holdings(self):
        engine = MarketRiskEngine()
        engine.load_returns(_returns_df("BTC"))
        var, std = engine.portfolio_var_matrix({})
        assert var == 0.0 and std == 0.0

    def test_portfolio_var_matrix_unknown_symbols(self):
        engine = MarketRiskEngine()
        engine.load_returns(_returns_df("BTC"))
        var, std = engine.portfolio_var_matrix({"ETH": 10_000})
        assert var == 0.0

    def test_symbol_metrics_fields(self):
        engine = MarketRiskEngine()
        engine.load_returns(_returns_df("BTC"))
        m = engine.symbol_metrics("BTC", 10_000)
        assert isinstance(m, MarketRiskMetrics)
        assert m.symbol == "BTC"
        assert m.var_95 > 0
        assert m.var_99 >= m.var_95
        assert m.cvar_95 >= m.var_95
        assert m.annualised_vol > 0

    def test_portfolio_cvar_exceeds_var(self):
        engine = MarketRiskEngine()
        engine.load_returns(_returns_df("BTC", seed=3))
        port_var, _ = engine.portfolio_var_matrix({"BTC": 10_000})
        port_cvar = engine.portfolio_cvar({"BTC": 10_000})
        assert port_cvar >= port_var


# ════════════════════════════════════════════════════════════════════
# StressTester
# ════════════════════════════════════════════════════════════════════

class TestStressTester:
    def test_crypto_long_loses_in_crypto_winter(self):
        tester = StressTester(portfolio_value=100_000)
        positions = [_long_btc(50_000)]
        results = tester.run_all_historical(positions)
        crypto_winter = next(r for r in results if "Crypto Winter" in r.scenario_name)
        # crypto shock is -0.80, long position → loss
        assert crypto_winter.pnl_impact < 0

    def test_crypto_short_profits_in_crypto_winter(self):
        tester = StressTester(portfolio_value=100_000)
        positions = [_short_btc(50_000)]
        results = tester.run_all_historical(positions)
        crypto_winter = next(r for r in results if "Crypto Winter" in r.scenario_name)
        assert crypto_winter.pnl_impact > 0

    def test_all_historical_scenarios_run(self):
        tester = StressTester(portfolio_value=100_000)
        results = tester.run_all_historical([_long_btc()])
        assert len(results) == len(HISTORICAL_SCENARIOS)

    def test_results_sorted_worst_first(self):
        tester = StressTester(portfolio_value=100_000)
        results = tester.run_all_historical([_long_btc()])
        impacts = [r.pnl_impact for r in results]
        assert impacts == sorted(impacts)

    def test_custom_scenario(self):
        tester = StressTester(portfolio_value=100_000)
        tester.add_custom_scenario("Test Crash", {"crypto": -0.50})
        results = tester.run_custom([_long_btc(50_000)])
        assert len(results) == 1
        assert results[0].pnl_impact == pytest.approx(-25_000.0, abs=1.0)

    def test_what_if_projected_worse_for_long(self):
        tester = StressTester(portfolio_value=100_000)
        current_pos = [_long_btc(50_000)]
        new_pos = [_long_btc(20_000)]
        shocks = {"crypto": -0.30}
        current, projected = tester.what_if(current_pos, new_pos, shocks=shocks)
        assert projected.pnl_impact < current.pnl_impact

    def test_pnl_pct_computed_from_portfolio_value(self):
        tester = StressTester(portfolio_value=100_000)
        tester.add_custom_scenario("Flat", {"crypto": -0.10})
        result = tester.run_custom([_long_btc(50_000)])[0]
        expected_pct = result.pnl_impact / 100_000 * 100
        assert result.pnl_pct == pytest.approx(expected_pct, abs=0.01)

    def test_positions_affected_count(self):
        tester = StressTester(portfolio_value=100_000)
        tester.add_custom_scenario("Equity shock only", {"equity": -0.20})
        positions = [
            StressPosition("AAPL", "equity", 30_000, True),
            StressPosition("BTC-USDT", "crypto", 20_000, True),  # not affected
        ]
        result = tester.run_custom(positions)[0]
        assert result.positions_affected == 1
