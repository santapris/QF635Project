"""
example_usage.py
=================
A COMPLETE, RUNNABLE WALKTHROUGH of the risk management package using
RANDOMLY GENERATED trade positions and RANDOMLY GENERATED market data.

Run it with:

    python example_usage.py

You do not need any real data or a broker connection. Everything here is
synthetic (made up with a random-number generator) so you can see exactly how
each piece works and what the output looks like.

The script is organised as eight numbered steps that mirror how you would use
the system in real life:

    STEP 1  Generate random historical returns (for VaR & correlation)
    STEP 2  Generate a random portfolio of open positions
    STEP 3  Start the MasterRiskManager and load everything in
    STEP 4  Run one market-data "tick" and read the live risk snapshot
    STEP 5  Decide how big a new trade should be (position sizing)
    STEP 6  Run a batch of random orders through the pre-trade checks
    STEP 7  Run stress tests and a what-if analysis
    STEP 8  Simulate a losing streak and watch the circuit breaker fire

Everything that is random is driven by a fixed SEED so you get the same
numbers every time you run it. Change the seed (or delete it) to get
different random data.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Import the pieces we need from the package.
from data_models import TradeOrder, Position
from main_risk_manager import MasterRiskManager, RiskManagerConfig
from realtime_risk_engine import StressTester


# A fixed seed makes the "random" data reproducible run-to-run.
SEED = 20240601
rng = np.random.default_rng(SEED)

# The universe of instruments we will trade in this example.
UNIVERSE = [
    ("AAPL",   "equity", 185.0),
    ("MSFT",   "equity", 410.0),
    ("GOOGL",  "equity", 175.0),
    ("NVDA",   "equity", 120.0),
    ("SPY",    "equity", 530.0),
    ("BTCUSD", "crypto", 68000.0),
    ("ETHUSD", "crypto", 3500.0),
    ("EURUSD", "fx",     1.08),
    ("ES_FUT", "futures", 5300.0),
]

INITIAL_CAPITAL = 1_000_000.0


# ════════════════════════════════════════════════════════════════════
def banner(title: str) -> None:
    """Print a nice section header so the output is easy to read."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


# ════════════════════════════════════════════════════════════════════
# STEP 1 -- Generate random historical daily returns
# ════════════════════════════════════════════════════════════════════
def generate_random_returns(days: int = 252) -> pd.DataFrame:
    """
    Build a DataFrame of fake daily returns:
        rows    = trading days (a business-day calendar)
        columns = our instruments
        values  = daily % returns (e.g. 0.01 = +1%)

    Crypto is given a bigger daily volatility than equities/fx to make the
    risk numbers realistic. We also make SPY and the equities share a common
    market factor so they are positively correlated (like the real world).
    """
    dates = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=days)

    # A shared "market" factor that all equities partly follow.
    market_factor = rng.normal(0.0004, 0.010, days)

    columns = {}
    for symbol, asset_class, _price in UNIVERSE:
        if asset_class == "crypto":
            daily = rng.normal(0.0010, 0.040, days)            # high vol
        elif asset_class == "fx":
            daily = rng.normal(0.0000, 0.005, days)            # low vol
        elif asset_class == "futures":
            daily = 0.7 * market_factor + rng.normal(0, 0.008, days)
        else:  # equity -- partly follows the market factor
            beta = rng.uniform(0.8, 1.3)
            daily = beta * market_factor + rng.normal(0, 0.008, days)
        columns[symbol] = daily

    returns = pd.DataFrame(columns, index=dates)
    benchmark = pd.Series(market_factor, index=dates, name="MARKET")
    return returns, benchmark


# ════════════════════════════════════════════════════════════════════
# STEP 2 -- Generate a random portfolio of open positions
# ════════════════════════════════════════════════════════════════════
def generate_random_positions(n: int = 5) -> list[Position]:
    """
    Pick `n` random instruments and build random open positions in them.

    For each we randomly choose:
      * long or short          (sign of the quantity)
      * a quantity sized so the position is a sensible fraction of capital
      * an entry price near the reference price
      * a current price that has drifted a little from entry (so there is
        some unrealised PnL to look at)
      * leverage (crypto/futures get more, cash equities get 1x)
    """
    chosen = rng.choice(len(UNIVERSE), size=n, replace=False)
    positions: list[Position] = []

    for i in chosen:
        symbol, asset_class, ref_price = UNIVERSE[i]

        # target this position at 5%-15% of capital
        target_notional = INITIAL_CAPITAL * rng.uniform(0.05, 0.15)
        direction = rng.choice([1, -1])           # +1 long, -1 short
        quantity = direction * (target_notional / ref_price)

        # entry within +/-2% of reference, current within +/-5% of entry
        entry_price = ref_price * (1 + rng.uniform(-0.02, 0.02))
        current_price = entry_price * (1 + rng.uniform(-0.05, 0.05))

        if asset_class == "crypto":
            leverage = rng.choice([2.0, 3.0, 5.0])
            mmr = 0.005
        elif asset_class == "futures":
            leverage = rng.choice([5.0, 10.0])
            mmr = 0.004
        else:
            leverage = 1.0
            mmr = 0.25                            # reg-T style for cash equity

        positions.append(Position(
            symbol=symbol,
            quantity=round(quantity, 4),
            avg_price=round(entry_price, 4),
            asset_class=asset_class,
            current_price=round(current_price, 4),
            leverage=leverage,
            maintenance_margin_rate=mmr,
        ))

    return positions


# ════════════════════════════════════════════════════════════════════
# STEP 6 helper -- generate a batch of random orders to test
# ════════════════════════════════════════════════════════════════════
def generate_random_orders(n: int = 8) -> list[tuple[TradeOrder, float]]:
    """
    Create `n` random orders together with a 'reference price' (the current
    market price) for the fat-finger check. A few orders are deliberately
    made silly (huge size or a price far from market) so you can see the
    pre-trade checks reject them.
    """
    orders: list[tuple[TradeOrder, float]] = []
    for k in range(n):
        symbol, asset_class, ref_price = UNIVERSE[rng.integers(len(UNIVERSE))]
        side = rng.choice(["BUY", "SELL"])

        # Most orders are a sensible size; ~1 in 4 is deliberately oversized.
        if rng.random() < 0.25:
            notional = INITIAL_CAPITAL * rng.uniform(0.3, 1.2)   # too big
        else:
            notional = INITIAL_CAPITAL * rng.uniform(0.01, 0.06)  # reasonable
        quantity = round(notional / ref_price, 4)

        # ~1 in 5 orders has a "fat finger" price far from market.
        if rng.random() < 0.2:
            order_price = ref_price * (1 + rng.choice([-1, 1]) * rng.uniform(0.05, 0.30))
        else:
            order_price = ref_price * (1 + rng.uniform(-0.01, 0.01))

        leverage = 3.0 if asset_class in ("crypto", "futures") else 1.0
        orders.append((
            TradeOrder(
                symbol=symbol, side=str(side), quantity=quantity,
                price=round(order_price, 4), asset_class=asset_class,
                strategy_id="example_strategy", leverage=leverage,
            ),
            round(ref_price, 4),   # reference (market) price
        ))
    return orders


# ════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════
def main() -> None:
    banner("ALGO TRADING RISK MANAGEMENT -- WORKED EXAMPLE (random data)")
    print(f"  Random seed     : {SEED} (change it for different data)")
    print(f"  Starting capital: ${INITIAL_CAPITAL:,.0f}")

    # ---- STEP 1 ----------------------------------------------------
    banner("STEP 1  Generate random historical returns")
    returns_df, benchmark = generate_random_returns(days=252)
    print(f"  Generated {returns_df.shape[0]} days x {returns_df.shape[1]} instruments.")
    print("  First few rows (daily % returns):")
    print((returns_df.head(3) * 100).round(2).to_string())
    print("\n  Annualised volatility by instrument:")
    ann_vol = (returns_df.std() * np.sqrt(252) * 100).round(1)
    for sym, v in ann_vol.items():
        print(f"    {sym:8s}: {v:5.1f}%")

    # ---- STEP 2 ----------------------------------------------------
    banner("STEP 2  Generate a random portfolio of open positions")
    positions = generate_random_positions(n=5)
    print(f"  Created {len(positions)} random positions:\n")
    print(f"  {'Symbol':8} {'Side':5} {'Qty':>12} {'Entry':>10} "
          f"{'Now':>10} {'Lev':>4} {'Notional':>14} {'uPnL':>10}")
    print("  " + "-" * 80)
    for p in positions:
        side = "LONG" if p.is_long else "SHORT"
        print(f"  {p.symbol:8} {side:5} {p.quantity:>12,.2f} {p.avg_price:>10,.2f} "
              f"{p.mark_price:>10,.2f} {p.leverage:>3.0f}x ${p.notional:>12,.0f} "
              f"${p.unrealised_pnl:>+9,.0f}")

    # ---- STEP 3 ----------------------------------------------------
    banner("STEP 3  Start the MasterRiskManager and load data")
    config = RiskManagerConfig(
        initial_capital=INITIAL_CAPITAL,
        log_level="ERROR",      # keep the console quiet for this walkthrough
        log_file=None,
        # make the per-trade limits a bit larger so reasonable random
        # orders are not all rejected
        risk_limits={
            "max_single_trade_notional_soft": 60_000,
            "max_single_trade_notional_hard": 120_000,
            "max_position_notional_soft": 150_000,
            "max_position_notional_hard": 250_000,
            "restricted_symbols": ["GME", "AMC"],
        },
    )
    rm = MasterRiskManager(config)
    rm.load_returns_history(returns_df, benchmark)
    rm.load_positions(positions)
    print("  MasterRiskManager is ready, returns + positions loaded.")

    # ---- STEP 4 ----------------------------------------------------
    banner("STEP 4  Run one market tick and read the risk snapshot")
    # build a dict of random 'current prices' that drift from entry
    tick_prices = {}
    for p in positions:
        tick_prices[p.symbol] = round(p.avg_price * (1 + rng.uniform(-0.03, 0.03)), 4)
    snapshot = rm.tick(tick_prices)
    rm.rt_monitor.print_snapshot(snapshot)

    # ---- STEP 5 ----------------------------------------------------
    banner("STEP 5  Position sizing for a NEW trade")
    print("  Suppose we want to add a position. How big should it be?\n")

    # ATR-based: pretend NVDA has an ATR of $4 (about 3% of its price)
    atr_units = rm.size_by_atr(atr=4.0, risk_pct=0.01, multiplier=2.0)
    print(f"  ATR sizing      : risk 1% of capital, ATR=$4, x2 stop")
    print(f"                    -> {atr_units:,.0f} units "
          f"(${atr_units * 120:,.0f} notional at $120)")

    # Volatility targeting: use NVDA's realised vol from our random data
    nvda_vol = float(returns_df["NVDA"].std() * np.sqrt(252))
    vt_units = rm.size_by_volatility_target(asset_vol=nvda_vol, price=120.0,
                                            target_vol=0.10)
    print(f"\n  Vol targeting   : target 10% annual vol, NVDA vol={nvda_vol:.0%}")
    print(f"                    -> {vt_units:,.0f} units "
          f"(${vt_units * 120:,.0f} notional)")

    # Signal scaling: a +0.6 bullish signal, three different curves
    print(f"\n  Signal scaling  : signal=+0.6, max position=1,000 units")
    for method in ("linear", "tanh", "cubic"):
        units = rm.size_by_signal(max_position=1000, signal=0.6, method=method)
        print(f"                    {method:7s}-> {units:>6,.0f} units")

    # ---- STEP 6 ----------------------------------------------------
    banner("STEP 6  Run random orders through the PRE-TRADE checks")
    orders = generate_random_orders(n=8)
    print(f"  Submitting {len(orders)} random orders. Some are deliberately")
    print("  oversized or mispriced so you can see them get blocked.\n")
    print(f"  {'Symbol':8} {'Side':5} {'Notional':>13} {'Price':>11}  Result")
    print("  " + "-" * 78)
    approved_count = 0
    for order, ref_price in orders:
        ok, reason, result = rm.approve_order(order, reference_price=ref_price)
        if ok:
            approved_count += 1
            rm.on_order_sent(order)
            verdict = "APPROVED"
        else:
            # shorten the reason for tidy printing
            verdict = "BLOCKED: " + reason.split(";")[0][:46]
        print(f"  {order.symbol:8} {order.side:5} ${order.notional:>11,.0f} "
              f"{order.price:>11,.2f}  {verdict}")
    print(f"\n  {approved_count} of {len(orders)} orders approved.")

    # ---- STEP 7 ----------------------------------------------------
    banner("STEP 7  Stress tests and what-if analysis")
    print("  How much would the portfolio lose in historical crash scenarios?\n")
    StressTester.print_report(rm.run_stress_tests())

    print("\n  WHAT-IF: what happens if we ADD a large BTC long?")
    hypothetical = [Position("BTCUSD", 5.0, 68000.0, "crypto",
                             current_price=68000.0, leverage=3.0)]
    current, projected = rm.what_if_analysis(
        hypothetical, scenario_name="Add 5 BTC",
        shocks={"equity": -0.20, "crypto": -0.40, "fx": -0.05, "futures": -0.15})
    print(f"    Current portfolio under shock : ${current.pnl_impact:>14,.0f}")
    print(f"    With the new BTC position     : ${projected.pnl_impact:>14,.0f}")
    print(f"    Extra risk from the BTC trade : "
          f"${projected.pnl_impact - current.pnl_impact:>14,.0f}")

    print("\n  SENSITIVITY: portfolio P&L if equities move from -30% to +10%:")
    sens = rm.sensitivity_report("equity")
    print(sens.to_string(index=False))

    # ---- STEP 8 ----------------------------------------------------
    banner("STEP 8  Simulate a losing streak -> circuit breaker fires")
    print("  We now feed the circuit breaker a run of losing trades and a")
    print("  worsening drawdown, and watch it move ACTIVE -> WARNING -> HALTED.\n")

    # Five random losing trades in a row
    for trade_no in range(1, 6):
        loss = -float(rng.uniform(5_000, 15_000))
        rm.on_trade_result(loss)
        state = rm.controls.circuit_breaker.state.name
        print(f"    Trade {trade_no}: P&L ${loss:>10,.0f}  ->  breaker state: {state}")

    print("\n  Now simulate the equity drawdown growing each tick:")
    for dd in (0.04, 0.07, 0.11, 0.21):
        rm.controls.update_risk_metrics(drawdown_pct=dd)
        state = rm.controls.circuit_breaker.state.name
        allowed, _ = rm.controls.check_order(10_000)
        print(f"    Drawdown {dd:>5.0%}  ->  state: {state:8}  "
              f"new orders allowed: {allowed}")

    print("\n  The desk investigates and performs a full reset:")
    rm.full_reset(by="head_of_risk")
    rm.on_market_data()
    allowed, msg = rm.controls.check_order(10_000)
    print(f"    After full_reset -> orders allowed: {allowed} ({msg})")

    # ---- Wrap up ---------------------------------------------------
    banner("DONE")
    print("  You have now seen the full workflow end to end:")
    print("   * historical returns drive VaR and correlation")
    print("   * positions are marked to market each tick")
    print("   * new trades are sized by ATR / volatility / signal")
    print("   * every order passes the pre-trade gate")
    print("   * stress tests and what-if analysis quantify tail risk")
    print("   * the circuit breaker halts trading when losses pile up")
    print("\n  Edit the SEED at the top of this file to generate a different")
    print("  random portfolio and market, then run it again.\n")


if __name__ == "__main__":
    main()
