"""Pedagogical example runners.

Each ``stageN_*`` script adds one architectural layer on top of the previous
one (market data → strategy → risk+OMS → order gateway). ``binance_testnet``
is a smoke test for the Binance integration.

These scripts hardcode their instrument, strategy, and risk limits. They are
for learning and debugging, not deployment. The production entry point is
``trading.runners.run_live``.
"""
