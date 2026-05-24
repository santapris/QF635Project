"""Command-line entry points.

Production runners live at this level:

- ``run_live``: paper or live trading, driven by a TOML config.
- ``run_backtest``: replay a CSV through the same components.

Pedagogical demos live under ``runners.examples`` — see that package's
docstring. Demos hardcode instruments and strategies; nothing there is
meant to be deployed.
"""
