# microstructure-mm (standalone)

Minimal, standalone project to connect to Binance (testnet/live), normalize events, and publish to an in-process event bus. Extracted from a larger MQF workspace but self-contained here.

## Quick Start

- Python 3.12+
- Create venv and install
```
python -m venv .venv && source .venv/bin/activate
pip install -e .
```

- Configure credentials (choose one)
  - Env vars: `export BINANCE_API_KEY=...; export BINANCE_API_SECRET=...`
  - Vault file: `~/vault/secret_file.txt` with lines `API_KEY=...` and `API_SECRET=...`
  - .env (dev only): copy `.env.example` to `.env` and fill keys

- Test connectivity
```
python -m trading.runners.binance_connectivity_check
```

- Stream normalized events to in-process bus and log
```
python -m trading.runners.binance_feed_to_bus
```

## Notes
- No external messaging or storage required for MVP.
- Extensible with Kafka, Postgres/Timescale, Redis, and observability packages via optional extras in `pyproject.toml`.

