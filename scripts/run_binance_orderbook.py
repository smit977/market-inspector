"""Run raw Binance depth ingestion inside Docker, optionally with --duration 20."""

from integrity_engine.ingestion.binance.orderbook import main


if __name__ == "__main__":
    raise SystemExit(main())
