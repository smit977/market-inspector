"""Run inside Docker; optionally pass --duration 20 for a bounded live check."""

from integrity_engine.ingestion.binance.trades import main


if __name__ == "__main__":
    raise SystemExit(main())
