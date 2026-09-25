"""Run the Coinbase raw trade ingestor inside the app container."""

from integrity_engine.ingestion.coinbase.trades import main

if __name__ == "__main__":
    raise SystemExit(main())
