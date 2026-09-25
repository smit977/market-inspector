"""Run the REST-plus-WebSocket local order-book reconstruction pipeline."""

from integrity_engine.orderbook.binance import main

if __name__ == "__main__":
    raise SystemExit(main())
