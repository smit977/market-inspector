"""Run the trade normalizer inside the app container; optionally --duration 20."""

from integrity_engine.normalization.multi import main


if __name__ == "__main__":
    raise SystemExit(main())
