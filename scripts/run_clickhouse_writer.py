import argparse
import asyncio
import structlog
from integrity_engine.storage.writer import run_from_env

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float)
    args = parser.parse_args()
    structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso", utc=True), structlog.processors.JSONRenderer()])
    asyncio.run(run_from_env(args.duration))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
