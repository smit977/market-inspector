import argparse
import asyncio
import structlog
from integrity_engine.storage.writer import run_from_env

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float)
    args = parser.parse_args()
    structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso", utc=True), structlog.processors.JSONRenderer()])
    try:
        asyncio.run(run_from_env(args.duration))
    except Exception as error:
        structlog.get_logger().error("clickhouse_writer_failed", error_type=type(error).__name__,
                                    action="offsets retained; correct failure and restart")
        return 1
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
