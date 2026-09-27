# Local operations

## Daily workflow

START:

1. Open Docker Desktop and wait for its engine to start.
2. Open Terminal.
3. `cd /Users/smitprajapati/Downloads/Folders/market-move-integrity`
4. Run `./scripts/start_stack.sh`.

CHECK:

```sh
./scripts/status_stack.sh
```

STOP:

```sh
./scripts/stop_stack.sh
```

After stop completes successfully, close Terminal and quit Docker Desktop.
The stack runs detached and does not rely on Terminal sessions. Start explicitly
selects the live services; replay and Fabric are never started by this command.
Status uses only the local Docker engine and reports container health/restarts;
worker running status alone is not proof of fresh exchange data.

Kafka UI: http://127.0.0.1:8080

Grafana: http://127.0.0.1:3000

MCP: http://127.0.0.1:8001/mcp

## Persistence and configuration

**NEVER use `docker compose down -v` unless intentionally deleting persisted data.**
The `-v` flag removes named volumes. Normal stop uses `docker compose stop` with
a 60-second graceful shutdown timeout and retains containers and volumes.
`docker compose down` without `-v` removes containers/network but preserves named
volumes; it is unnecessary for the daily workflow.

Named volumes are `kafka_data`, `clickhouse_data`, `clickhouse_logs`, and
`grafana_data`, under the stable `market-move-integrity` project name. Do not
change that name, Kafka cluster identity, or consumer run IDs when restarting.
Keep the gitignored local `.env` and `data/replay/` directory. Neither is inside
a disposable container. Credentials are never printed by the operations scripts.
Compose defaults supply non-secret settings; `.env` preserves credentials and
live consumer identities. Avoid exporting conflicting Compose settings in your
shell: ordinary Compose environment-variable precedence still applies.

Infrastructure and live workers use `unless-stopped`. Initialization jobs exit
normally and are not restarted automatically. Replay remains an explicit manual
operation documented in [replay.md](replay.md). No historical replay starts during
normal startup. The optional development `app` container is not a live worker.

Startup waits for Kafka, ClickHouse, Grafana and MCP health checks and requires
successful credential initialization. Feature windows and detector baselines are
in memory and warm up again after restart; historical analytical rows and Kafka
consumer offsets persist. Order-book reconstruction obtains a new snapshot.
Anomalies/integrity are event-triggered and need not produce new events immediately.
External exchange availability remains necessary for fresh live observations.

The scripts reuse installed Docker images. After changing dependencies or the
Dockerfile, build the affected images with `docker compose build` before starting.
This procedure validates a full container stop/start on the existing volumes;
it does not test a host reboot, Docker Desktop upgrade, or disk failure. Backups
are separate from restart persistence.
