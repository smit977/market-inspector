# Microsoft Fabric Real-Time Intelligence setup

This integration exports only curated Kafka topics. Raw trades and raw order-book data remain local.

1. Create a Fabric workspace, Eventhouse, and KQL Database.
2. Create an Eventstream and add a Custom Endpoint source.
3. Select the Kafka-compatible protocol and copy its bootstrap server, username, password, security protocol, and SASL mechanism.
4. Put those values in a local `.env` file using the `FABRIC_*` variables. Keep `FABRIC_ENABLED=false` until the endpoint is ready; never commit the password.
5. Set destination topic names for `market.metrics`, `market.metrics.cross_venue`, `market.metrics.derivatives`, `market.anomalies`, and `market.integrity`.
6. Start the optional exporter with `docker compose --profile fabric up fabric-exporter`.
7. Add an Eventhouse destination in the Eventstream and map each destination to the matching KQL table.
8. Validate that envelopes arrive with `source_topic`, offsets, event IDs, timestamps, and the original payload.

The exporter commits a local Kafka offset only after Fabric acknowledges the send. Failed records are retried and then written to `system.dlq`.
