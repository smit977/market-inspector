"""Lossless, bounded Parquet archive. No Kafka commits or deletion."""
import hashlib
import heapq
import json
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq
from .models import ArchivedEvent, ReplayManifest

UTC = timezone.utc
SCHEMA = pa.schema([
    ("archive_schema_version", pa.string()), ("schema_version", pa.string()),
    ("exchange", pa.string()), ("stream", pa.string()), ("event_type", pa.string()),
    ("exchange_symbol", pa.string()), ("canonical_pair", pa.string()),
    ("exchange_timestamp", pa.timestamp("us", tz="UTC")),
    ("received_timestamp", pa.timestamp("us", tz="UTC")), ("sequence_id", pa.string()),
    ("source_topic", pa.string()), ("source_partition", pa.int32()), ("source_offset", pa.int64()),
    ("kafka_timestamp", pa.timestamp("us", tz="UTC")), ("raw_payload", pa.binary()),
    ("source_key", pa.binary()), ("quality_flags", pa.list_(pa.string()))],
    metadata={b"archive_schema_version": b"1.0"})


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def stamp(value):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None:
        raise ValueError("explicit timezone required")
    return parsed.astimezone(UTC)


def archived_record(topic, partition, offset, value, kafka_time, key=None):
    flags = []
    raw = {}
    try:
        raw = json.loads(value)
        if not isinstance(raw, dict):
            raise ValueError("not object")
    except (ValueError, UnicodeError):
        raw = {}; flags.append("invalid_json")
    received = kafka_time
    try:
        received = stamp(raw["received_timestamp"])
    except (KeyError, ValueError, TypeError, AttributeError):
        flags.append("missing_received_timestamp_using_kafka_time")
    exchange_time = None
    sequence = raw.get("sequence_id")
    pair = raw.get("canonical_asset_pair") or raw.get("canonical_pair")
    try:
        if ".cleaned." in topic:
            exchange_time = stamp(raw["exchange_timestamp"])
        elif topic.endswith("trades"):
            from integrity_engine.normalization.multi import MultiExchangeTradeNormalizer
            event = MultiExchangeTradeNormalizer().normalize(raw)
            exchange_time, sequence, pair = event.exchange_timestamp, event.sequence_id, event.canonical_asset_pair
        elif topic.endswith("orderbook"):
            payload = raw.get("payload", {})
            if "E" in payload:
                exchange_time = datetime.fromtimestamp(payload["E"] / 1000, UTC)
                sequence = payload.get("u")
            else:
                exchange_time = received
        else:
            from integrity_engine.normalization.derivatives import normalize_derivative_batch
            events = [event for _, _, event, error in normalize_derivative_batch(raw) if event is not None]
            exchange_time = min((e.exchange_timestamp for e in events), default=None)
            pair = "BTC-USD" if events else None
    except (ValueError, TypeError, KeyError, AttributeError):
        flags.append("metadata_normalization_failed")
    if exchange_time is None:
        flags.append("exchange_timestamp_unavailable")
    if raw.get("schema_version") != "1.0":
        flags.append("schema_mismatch")
    return ArchivedEvent(schema_version=str(raw.get("schema_version", "unknown")), exchange=raw.get("exchange", "unknown"),
        stream=topic.rsplit(".", 1)[-1], event_type=raw.get("event_type"),
        exchange_symbol=raw.get("exchange_symbol") or raw.get("symbol", "unknown"), canonical_pair=pair,
        exchange_timestamp=exchange_time, received_timestamp=received, sequence_id=str(sequence) if sequence is not None else None,
        source_topic=topic, source_partition=partition, source_offset=offset, kafka_timestamp=kafka_time,
        raw_payload=value, source_key=key, quality_flags=flags)


def write_archive(records, root, replay_id, start, end, topics, compression="zstd", partition_ranges=None, notes=None, chunk_size=10000):
    started = time.perf_counter()
    directory = Path(root) / replay_id
    directory.mkdir(parents=True, exist_ok=True)
    if (directory / "manifest.json").exists():
        # Never overwrite a completed archive with a different export.
        raise FileExistsError("completed replay_id already exists; verify/reuse its manifest")
    groups = defaultdict(list); files = {}; exchanges = set(); versions = set(); counts = Counter()
    event_count = raw_bytes = 0; first = last = None
    def flush():
        for group, rows in sorted(groups.items()):
            rows.sort(key=lambda r: r.identity)
            identities = "\n".join(str(r.identity) for r in rows).encode()
            rel = f"{group}/{hashlib.sha256(identities).hexdigest()[:24]}.parquet"
            path = directory / rel; path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            pq.write_table(pa.Table.from_pylist([r.model_dump() for r in rows], schema=SCHEMA), tmp, compression=compression)
            tmp.replace(path); files[rel] = sha(path)
        groups.clear()
    for record in records:
        event_count += 1; raw_bytes += len(record.raw_payload)
        exchanges.add(record.exchange); versions.add(record.schema_version); counts.update(record.quality_flags)
        t = record.exchange_timestamp or record.received_timestamp
        first = min(first, t) if first else t; last = max(last, t) if last else t
        group = "/".join(f"{k}={quote(v, safe='')}" for k, v in (
            ("exchange", record.exchange), ("stream", record.stream), ("date", record.received_timestamp.date().isoformat()), ("symbol", record.exchange_symbol)))
        groups[group].append(record)
        if event_count % chunk_size == 0: flush()
    flush()
    size = sum((directory / p).stat().st_size for p in files)
    elapsed = time.perf_counter() - started
    manifest = ReplayManifest(replay_id=replay_id, created_at=datetime.now(UTC), start_time=start, end_time=end,
        source_topics=sorted(topics), source_files=sorted(files), exchange_set=sorted(exchanges), event_count=event_count,
        first_event_timestamp=first, last_event_timestamp=last, schema_versions=sorted(versions), file_hashes=files,
        partition_ranges=partition_ranges or {}, notes=notes or [], quality_counts=dict(counts),
        archive_statistics={"parquet_bytes": size, "raw_payload_bytes": raw_bytes, "compression_ratio": raw_bytes / size if size else None,
                            "write_seconds": elapsed, "events_per_second": event_count / elapsed})
    (directory / "manifest.json").write_text(manifest.model_dump_json(indent=2))
    return directory / "manifest.json"


def read_archive(manifest_path, max_events=1000000):
    path = Path(manifest_path); manifest = ReplayManifest.model_validate_json(path.read_text())
    if manifest.event_count > max_events:
        raise ValueError("archive exceeds in-memory replay limit; split the capture")
    rows = []; seen = set()
    for rel in manifest.source_files:
        file = (path.parent / rel).resolve()
        if not file.is_relative_to(path.parent.resolve()) or sha(file) != manifest.file_hashes[rel]:
            raise ValueError("archive path or hash mismatch")
        parquet = pq.ParquetFile(file)
        if parquet.schema_arrow != SCHEMA:
            raise ValueError("Parquet schema mismatch")
        for batch in parquet.iter_batches(batch_size=10000):
            for raw in batch.to_pylist():
                row = ArchivedEvent.model_validate(raw)
                if row.identity in seen: raise ValueError("duplicate source offset")
                seen.add(row.identity); rows.append(row)
    if len(rows) != manifest.event_count: raise ValueError("manifest count mismatch")
    return manifest, rows


def ordered_events(records):
    """Stable head merge: partition offsets are hard constraints, timestamps rank eligible heads."""
    partitions = defaultdict(list)
    for row in records: partitions[row.identity[:2]].append(row)
    heap = []
    for key, values in sorted(partitions.items()):
        values.sort(key=lambda r: r.source_offset)
        heapq.heappush(heap, (values[0].order_key, key, 0))
    while heap:
        _, key, i = heapq.heappop(heap)
        values = partitions[key]; yield values[i]
        if i + 1 < len(values): heapq.heappush(heap, (values[i+1].order_key, key, i+1))


def export_kafka(bootstrap, root, replay_id, start, end, topics, compression="zstd", max_events=1000000):
    from confluent_kafka import Consumer, TopicPartition, KafkaError
    from .models import TOPICS
    if not set(topics) <= TOPICS or start >= end or end > datetime.now(UTC):
        raise ValueError("use supported topics and a closed historical time range")
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"archive-readonly-{replay_id}",
                  "enable.auto.commit": False, "enable.partition.eof": True, "auto.offset.reset": "error"})
    ranges = {}; assignments = []; ends = {}
    try:
        for topic in sorted(topics):
            meta = c.list_topics(topic, timeout=10).topics[topic]
            if meta.error: raise RuntimeError(str(meta.error))
            for partition in sorted(meta.partitions):
                tp = TopicPartition(topic, partition)
                low, high = c.get_watermark_offsets(tp, timeout=10)
                found = c.offsets_for_times([TopicPartition(topic, partition, int(start.timestamp()*1000))], timeout=10)[0]
                begin = high if found.offset < 0 else max(low, found.offset)
                finish = c.offsets_for_times([TopicPartition(topic, partition, int(end.timestamp()*1000))], timeout=10)[0].offset
                finish = high if finish < 0 else min(high, finish)
                ranges[f"{topic}:{partition}"] = {"low_watermark": low, "start_offset": begin, "end_offset_exclusive": finish,
                                                   "coverage_before_start_unverified": True}
                if begin < finish:
                    assignments.append(TopicPartition(topic, partition, begin)); ends[(topic, partition)] = finish
        c.assign(assignments)
        def records():
            count = 0; idle = time.monotonic()
            while ends:
                msg = c.poll(1)
                if msg is None:
                    if time.monotonic()-idle > 30: raise TimeoutError("archive stalled before captured end offsets")
                    continue
                key = (msg.topic(), msg.partition())
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF and key in ends and msg.offset() >= ends[key]:
                        ends.pop(key); continue
                    raise RuntimeError(str(msg.error()))
                idle = time.monotonic()
                if key not in ends: continue
                finish = ends[key]
                if msg.offset() >= finish:
                    ends.pop(key); c.pause([TopicPartition(*key)]); continue
                kafka_time = datetime.fromtimestamp(msg.timestamp()[1]/1000, UTC)
                if start <= kafka_time < end:
                    count += 1
                    if count > max_events: raise ValueError("capture max_events exceeded")
                    yield archived_record(*key, msg.offset(), msg.value(), kafka_time, msg.key())
                if msg.offset()+1 >= finish:
                    ends.pop(key); c.pause([TopicPartition(*key)])
        return write_archive(records(), root, replay_id, start, end, topics, compression, ranges,
            ["Selection uses Kafka record timestamps [start,end); payload times remain unchanged.",
             "Kafka retention before start is not inferred; offsets and watermarks are recorded."])
    finally: c.close()
