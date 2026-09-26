"""In-process isolated pipeline: shared engines, fixed virtual ticks, no live producer."""
import hashlib
import json
import os
import subprocess
import time
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

from integrity_engine.detection.detectors import DetectionConfig
from integrity_engine.detection.engine import MoveDetector
from integrity_engine.features.engine import FeatureEngine
from integrity_engine.features.cross_venue import CrossVenueFeatureEngine, WINDOWS as CROSS_WINDOWS
from integrity_engine.features.derivatives import DerivativesFeatureEngine, WINDOWS
from integrity_engine.integrity.engine import IntegrityEngine
from integrity_engine.normalization.multi import MultiExchangeTradeNormalizer
from integrity_engine.normalization.derivatives import normalize_derivative_batch
from integrity_engine.orderbook.binance import LocalOrderBook, parse_depth_event, build_snapshot, SequenceGap
from integrity_engine.schemas.trade import TradeEvent
from integrity_engine.schemas.orderbook import OrderBookSnapshot
from integrity_engine.schemas.derivatives import PerpetualTrade, FundingRate, OpenInterest, LiquidationEvent
from .archive import read_archive, ordered_events, sha
from .clock import ReplayClock
from .models import ReplayRun


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def replay_topic(topic):
    if not topic.startswith("market."):
        raise ValueError("expected known market topic")
    result = "replay." + topic[len("market."):]
    if result not in {"replay." + x for x in ("raw.trades", "raw.orderbook", "raw.derivatives", "cleaned.trades", "cleaned.orderbook", "cleaned.derivatives", "metrics", "metrics.cross_venue", "metrics.derivatives", "anomalies", "integrity")}:
        raise ValueError("unsupported replay topic")
    return result


def provenance():
    def git(*args):
        return subprocess.check_output(["git", "-c", "safe.directory=*", *args], text=True).strip()
    commit = git("rev-parse", "HEAD")
    dirty = bool(git("status", "--porcelain"))
    h = hashlib.sha256()
    for folder in ("src", "scripts"):
        for p in sorted(Path(folder).rglob("*.py")):
            h.update(str(p).encode()); h.update(p.read_bytes())
    for name in ("pyproject.toml", "Dockerfile"):
        h.update(Path(name).read_bytes())
    return {"git_commit": commit, "dirty": dirty, "code_hash": h.hexdigest()}


class ReplayPipeline:
    def __init__(self, replay_id, clock, config=None, book_source="raw", tick_seconds=1.0,
                 cross_minimum=3, flat_threshold="0.00005", evidence_max_age=5, derivative_max_age=5):
        from decimal import Decimal
        self.replay_id, self.clock, self.book_source = replay_id, clock, book_source
        self.config = config or DetectionConfig()
        self.spot = FeatureEngine(None)
        self.cross = CrossVenueFeatureEngine(minimum_active_venues=cross_minimum, flat_return_threshold=Decimal(flat_threshold))
        self.derivatives = {venue: DerivativesFeatureEngine() for venue in ("bybit", "okx", "binance")}
        for engine in self.derivatives.values(): engine.max_age = derivative_max_age
        self.detector = MoveDetector(None, self.config, clock=clock.monotonic)
        self.integrity = IntegrityEngine(None, max_age_seconds=evidence_max_age, clock=clock)
        self.normalizer = MultiExchangeTradeNormalizer(); self.book = LocalOrderBook()
        self.output = []; self.dlq = []; self.counts = Counter(); self.quality = Counter()
        self.tick_seconds = tick_seconds; self.last_times = {}; self.last_offsets = {}
        self.next_tick = clock.now() + timedelta(seconds=tick_seconds)

    def emit(self, topic, event):
        topic = replay_topic(topic)
        data = event.model_dump(mode="json")
        # All calculated event IDs are deterministic; references use these IDs downstream.
        data.pop("event_id", None)
        identity = uuid5(NAMESPACE_URL, self.replay_id + ":" + topic + ":" + digest(data))
        event = event.model_copy(update={"event_id": identity})
        data = event.model_dump(mode="json")
        original_time = data.get("exchange_timestamp") or data.get("detected_at") or data.get("calculated_at")
        self.output.append({"replay_id": self.replay_id, "is_replay": True, "topic": topic,
                            "original_event_time": original_time, "event": data})
        self.counts[topic] += 1
        self.quality.update(data.get("data_quality_flags", []))
        return event

    def error(self, row, error):
        self.dlq.append({"replay_id": self.replay_id, "is_replay": True, "source": row.identity,
                         "error": str(error), "raw_payload_hex": row.raw_payload.hex()})

    def process(self, row):
        self.quality.update(row.quality_flags)
        prior = self.last_offsets.get(row.identity[:2])
        if prior is not None and row.source_offset != prior+1: self.quality["noncontiguous_selected_source_offsets"] += 1
        self.last_offsets[row.identity[:2]] = row.source_offset
        stamp = row.exchange_timestamp or row.received_timestamp
        if stamp > row.received_timestamp:
            self.quality["future_exchange_timestamp_scheduled_at_receipt"] += 1
        key = (row.exchange, row.stream)
        if key in self.last_times and stamp < self.last_times[key]: self.quality["timestamp_disorder"] += 1
        self.last_times[key] = max(stamp, self.last_times.get(key, stamp))
        try:
            raw = json.loads(row.raw_payload)
            if raw.get("schema_version") != "1.0": raise ValueError("schema_mismatch")
            if row.stream == "trades":
                trade = TradeEvent.model_validate(raw) if ".cleaned." in row.source_topic else self.normalizer.normalize(raw)
                trade = self.emit("market.cleaned.trades", trade)
                self.cross.add_trade(trade)
                if trade.exchange == "binance": self.spot.add_trade(trade)
                for engine in self.derivatives.values(): engine.add_spot(trade)
            elif row.stream == "derivatives":
                if ".cleaned." in row.source_topic:
                    kinds = {"perpetual_trade": PerpetualTrade, "funding_rate": FundingRate, "open_interest": OpenInterest, "liquidation": LiquidationEvent}
                    events = [(0, raw, kinds[raw["event_type"]].model_validate(raw), None)]
                else: events = normalize_derivative_batch(raw)
                for _, _, event, error in events:
                    if error: self.error(row, error); continue
                    event = self.emit("market.cleaned.derivatives", event)
                    self.derivatives[event.exchange].add(event)
            elif row.stream == "orderbook":
                if self.book_source == "curated":
                    if ".raw." in row.source_topic:
                        self.counts["raw_book_preserved_curated_selected"] += 1; return
                    snapshot = OrderBookSnapshot.model_validate(raw)
                else:
                    if ".cleaned." in row.source_topic: return
                    if "lastUpdateId" in raw.get("payload", {}):
                        self.book.initialize(raw["payload"]); self.counts["archived_book_checkpoint"] += 1; return
                    event = parse_depth_event(raw)
                    if self.book.last_update_id is None:
                        self.quality["missing_historical_book_checkpoint"] += 1; return
                    if event.final_update_id <= self.book.last_update_id:
                        self.quality["stale_orderbook_delta_ignored"] += 1
                        return
                    if not self.book.synchronized:
                        if not self.book.bridge(event):
                            if event.final_update_id > self.book.last_update_id: raise SequenceGap(self.book.last_update_id, event)
                            return
                    else: self.book.apply(event)
                    snapshot = build_snapshot(self.book, event, row.received_timestamp, 20)
                self.spot.add_book(self.emit("market.cleaned.orderbook", snapshot))
        except (ValueError, TypeError, KeyError, AttributeError, SequenceGap) as error:
            if isinstance(error, SequenceGap):
                self.quality["orderbook_sequence_gap"] += 1
                self.book = LocalOrderBook()  # Await an archived checkpoint; never fetch today's REST book.
            self.error(row, error)

    def tick(self):
        now = self.clock.now()
        metrics = [self.emit("market.metrics", m) for m in self.spot.calculate(now)]
        for m in metrics: self.integrity.metrics[(m.exchange, m.symbol, m.window)] = m
        for window in CROSS_WINDOWS:
            cross = self.emit("market.metrics.cross_venue", self.cross.calculate(window, now))
            self.integrity.cross[window] = cross
            for engine in self.derivatives.values(): engine.add_spot_metric(cross.model_dump(mode="python"))
        for venue, engine in sorted(self.derivatives.items()):
            if engine.exchange is None: continue
            for window in WINDOWS:
                d = self.emit("market.metrics.derivatives", engine.calculate(window, now))
                self.integrity.derivatives[(venue, window)] = d
        for metric in metrics:
            for anomaly in self.detector.eligible(metric):
                anomaly = self.emit("market.anomalies", anomaly)
                self.emit("market.integrity", self.integrity.process(anomaly))


def run_replay(manifest_path, output_dir, speed="max", configuration=None, start=None, end=None,
               exchanges=None, streams=None, replay_run_id=None, clock_factory=ReplayClock, code_provenance=None):
    started = datetime.now(timezone.utc); wall = time.perf_counter()
    manifest, rows = read_archive(manifest_path)
    configuration = dict(configuration or {})
    allowed = {"thresholds", "tick_seconds", "book_source", "cross_minimum", "flat_threshold", "evidence_max_age", "derivative_max_age"}
    if not set(configuration) <= allowed:
        raise ValueError("unknown replay configuration fields")
    start, end = start or manifest.start_time, end or manifest.end_time
    if start < manifest.start_time or end > manifest.end_time or start >= end: raise ValueError("override outside manifest range")
    tick = float(configuration.get("tick_seconds", 1))
    if not 0.1 <= tick <= 60: raise ValueError("tick_seconds must be 0.1..60")
    detector_config = DetectionConfig(**configuration.get("thresholds", {}))
    import math
    if any(value is not None and (not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0)
           for value in asdict(detector_config).values()):
        raise ValueError("thresholds must be finite nonnegative numbers")
    if detector_config.minimum_observations < 2 or detector_config.confirmation_count < 1 or detector_config.baseline_minutes < 1 or detector_config.cooldown_seconds < 0:
        raise ValueError("invalid detector thresholds")
    effective = {"processing_mode": "replay", "thresholds": asdict(detector_config), "tick_seconds": tick,
                 "book_source": configuration.get("book_source", "curated" if "market.cleaned.orderbook" in manifest.source_topics else "raw"),
                 "cross_minimum": configuration.get("cross_minimum", 3), "flat_threshold": configuration.get("flat_threshold", "0.00005"),
                 "evidence_max_age": configuration.get("evidence_max_age", 5), "derivative_max_age": configuration.get("derivative_max_age", 5),
                 "start": start.isoformat(), "end": end.isoformat(), "exchanges": sorted(exchanges or manifest.exchange_set),
                 "streams": sorted(streams or ["trades", "orderbook", "derivatives"])}
    if effective["book_source"] not in {"raw", "curated"}: raise ValueError("book_source must be raw or curated")
    prov = code_provenance or provenance()
    run = ReplayRun(replay_run_id=replay_run_id or str(uuid4()), replay_id=manifest.replay_id, **prov,
                    started_at=started, speed=str(speed), configuration=effective, config_hash=digest(effective),
                    manifest_hash=sha(manifest_path), schema_versions=manifest.schema_versions)
    directory = Path(output_dir) / run.replay_run_id
    directory.mkdir(parents=True, exist_ok=False)
    clock = clock_factory(start, speed)
    pipeline = ReplayPipeline(manifest.replay_id, clock, detector_config, effective["book_source"], tick,
                              effective["cross_minimum"], effective["flat_threshold"], effective["evidence_max_age"], effective["derivative_max_age"])
    selected = [r for r in rows if start <= r.kafka_timestamp < end and r.exchange in effective["exchanges"] and r.stream in effective["streams"]]
    # Never double count raw and cleaned copies of the same stream.
    raw_streams = {(r.exchange, r.stream) for r in selected if ".raw." in r.source_topic and r.stream != "orderbook"}
    selected = [r for r in selected if not (".cleaned." in r.source_topic and (r.exchange, r.stream) in raw_streams)]
    for row in ordered_events(selected):
        arrival = row.received_timestamp
        # Fixed ticks happen before future arrivals; no future evidence is visible to earlier ticks.
        while pipeline.next_tick < arrival and pipeline.next_tick <= end:
            clock.advance(pipeline.next_tick); pipeline.tick(); pipeline.next_tick += timedelta(seconds=tick)
        clock.advance(min(end, arrival)); pipeline.process(row); run.event_count += 1
    while pipeline.next_tick <= end:
        clock.advance(pipeline.next_tick); pipeline.tick(); pipeline.next_tick += timedelta(seconds=tick)
    clock.advance(end)
    run.anomaly_count = pipeline.counts["replay.anomalies"]; run.integrity_event_count = pipeline.counts["replay.integrity"]
    run.error_count = run.DLQ_count = len(pipeline.dlq)
    run.determinism_hash = digest({"outputs": pipeline.output, "dlq": pipeline.dlq, "quality": dict(pipeline.quality)})
    elapsed = time.perf_counter()-wall; virtual = (end-start).total_seconds()
    run.finished_at = datetime.now(timezone.utc)
    run.statistics = {"events_published": len(pipeline.output), "events_skipped": manifest.event_count-run.event_count,
        "output_counts": dict(pipeline.counts), "quality_counts": dict(pipeline.quality), "wall_seconds": elapsed,
        "utc_elapsed_seconds": (run.finished_at-started).total_seconds(),
        "elapsed_clock": "monotonic; compare utc_elapsed_seconds for host suspension or wall-clock jumps",
        "virtual_seconds": virtual, "effective_speed": virtual/elapsed, "events_per_second": run.event_count/elapsed,
        "quality_suppressed": pipeline.detector.quality_suppressed, "cooldown_suppressed": pipeline.detector.cooldown_suppressed,
        "metrics_evaluated": pipeline.detector.metrics_consumed,
        "kafka_lag": "not_applicable_in_process_replay_no_kafka_writes"}
    with (directory / "outputs.jsonl").open("w") as file:
        for output in pipeline.output: file.write(canonical(output)+"\n")
    (directory / "dlq.json").write_text(canonical(pipeline.dlq))
    (directory / "run.json").write_text(run.model_dump_json(indent=2))
    from .evaluation import catalog, evaluate_detector
    entries = catalog(run, pipeline.output)
    (directory / "catalog.json").write_text(canonical([e.model_dump(mode="json") for e in entries]))
    (directory / "evaluation.json").write_text(canonical(evaluate_detector(run, pipeline.output)))
    return run, pipeline.output, directory
