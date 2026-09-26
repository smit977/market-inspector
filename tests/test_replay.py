import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from integrity_engine.replay.archive import read_archive, ordered_events, write_archive, archived_record
from integrity_engine.replay.clock import ReplayClock
from integrity_engine.replay.models import ReplayManifest, LabeledWindow
from integrity_engine.replay.runner import ReplayPipeline, run_replay, replay_topic
from integrity_engine.replay.synthetic import synthetic_archive
from integrity_engine.replay.storage import ReplayStore, TABLES
from integrity_engine.replay.evaluation import evaluate_detector, threshold_grid, integrity_sensitivity, similarity_benchmark, incident_report

PROV={"git_commit":"a"*40,"dirty":True,"code_hash":"b"*64}


@pytest.fixture
def dataset(tmp_path):return synthetic_archive(tmp_path)


@pytest.fixture
def replay(dataset,tmp_path):
    return run_replay(dataset,tmp_path/"runs",configuration={"thresholds":{"minimum_observations":5}},code_provenance=PROV)


def test_parquet_archive_roundtrip_payload_and_manifest(dataset):
    manifest, rows=read_archive(dataset)
    assert manifest.event_count==len(rows)==316
    assert manifest.exchange_set==["binance","bybit","coinbase","kraken","okx"]
    assert manifest.file_hashes and manifest.archive_statistics["parquet_bytes"]>0
    assert all(isinstance(r.raw_payload,bytes) for r in rows)
    original=json.loads(rows[0].raw_payload)
    assert original["schema_version"]=="1.0"
    assert manifest.first_event_timestamp is not None


def test_manifest_validation_and_tamper_detection(dataset):
    manifest,_=read_archive(dataset)
    with pytest.raises(ValueError):ReplayManifest.model_validate({**manifest.model_dump(),"end_time":manifest.start_time})
    with pytest.raises(ValueError):ReplayManifest.model_validate({**manifest.model_dump(),"file_hashes":{}})
    file=dataset.parent/manifest.source_files[0];file.write_bytes(file.read_bytes()+b"tamper")
    with pytest.raises(ValueError,match="hash"):read_archive(dataset)


def test_idempotent_archive_refuses_completed_overwrite(dataset):
    m,rows=read_archive(dataset)
    with pytest.raises(FileExistsError):write_archive(rows,dataset.parent.parent,m.replay_id,m.start_time,m.end_time,m.source_topics)


def test_ordering_partition_constraints_and_timestamp_ties(dataset):
    _,rows=read_archive(dataset);r=rows[0]
    a=r.model_copy(update={"source_topic":"market.raw.orderbook","source_partition":0,"source_offset":1,"exchange_timestamp":r.received_timestamp+timedelta(seconds=10)})
    b=a.model_copy(update={"source_offset":2,"exchange_timestamp":r.received_timestamp})
    c=r.model_copy(update={"source_topic":"market.raw.trades","source_partition":1,"source_offset":0})
    result=list(ordered_events([b,a,c]))
    assert result.index(a)<result.index(b)
    assert [r.identity for r in ordered_events([c,b,a])]==[r.identity for r in result]


@pytest.mark.parametrize("speed,expected",[("1",10),("10",1),("max",0)])
def test_clock_modes_preserve_virtual_time(speed,expected):
    start=datetime(2026,1,1,tzinfo=timezone.utc);elapsed=[0.0]
    def sleep(s):elapsed[0]+=s
    clock=ReplayClock(start,speed,sleep,lambda:elapsed[0]);clock.advance(start+timedelta(seconds=10))
    assert elapsed[0]==expected and clock.monotonic()==10
    clock.advance(start);assert clock.now()==start+timedelta(seconds=10)


def test_invalid_replay_speed():
    for speed in ("0","-1","nan","inf"):
        with pytest.raises(ValueError):ReplayClock(datetime.now(timezone.utc),speed)


def test_full_synthetic_pipeline_metadata_and_sequence(replay):
    run,outputs,directory=replay
    assert run.DLQ_count==0
    topics={o["topic"] for o in outputs}
    assert set(TABLES)|{"replay.cleaned.trades","replay.cleaned.orderbook","replay.cleaned.derivatives"}<=topics
    assert run.anomaly_count>0 and run.integrity_event_count==run.anomaly_count
    assert all(o["replay_id"]==run.replay_id and o["is_replay"] for o in outputs)
    seq=[o["event"]["sequence_id"] for o in outputs if o["topic"]=="replay.cleaned.orderbook"]
    assert seq==list(range(101,146))
    assert run.git_commit==PROV["git_commit"] and run.dirty


def test_replay_topics_cannot_route_to_live():
    assert replay_topic("market.integrity")=="replay.integrity"
    for topic in ("system.dlq","replay.integrity","market.unknown"):
        with pytest.raises(ValueError):replay_topic(topic)


def test_clickhouse_routes_only_dedicated_replay_tables(replay):
    run,outputs,directory=replay;store=MagicMock();store.database="market_integrity"
    storage=ReplayStore(store);storage.initialize();storage.write(run,outputs,json.loads((directory/"catalog.json").read_text()))
    inserted={c.args[0] for c in store.client.insert.call_args_list}
    assert inserted==set(TABLES.values())|{"replay_runs","historical_events"}
    assert not inserted & {"market_metrics","market_integrity","market_anomalies"}


def test_deterministic_outputs_and_counts_at_different_speeds(dataset,tmp_path):
    def clock(start,speed):return ReplayClock(start,speed,lambda _:None,lambda:0)
    runs=[run_replay(dataset,tmp_path/"runs",speed=s,clock_factory=clock,code_provenance=PROV) for s in ("1","10","max","max")]
    assert len({r[0].determinism_hash for r in runs})==1
    assert len({r[0].config_hash for r in runs})==1
    assert all(r[1]==runs[0][1] for r in runs)


def test_threshold_evaluation_no_invented_labels(dataset,tmp_path,replay):
    run,outputs,_=replay
    metrics=evaluate_detector(run,outputs)
    assert metrics["precision"] is None and metrics["recall"] is None
    result=threshold_grid(dataset,tmp_path/"grid",{"z_threshold":[2,4]},code_provenance=PROV)
    assert len(result["scenarios"])==2 and result["selection"] is None


def test_explicit_labeled_window_metrics(replay):
    run,outputs,_=replay
    a=next(o["event"] for o in outputs if o["topic"]=="replay.anomalies")
    at=datetime.fromisoformat(a["detected_at"])
    label=LabeledWindow(label_id="fixture",start_time=at-timedelta(seconds=1),end_time=at+timedelta(seconds=1),
        label="volume_anomaly",definition="Fixture requires one trigger in this window",evidence=["synthetic injected discontinuity"],expected_trigger=True)
    report=evaluate_detector(run,outputs,[label.model_dump()])
    assert report["recall"]==1 and report["time_to_detection_seconds"]==[1]


@pytest.mark.parametrize("name",["Cross-Venue Confirmation","Spot Participation","Liquidity Stability","Volume Quality","Derivatives Dependence","Liquidation Dependence"])
def test_component_sensitivity_is_bounded_and_reported(name):
    from tests.test_integrity import anomaly,spot,cross,deriv
    d=deriv().model_copy(update={"liquidation_quote_volume":Decimal("1"),"open_interest":Decimal("10"),"funding_rate":Decimal("0.0001")})
    report=integrity_sensitivity(anomaly(),spot(),cross(),d)
    assert all(name in p["component_score_deltas"] for p in report["perturbations"])
    assert all(abs(p["component_score_deltas"][name] or 0)<=2 for p in report["perturbations"] if p["perturbation"] not in {"missing_derivative_fields","one_fewer_active_venue"})


def test_classification_stability_and_conflicting_evidence():
    from tests.test_integrity import anomaly,spot,cross,deriv
    report=integrity_sensitivity(anomaly(),spot(),cross(),deriv())
    assert "margin_to_next_classification" in report and report["margin_explanation"]
    assert all("classification_confidence" in p and "conflicting_evidence" in p for p in report["perturbations"])


def test_incident_reconstruction_and_similarity(replay):
    run,outputs,directory=replay
    event=next(o["event"] for o in outputs if o["topic"]=="replay.integrity")
    report=incident_report(directory,event["event_id"])
    assert report["replay_id"]==run.replay_id and report["trigger"] and report["timeline"]
    assert report["cross_venue"] and report["derivatives_and_liquidations"]
    benchmark=similarity_benchmark(outputs)
    assert benchmark["reports"] and benchmark["human_alignment_judgment"] is None


def test_orderbook_gap_goes_to_isolated_dlq(dataset):
    m,rows=read_archive(dataset);clock=ReplayClock(m.start_time);p=ReplayPipeline(m.replay_id,clock)
    book=sorted([r for r in rows if r.stream=="orderbook"],key=lambda r:r.source_offset)
    p.process(book[0]);p.process(book[1]);p.process(book[3])
    assert len(p.dlq)==1 and p.quality["orderbook_sequence_gap"]==1
    assert p.book.last_update_id is None


def test_missing_checkpoint_not_silently_repaired(dataset):
    m,rows=read_archive(dataset);p=ReplayPipeline(m.replay_id,ReplayClock(m.start_time))
    row=next(r for r in rows if r.stream=="orderbook" and r.source_offset==1)
    p.process(row)
    assert p.quality["missing_historical_book_checkpoint"]==1
    assert not p.output


def test_future_funding_timestamp_does_not_jump_replay_clock(dataset):
    m,rows=read_archive(dataset);base=rows[0]
    future=base.model_copy(update={"exchange_timestamp":base.received_timestamp+timedelta(hours=8),"event_type":"funding_rate"})
    assert future.order_key[0]==future.received_timestamp
    assert future.exchange_timestamp==base.received_timestamp+timedelta(hours=8)


def test_detector_clock_cooldown_at_virtual_zero():
    from integrity_engine.detection.engine import MoveDetector
    from tests.test_integrity import anomaly
    detector=MoveDetector(None,clock=lambda:0)
    detector.process=lambda _: [anomaly().model_copy(update={"severity":"low"})]
    assert len(detector.eligible(None))==1
    assert detector.eligible(None)==[]
    assert detector.cooldown_suppressed==1


@pytest.mark.parametrize("scenario",["organic_derivatives","liquidity_mixed","divergence_mixed"])
def test_classification_boundary_flips_are_reported_not_hidden(scenario):
    from tests.test_integrity import anomaly,spot,cross,deriv
    m,c,d=spot(),cross(),deriv()
    if scenario=="organic_derivatives":d=d.model_copy(update={"perpetual_to_spot_volume_ratio":Decimal("4")})
    elif scenario=="liquidity_mixed":m=m.model_copy(update={"spread_bps":Decimal("5.5")})
    else:c=c.model_copy(update={"confirmation_ratio":Decimal("0.5"),"confirmation_state":"fragmented"})
    report=integrity_sensitivity(anomaly(),m,c,d)
    assert report["nearest_observed_classification_flips"]


@pytest.mark.skipif(__import__('os').getenv('REPLAY_CLICKHOUSE_INTEGRATION')!='1',reason='explicit replay ClickHouse integration')
def test_synthetic_pipeline_outputs_in_dedicated_clickhouse_tables(replay):
    from integrity_engine.storage.clickhouse import ClickHouseStore
    run,outputs,directory=replay;store=ClickHouseStore()
    try:
        sink=ReplayStore(store);sink.initialize();sink.write(run,outputs,json.loads((directory/'catalog.json').read_text()))
        for table in TABLES.values():
            n=store.query(f"SELECT count() FROM {table} FINAL WHERE replay_run_id = '{run.replay_run_id}' AND replay_id = 'synthetic' AND is_replay = 1").result_rows[0][0]
            assert n>0
    finally:store.close()


def test_timezone_required_for_archive_cli():
    from integrity_engine.replay.archive import stamp
    with pytest.raises(ValueError,match='timezone'):stamp('2026-01-01T00:00:00')


def test_invalid_configuration_not_silently_ignored(dataset,tmp_path):
    with pytest.raises(ValueError,match='unknown'):run_replay(dataset,tmp_path/'invalid',configuration={'typo':True},code_provenance=PROV)


def test_catalog_extrema_use_observed_window_evidence(replay):
    _,_,directory=replay
    rows=json.loads((directory/'catalog.json').read_text())
    assert rows and all(r['peak_return'] is not None for r in rows)
    assert any('first observed' in note for note in rows[0]['notes'])


def test_sensitivity_does_not_turn_missing_liquidations_into_zero():
    from tests.test_integrity import anomaly,spot,cross,deriv
    report=integrity_sensitivity(anomaly(),spot(),cross(),deriv())
    assert not any(p['perturbation'].startswith('liquidation_notional') for p in report['perturbations'])
    assert any(p['perturbation'].startswith('liquidation_notional') for p in report['skipped_unavailable_inputs'])


def test_duplicate_book_delta_does_not_emit_another_snapshot(dataset):
    m,rows=read_archive(dataset);p=ReplayPipeline(m.replay_id,ReplayClock(m.start_time))
    book=sorted([r for r in rows if r.stream=='orderbook'],key=lambda r:r.source_offset)
    p.process(book[0]);p.process(book[1]);count=len(p.output);p.process(book[1])
    assert len(p.output)==count and p.quality['stale_orderbook_delta_ignored']==1
