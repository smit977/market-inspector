"""Inspect score perturbations and existing feature-similarity retrieval for a run."""
import argparse
import json
from pathlib import Path
from integrity_engine.replay.evaluation import integrity_sensitivity, similarity_benchmark
from integrity_engine.schemas.anomaly import MarketAnomaly
from integrity_engine.schemas.metrics import MarketMetrics
from integrity_engine.schemas.cross_venue import CrossVenueMetrics
from integrity_engine.schemas.derivatives import DerivativesMetrics


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--run-dir",required=True);a=p.parse_args()
    root=Path(a.run_dir);outputs=[json.loads(s) for s in (root/"outputs.jsonl").read_text().splitlines()]
    (root/"similarity.json").write_text(json.dumps(similarity_benchmark(outputs),indent=2))
    crosses={(o["event"]["calculated_at"],o["event"]["window"]):o["event"] for o in outputs if o["topic"]=="replay.metrics.cross_venue"}
    derivatives={}
    for o in outputs:
        if o["topic"]=="replay.metrics.derivatives" and o["event"]["perpetual_to_spot_volume_ratio"] is not None:
            d=o["event"];key=(d["calculated_at"],d["window"])
            if key not in derivatives or d["exchange"]>derivatives[key]["exchange"]:derivatives[key]=d
    reports=[]
    for o in outputs:
        if o["topic"]!="replay.anomalies":continue
        event=o["event"];key=(event["detected_at"],event["window"])
        if key not in crosses or key not in derivatives:continue
        reports.append({"anomaly_event_id":event["event_id"],"report":integrity_sensitivity(MarketAnomaly.model_validate(event),
            MarketMetrics.model_validate(event["metrics_snapshot"]),CrossVenueMetrics.model_validate(crosses[key]),DerivativesMetrics.model_validate(derivatives[key]))})
        if len(reports)>=20:break
    (root/"stability.json").write_text(json.dumps({"assessments":reports,"limit":20,"method":"small explicit perturbations of recorded inputs"},indent=2))
    print(json.dumps({"similarity":str(root/"similarity.json"),"stability_assessments":len(reports)}))


if __name__=="__main__":main()
