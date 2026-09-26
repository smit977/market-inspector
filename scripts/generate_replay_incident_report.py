"""Reconstruct an archived replay event as inspectable JSON and Markdown."""
import argparse
import json
from pathlib import Path
from integrity_engine.replay.evaluation import incident_report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--replay-id", required=True); p.add_argument("--run-dir", required=True)
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--integrity-event-id"); group.add_argument("--anomaly-event-id")
    p.add_argument("--output", default="incident")
    a = p.parse_args(); report = incident_report(a.run_dir, a.integrity_event_id or a.anomaly_event_id)
    if report["replay_id"] != a.replay_id: p.error("replay_id does not match run")
    output = Path(a.output); output.parent.mkdir(parents=True, exist_ok=True)
    output.with_suffix(".json").write_text(json.dumps(report, indent=2))
    body = [f"# Historical replay incident: {a.replay_id}", report["interpretation"]]
    for name in ("trigger", "integrity_assessment", "price_volume_orderbook", "cross_venue", "derivatives_and_liquidations", "data_quality_issues", "source_availability", "key_evidence", "timeline"):
        body.extend([f"## {name.replace('_', ' ').title()}", "```json\n"+json.dumps(report[name], indent=2)+"\n```"])
    output.with_suffix(".md").write_text("\n\n".join(body))
    print(output.with_suffix(".json"))


if __name__ == "__main__": main()
