"""Run explicit threshold scenarios; never auto-select a best threshold."""
import argparse
import json
from pathlib import Path
from integrity_engine.replay.evaluation import threshold_grid


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", required=True); p.add_argument("--grid", required=True)
    p.add_argument("--labels"); p.add_argument("--configuration"); p.add_argument("--output-dir", default="data/replay/evaluation")
    a = p.parse_args()
    report = threshold_grid(a.manifest, a.output_dir, json.loads(Path(a.grid).read_text()),
        json.loads(Path(a.labels).read_text()) if a.labels else None,
        json.loads(Path(a.configuration).read_text()) if a.configuration else None)
    Path(a.output_dir).mkdir(parents=True, exist_ok=True)
    (Path(a.output_dir)/"thresholds.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__": main()
