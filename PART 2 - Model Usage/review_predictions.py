"""Create a blind 50-message sample, then evaluate genuine human labels."""
import argparse
import csv
import hashlib
import json
import random
from pathlib import Path

CLASSES = ["World", "Sports", "Business", "Sci/Tech"]
NONE = "None of these"


def read_csv(path):
    with path.open(newline="", encoding="utf-8-sig") as handle:
        yield from csv.DictReader(handle)


def predictions(path):
    path = Path(path)
    files = sorted(path.glob("part-*.csv")) if path.is_dir() else [path]
    if not files:
        raise ValueError("No prediction part-*.csv files found")
    rows = []
    seen = set()
    for file in files:
        for row in read_csv(file):
            required = {"id", "text", "category", "confidence"}
            if not required.issubset(row):
                raise ValueError("Use predictions from the updated batch_predict.py")
            identifier = hashlib.sha256(row["text"].encode("utf-8")).hexdigest()
            if row["id"] != identifier or identifier in seen:
                raise ValueError("Prediction IDs are duplicated or do not match their text")
            if row["category"] not in CLASSES or not 0 <= float(row["confidence"]) <= 1:
                raise ValueError("Invalid predicted category or probability")
            seen.add(identifier)
            rows.append({key: row[key] for key in required})
    return rows


def write_csv(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    sample = commands.add_parser("sample", help="Prepare 50 messages without exposing predictions")
    sample.add_argument("--predictions", required=True)
    sample.add_argument("--output", required=True)
    sample.add_argument("--seed", type=int, default=4371)
    evaluate = commands.add_parser("evaluate", help="Compare the 50 labels with saved predictions")
    evaluate.add_argument("--predictions", required=True)
    evaluate.add_argument("--labels", required=True)
    evaluate.add_argument("--output", required=True)
    evaluate.add_argument("--summary", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    targets = [output] + ([Path(args.summary)] if args.command == "evaluate" else [])
    if len({p.resolve() for p in targets}) != len(targets) or any(p.exists() for p in targets):
        parser.error("Choose distinct, new output files; existing work is never overwritten")
    rows = predictions(args.predictions)
    if args.command == "sample":
        if len(rows) < 50:
            raise ValueError("Need at least 50 unique messages")
        selected = random.Random(args.seed).sample(sorted(rows, key=lambda row: row["id"]), 50)
        write_csv(output, [{"id": row["id"], "text": row["text"], "human_label": ""} for row in selected],
                  ["id", "text", "human_label"])
        print(f"Fill human_label in {output} with {', '.join(CLASSES + [NONE])}. Seed: {args.seed}")
        return
    labels = list(read_csv(Path(args.labels)))
    if len(labels) != 50:
        raise ValueError("Exactly 50 hand-labeled messages are required")
    by_id = {row["id"]: row for row in rows}
    seen, comparison = set(), []
    for row in labels:
        identifier = row.get("id")
        label = (row.get("human_label") or "").strip()
        if identifier in seen or identifier not in by_id:
            raise ValueError("Duplicate or unknown hand-label ID")
        if label not in CLASSES + [NONE]:
            raise ValueError(f"Missing or invalid human_label: {label!r}")
        prediction = by_id[identifier]
        if row.get("text") != prediction["text"]:
            raise ValueError("Sample text changed; edit only the human_label column")
        seen.add(identifier)
        comparison.append({**prediction, "human_label": label,
                           "correct": "" if label == NONE else label == prediction["category"]})
    eligible = [row for row in comparison if row["human_label"] != NONE]
    correct = sum(row["correct"] for row in eligible)
    confusion = {actual: {predicted: 0 for predicted in CLASSES} for actual in CLASSES}
    for row in eligible:
        confusion[row["human_label"]][row["category"]] += 1
    summary = {"sample_size": 50, "in_scope": len(eligible), "none_of_these": 50-len(eligible),
               "correct": correct, "accuracy": correct / len(eligible) if eligible else None,
               "confusion_matrix": confusion,
               "mistakes": [row for row in eligible if not row["correct"]]}
    write_csv(output, comparison, ["id", "text", "human_label", "category", "confidence", "correct"])
    targets[1].parent.mkdir(parents=True, exist_ok=True)
    with targets[1].open("x", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    print(f"In-scope accuracy: {summary['accuracy']}; None of these: {summary['none_of_these']}")
    print(f"Comparison: {output}; report details: {args.summary}")


if __name__ == "__main__":
    main()
