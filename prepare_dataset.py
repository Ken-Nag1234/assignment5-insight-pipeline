"""Build the course dataset from version 2 of the public Spotify reviews ZIP.

Usage: python prepare_dataset.py /path/to/spotify-source.zip --output data
Uses only the Python standard library. Does not call a model.
"""

import argparse
import csv
import hashlib
import heapq
import io
import json
from collections import Counter
from pathlib import Path
import zipfile


SOURCE_URL = "https://www.kaggle.com/datasets/bwandowando/3-4-million-spotify-google-store-reviews"
DOWNLOAD_URL = "https://www.kaggle.com/api/v1/datasets/download/bwandowando/3-4-million-spotify-google-store-reviews?datasetVersionNumber=2"
START = "2022-05-17"
END = "2023-11-17"  # Exclusive: a fixed 18-month historical window.
FIELDS = ["review_id", "review_text", "review_rating", "review_likes", "app_version", "review_timestamp"]
GOLD_FIELDS = ["topic", "intent", "sentiment", "severity", "entities", "evidence_quote", "needs_review"]
SEED = "berkeley-fall-2026-assignment-5-v1"


def digest(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def write_csv(path, rows, fields=FIELDS):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_zip", type=Path)
    parser.add_argument("--output", type=Path, default=Path("data"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    full = args.output / "spotify_reviews_18months.csv"
    source_rows = 0
    seen = set()
    months = Counter()
    stars = Counter()
    stats = Counter()
    sample = []
    first, last = "9999", ""
    with zipfile.ZipFile(args.source_zip) as archive, archive.open("SPOTIFY_REVIEWS.csv") as raw, full.open("w", encoding="utf-8", newline="") as out:
        reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))
        expected = set(FIELDS) - {"app_version"} | {"author_app_version"}
        if not expected.issubset(reader.fieldnames or []):
            raise ValueError("Source CSV schema does not match the pinned dataset.")
        writer = csv.DictWriter(out, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        for raw_row in reader:
            source_rows += 1
            timestamp = raw_row["review_timestamp"]
            if not START <= timestamp[:10] < END:
                continue
            row = {key: raw_row["author_app_version" if key == "app_version" else key] for key in FIELDS}
            writer.writerow(row)
            stats["records"] += 1
            months[timestamp[:7]] += 1
            stars[row["review_rating"]] += 1
            first, last = min(first, timestamp), max(last, timestamp)
            record_id = row["review_id"]
            duplicate = record_id in seen
            stats["duplicate_review_ids"] += int(duplicate)
            seen.add(record_id)
            empty = not row["review_text"].strip()
            stats["empty_review_text"] += int(empty)
            stats["missing_app_version"] += int(not row["app_version"].strip())
            # Samples contain unique, nonempty, valid-rating records. The full
            # CSV retains source defects so ingestion can report them.
            if duplicate or empty or not record_id or row["review_rating"] not in {"1", "2", "3", "4", "5"}:
                continue
            rank = int.from_bytes(hashlib.sha256((SEED + ":" + record_id).encode()).digest(), "big")
            entry = (-rank, record_id, row)
            if len(sample) < 10050:
                heapq.heappush(sample, entry)
            elif rank < -sample[0][0]:
                heapq.heapreplace(sample, entry)
    if len(sample) != 10050:
        raise ValueError("Not enough eligible records for the fixed samples.")
    selected = [entry[2] for entry in sorted(sample, key=lambda x: (-x[0], x[1]))]
    gold, analysis = selected[:50], selected[50:]
    write_csv(args.output / "golden_50_to_label.csv", gold, FIELDS + GOLD_FIELDS)
    write_csv(args.output / "cost_100.csv", analysis[:100])
    write_csv(args.output / "checkpoint_500.csv", analysis[:500])
    write_csv(args.output / "analysis_10000.csv", analysis)
    data_files = [full, args.output / "cost_100.csv", args.output / "checkpoint_500.csv", args.output / "golden_50_to_label.csv", args.output / "analysis_10000.csv"]
    manifest = {
        "source": {"title": "3.4 Million Spotify Google Store Reviews", "creator": "BwandoWando", "url": SOURCE_URL,
                   "download_url": DOWNLOAD_URL, "version": 2, "publisher_license": "CC0: Public Domain",
                   "archive_sha256": digest(args.source_zip), "archive_bytes": args.source_zip.stat().st_size,
                   "records": source_rows},
        "window": {"start_inclusive": START, "end_exclusive": END, "first_review": first, "last_review": last},
        "transformations": ["Keep all source rows in the fixed date window, in source order.",
                            "Drop the source row index, author_name, and pseudo_author_id columns.",
                            "Rename author_app_version to app_version; preserve retained field values.",
                            "Write UTF-8 CSV with LF line endings; preserve quoted multiline review text."],
        "profile": dict(stats), "reviews_by_month": dict(sorted(months.items())), "reviews_by_rating": dict(sorted(stars.items())),
        "samples": {"seed": SEED, "method": "Lowest SHA-256(seed + ':' + review_id) values among unique nonempty valid-rating records.",
                    "golden": "First 50; blank labels for students. Excluded from development checkpoints. Original texts remain in the full final run; human answer labels are never model inputs.",
                    "analysis": "Next 10,000; uniform deterministic development and budget checkpoint before the required full-corpus run.",
                    "checkpoint": "First 500 of the analysis sample.",
                    "cost_pilot": "First 100 of the checkpoint sample; fixed input for the required measured cost/runtime calculator."},
        "assignment_scope": {"input_records": stats["records"], "nonempty_to_classify": stats["records"] - stats["empty_review_text"], "empty_text_quarantines": stats["empty_review_text"], "final_input": "spotify_reviews_18months.csv", "development_only": ["cost_100.csv", "checkpoint_500.csv", "analysis_10000.csv"], "cost_calculator_spec": "COST_CALCULATOR.md", "grading_contract": "GRADING_CONTRACT.md"},
        "files": {p.name: {"bytes": p.stat().st_size, "sha256": digest(p)} for p in data_files},
    }
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"profile": dict(stats), "window": manifest["window"], "files": manifest["files"]}, indent=2))


if __name__ == "__main__":
    main()
