"""Deterministic Assignment 5 audit. Standard library only; no network/model calls.

This checks submitted evidence, not authorship or whether API calls really happened.
Run with --help. Instructor labels and student reports must stay outside the website.
"""

import argparse
import csv
import hashlib
import gzip
import json
import math
import sys
from collections import Counter, defaultdict
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

VERSION = "a5-audit-v1"
FIELDS = ("review_id", "review_text", "review_rating", "review_likes", "app_version", "review_timestamp")
TOPICS = ("access", "usability", "playback", "downloads", "catalog", "billing", "support", "other")
INTENTS = ("complaint", "request", "praise", "cancellation", "unclear")


def canonical(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def row_sha(row):
    # Strings are preserved exactly; no trimming, translating, or Unicode normalization.
    return hashlib.sha256(canonical([row[k] for k in FIELDS]).encode("utf-8")).hexdigest()


def reject_constant(value):
    raise ValueError("Non-finite JSON number: " + value)


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key: " + key)
        result[sys.intern(key)] = value
    return result


def loads(text):
    return json.loads(text, parse_constant=reject_constant, object_pairs_hook=unique_keys)


def read_json(path):
    return loads(Path(path).read_text(encoding="utf-8"))


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def csv_rows(path):
    with Path(path).open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, strict=True)
        if not reader.fieldnames or len(set(reader.fieldnames)) != len(reader.fieldnames):
            raise ValueError("Missing or duplicate CSV headers")
        for row in reader:
            if None in row or any(v is None for v in row.values()):
                raise ValueError("Malformed CSV row")
            yield row


def profile(path):
    ids, months, ratings = set(), Counter(), Counter()
    counts = Counter(records=0, duplicate_review_ids=0, empty_review_text=0, missing_app_version=0)
    content = hashlib.sha256()
    first, last = None, None
    for row in csv_rows(path):
        digest = row_sha(row)
        content.update(bytes.fromhex(digest))
        counts["records"] += 1
        counts["duplicate_review_ids"] += row["review_id"] in ids
        ids.add(row["review_id"])
        counts["empty_review_text"] += not row["review_text"].strip()
        counts["missing_app_version"] += not row["app_version"].strip()
        months[row["review_timestamp"][:7]] += 1
        ratings[row["review_rating"]] += 1
        first = min(first, row["review_timestamp"]) if first else row["review_timestamp"]
        last = max(last, row["review_timestamp"]) if last else row["review_timestamp"]
    return {"file_sha256": sha(path), "parsed_rows_sha256": content.hexdigest(), "counts": dict(counts),
            "reviews_by_month": dict(months), "reviews_by_rating": dict(ratings), "first_review": first, "last_review": last}


def reference(full, analysis):
    expected = {}
    for row in csv_rows(analysis):
        if row["review_id"] in expected:
            raise ValueError("Duplicate reference ID")
        expected[row["review_id"]] = {"source_sha256": row_sha(row), "review_text": row["review_text"]}
    # Verify sample content against the actual full input, not just its declared checksum.
    unmatched = set(expected)
    for row in csv_rows(full):
        rid = row["review_id"]
        if rid in expected:
            if row_sha(row) != expected[rid]["source_sha256"]:
                raise ValueError("Analysis row differs from full input: " + rid)
            unmatched.discard(rid)
    if unmatched:
        raise ValueError("Analysis includes IDs absent from full input")
    return {"version": VERSION, "ingestion": profile(full), "analysis_sha256": sha(analysis), "rows": expected}


def fraction(a, b):
    return round(a / b, 6) if b else None


def mean_string(total, n):
    return str((Decimal(total) / Decimal(n)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


def reference_hash(ref):
    digest = hashlib.sha256()
    encoder = json.JSONEncoder(ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)
    for part in encoder.iterencode(ref):
        digest.update(part.encode("utf-8"))
    return digest.hexdigest()


def audit(folder, ref, gold=None, ref_digest=None):
    folder = Path(folder)
    issues = []
    issue_counts = Counter()
    def flag(code, detail):
        issue_counts[code] += 1
        if sum(i["code"] == code for i in issues) < 10:
            issues.append({"code": code, "detail": str(detail)[:300]})
    def obj(name):
        try:
            value = read_json(folder / name)
            if not isinstance(value, dict):
                raise ValueError("Expected a JSON object")
            return value
        except (OSError, ValueError) as e:
            flag("unreadable_file", name + ": " + str(e))
            return {}
    def lines(name):
        try:
            plain, compressed = folder / name, folder / (name + ".gz")
            if plain.exists() and compressed.exists():
                flag("ambiguous_file", name + " and compressed version both supplied")
            stream = plain.open(encoding="utf-8") if plain.exists() else gzip.open(compressed, "rt", encoding="utf-8")
            with stream as f:
                for number, line in enumerate(f, 1):
                    if not line.strip():
                        continue
                    try:
                        value = loads(line)
                        if not isinstance(value, dict):
                            raise ValueError("Expected a JSON object")
                        yield value
                    except ValueError as e:
                        flag("malformed_jsonl", f"{name}:{number}: {e}")
        except (OSError, EOFError, UnicodeError) as e:
            flag("unreadable_file", name + ": " + str(e))
    def table(name):
        try:
            return list(csv_rows(folder / name))
        except (OSError, ValueError, csv.Error) as e:
            flag("unreadable_file", name + ": " + str(e))
            return []

    expected = ref["rows"]
    run = obj("run.json")
    for key, value in {"version": VERSION, "analysis_count": len(expected), "analysis_sha256": ref["analysis_sha256"],
                       "classification_input_fields": ["review_text"]}.items():
        if run.get(key) != value:
            flag("run_manifest_mismatch", key)
    ingestion_ok = obj("ingestion.json") == ref["ingestion"]
    if not ingestion_ok:
        flag("ingestion_mismatch", "Full-file parsed digest, file hash, counts, dates or distributions differ.")
    unique, valid, quarantined, duplicate_ids = {}, {}, set(), set()
    raw_count = 0
    for row in lines("records.jsonl"):
        raw_count += 1
        rid = row.get("review_id")
        if not isinstance(rid, str) or rid not in expected:
            flag("unknown_review_id", rid)
            continue
        if rid in unique:
            flag("duplicate_review_id", rid)
            duplicate_ids.add(rid)
            valid.pop(rid, None)
            quarantined.discard(rid)
            continue
        unique[rid] = True
        if row.get("source_sha256") != expected[rid]["source_sha256"]:
            flag("source_mismatch", rid)
            continue
        if row.get("status") == "quarantined":
            if not isinstance(row.get("reason"), str) or not row["reason"].strip():
                flag("quarantine_without_reason", rid)
            elif not expected[rid]["review_text"].strip() and row["reason"] != "empty_review_text":
                flag("incorrect_empty_reason", rid)
            else:
                quarantined.add(rid)
            continue
        if row.get("status") != "completed":
            flag("invalid_status", rid)
            continue
        schema = (row.get("topic") in TOPICS and row.get("intent") in INTENTS
                  and isinstance(row.get("label_config"), str) and bool(row["label_config"].strip())
                  and type(row.get("severity")) is int and 1 <= row["severity"] <= 5
                  and type(row.get("sentiment")) in (int, float) and math.isfinite(row["sentiment"]) and -1 <= row["sentiment"] <= 1
                  and type(row.get("needs_review")) is bool
                  and isinstance(row.get("entities"), list) and all(isinstance(x, str) and x.strip() for x in row["entities"])
                  and isinstance(row.get("evidence_quote"), str) and bool(row["evidence_quote"].strip()))
        if not schema:
            flag("invalid_schema", rid)
            continue
        if row["evidence_quote"] not in expected[rid]["review_text"]:
            flag("unsupported_quote", rid)
            continue
        valid[rid] = row
    for rid in duplicate_ids:
        valid.pop(rid, None)
        quarantined.discard(rid)
    missing = sorted(set(expected) - set(unique))
    if missing:
        flag("missing_records", f"{len(missing)} IDs; first: {missing[:5]}")
    labelable = {rid for rid, row in expected.items() if row["review_text"].strip()}
    cached = set()
    invalid_cached = set()
    label_fields = ("topic", "intent", "sentiment", "severity", "entities", "evidence_quote", "needs_review", "label_config")
    for rid, row in valid.items():
        origin = row.get("cache_source_id")
        if not origin:
            continue
        original = valid.get(origin) if isinstance(origin, str) else None
        if (not original or origin == rid or original.get("cache_source_id")
                or expected[rid]["review_text"] != expected[origin]["review_text"]
                or any(row.get(k) != original.get(k) for k in label_fields)
                or run.get("classification_input_fields") != ["review_text"]):
            flag("invalid_cache_reuse", rid)
            invalid_cached.add(rid)
        else:
            cached.add(rid)
    for rid in invalid_cached:
        valid.pop(rid)
    if labelable - set(valid):
        flag("unfinished_classification", len(labelable - set(valid)))

    members, member_reviews, pairs = defaultdict(list), set(), set()
    for row in table("membership.csv"):
        rid, iid = row.get("review_id"), row.get("issue_id")
        if not iid or rid not in valid:
            flag("invalid_membership", row)
            continue
        if (iid, rid) in pairs:
            flag("duplicate_membership", row)
            continue
        pairs.add((iid, rid))
        if valid[rid]["intent"] not in {"complaint", "cancellation"}:
            flag("noncomplaint_in_ranking", rid)
            continue
        member_reviews.add(rid)
        members[iid].append(valid[rid]["severity"])
    complaints = {rid for rid, row in valid.items() if row["intent"] in {"complaint", "cancellation"}}
    if complaints - member_reviews:
        flag("ungrouped_complaints", len(complaints - member_reviews))
    if run.get("allow_multi_issue") is not True:
        multi = [rid for rid, count in Counter(rid for _, rid in pairs).items() if count > 1]
        if multi:
            flag("undeclared_multi_issue", len(multi))
    calculated = []
    for iid, values in members.items():
        calculated.append({"issue_id": iid, "complaint_count": len(values), "severity_sum": sum(values),
                           "mean_severity": mean_string(sum(values), len(values)), "priority_score": sum(values)})
    calculated.sort(key=lambda x: (-x["priority_score"], x["issue_id"]))
    for rank, row in enumerate(calculated, 1):
        row["rank"] = rank
    actual_ranking = table("ranking.csv")
    rank_ids = set()
    for index, row in enumerate(actual_ranking):
        iid = row.get("issue_id")
        if iid in rank_ids:
            flag("duplicate_ranked_issue", iid)
        rank_ids.add(iid)
        target = calculated[index] if index < len(calculated) else None
        if target is None or any(str(target.get(k)) != row.get(k) for k in ("rank", "issue_id", "complaint_count", "severity_sum", "mean_severity", "priority_score")):
            flag("ranking_mismatch", f"row {index + 1}: {iid}")
    if len(actual_ranking) != len(calculated):
        flag("ranking_length", f"expected {len(calculated)}, received {len(actual_ranking)}")
    calc_by_id = {x["issue_id"]: x for x in calculated}
    claims = table("claims.csv")
    seen_claims = set()
    if not claims:
        flag("missing_claims", "Export numerical memo claims for cross-checking.")
    for claim in claims:
        cid, iid, metric = claim.get("claim_id"), claim.get("issue_id"), claim.get("metric")
        if not cid or cid in seen_claims:
            flag("invalid_claim_id", cid)
        seen_claims.add(cid)
        target = calc_by_id.get(iid, {})
        if metric not in {"complaint_count", "severity_sum", "mean_severity", "priority_score"} or str(target.get(metric)) != claim.get("value"):
            flag("claim_mismatch", cid)

    # Self-reported execution evidence: consistency checks, never execution attestation.
    before = obj("checkpoint_before.json").get("completed_ids", [])
    after = obj("checkpoint_after.json").get("completed_ids", [])
    for ids in (before, after):
        if isinstance(ids, list) and all(isinstance(x, str) for x in ids) and len(ids) != len(set(ids)):
            flag("duplicate_checkpoint_id", "Checkpoint completed_ids must be unique")
    before = set(before) if isinstance(before, list) and all(isinstance(x, str) for x in before) else set()
    after = set(after) if isinstance(after, list) and all(isinstance(x, str) for x in after) else set()
    if not before or not before < after or not after <= set(valid):
        flag("resume_snapshot_mismatch", "Need a nonempty checkpoint and additional completed IDs after resume.")
    roles, logged, request_ids = set(), set(), set()
    initial_logged, resume_logged = set(), set()
    model_calls = 0
    tokens_in = tokens_out = 0
    for event in lines("calls.jsonl"):
        model_calls += 1
        role = event.get("role")
        ids = event.get("review_ids")
        request_id = event.get("request_id")
        if not isinstance(request_id, str) or not request_id or request_id in request_ids:
            flag("invalid_request_id", request_id)
        else:
            request_ids.add(request_id)
        if role not in {"enrich", "verify", "group", "memo"} or not isinstance(event.get("model"), str) or not event["model"]:
            flag("invalid_call_role", request_id)
        elif event.get("outcome") == "succeeded":
            roles.add(role)
        if event.get("outcome") not in {"succeeded", "failed"}:
            flag("invalid_call_outcome", request_id)
        if not isinstance(ids, list) or not all(isinstance(x, str) and x in expected for x in ids) or len(ids) != len(set(ids)):
            flag("invalid_call_ids", request_id)
            continue
        for field in ("input_tokens", "output_tokens"):
            if type(event.get(field)) is not int or event[field] < 0:
                flag("invalid_usage", request_id)
        tokens_in += event.get("input_tokens", 0) if type(event.get("input_tokens")) is int else 0
        tokens_out += event.get("output_tokens", 0) if type(event.get("output_tokens")) is int else 0
        if role == "enrich":
            if not 1 <= len(ids) <= 50:
                flag("unbounded_batch", request_id)
            if event.get("outcome") == "succeeded":
                for rid in ids:
                    if rid in valid and event.get("label_config") == valid[rid]["label_config"]:
                        logged.add(rid)
                        if event.get("phase") == "initial":
                            initial_logged.add(rid)
                        elif event.get("phase") == "resume":
                            resume_logged.add(rid)
                    elif rid in valid:
                        flag("call_config_mismatch", rid)
            if event.get("phase") not in {"initial", "resume"}:
                flag("invalid_run_phase", request_id)
            if event.get("phase") == "resume" and before.intersection(ids):
                flag("reprocessed_checkpoint", request_id)
    if set(valid) - cached - logged:
        flag("unlogged_completed_records", len(set(valid) - cached - logged))
    if before - cached - initial_logged or not ((after - before - cached) & resume_logged):
        flag("resume_call_evidence", "Need successful initial calls before the checkpoint and new completed calls after resume.")
    if roles != {"enrich", "verify", "group", "memo"}:
        flag("missing_model_roles", sorted({"enrich", "verify", "group", "memo"} - roles))

    quality = score_gold(gold, valid, expected) if gold else {"status": "not_supplied", "official": False}
    files = {p.name: sha(p) for p in sorted(folder.iterdir()) if p.is_file() and p.suffix in {".json", ".jsonl", ".csv", ".gz"}}
    return {"audit_version": VERSION, "audit_script_sha256": sha(__file__), "reference_sha256": ref_digest or reference_hash(ref),
            "hash_convention": "Reference/benchmark: canonical UTF-8 JSON via canonical(); input files and audit script: exact bytes.",
            "input_files_sha256": files, "status": "pass" if not issue_counts else "review_required", "automatic_grade": None,
            "working_coverage_point_candidate": round(.2 * ingestion_ok + .2 * ((len(valid) + len(quarantined)) / len(expected) if expected else 0)
                                                       + .6 * (len(set(valid) & labelable) / len(labelable) if labelable else 0), 4),
            "coverage": {"expected": len(expected), "received_lines": raw_count, "unique_expected_ids": len(unique),
                         "missing": len(missing), "duplicate_ids": len(duplicate_ids), "valid_completed": len(valid),
                         "quarantined": len(quarantined), "invalid_or_unresolved": len(unique) - len(valid) - len(quarantined),
                         "labelable": len(labelable), "valid_cache_reuses": len(cached),
                         "labelable_completion_fraction": fraction(len(set(valid) & labelable), len(labelable)),
                         "accounted_fraction": fraction(len(valid) + len(quarantined), len(expected)),
                         "valid_completion_fraction": fraction(len(valid), len(expected))},
            "issue_counts": dict(sorted(issue_counts.items())), "examples": issues, "quality": quality,
            "calculated_ranking": calculated, "reported_calls": model_calls,
            "reported_input_tokens": tokens_in, "reported_output_tokens": tokens_out,
            "limits": ["Artifacts do not prove actual model execution, authorship, cost, or independent verification.",
                       "Exact quote membership does not prove that the quote supports the label.",
                       "Claims export checks numbers; a person must check that all memo claims are represented.",
                       "Benchmark agreement is diagnostic; final 4/3/3 grades require rubric review."]}


def score_gold(gold, valid, expected):
    rows = [row for row in gold.get("cases", [])]
    if not rows or len({r["review_id"] for r in rows}) != len(rows) or any(r["review_id"] not in expected for r in rows):
        raise ValueError("Invalid benchmark IDs")
    approved = [r for r in rows if r.get("status") == "approved" and r.get("reviewer")]
    totals = Counter()
    per_field = {name: Counter() for name in ("topic", "intent", "severity")}
    singleton_pairs = defaultdict(list)
    case_results = []
    for case in approved:
        rid = case["review_id"]
        prediction = valid.get(rid, {})
        result = {"review_id": rid, "valid_prediction": bool(prediction)}
        for field, allowed in (("topic", TOPICS), ("intent", INTENTS), ("severity", (1, 2, 3, 4, 5))):
            accepted = case.get("accepted_" + field)
            if not isinstance(accepted, list) or not accepted or any(type(x) is not type(allowed[0]) or x not in allowed for x in accepted):
                raise ValueError("Invalid accepted labels in benchmark: " + rid)
            ok = prediction.get(field) in accepted
            totals[field + "_correct"] += ok
            # Per-class metrics only for unambiguous singleton references.
            if len(accepted) == 1:
                label = str(accepted[0])
                per_field[field][label + ":support"] += 1
                per_field[field][label + ":correct"] += ok
                singleton_pairs[field].append((label, str(prediction.get(field, "<abstain>"))))
            result[field + "_correct"] = ok
        totals["joint_correct"] += all(result[f + "_correct"] for f in per_field)
        totals["abstentions"] += not bool(prediction)
        if prediction:
            totals["severity_distance_sum"] += min(abs(prediction["severity"] - x) for x in case["accepted_severity"])
            totals["severity_observed"] += 1
        case_results.append(result)
    n = len(approved)
    metrics = {}
    for field, pairs in singleton_pairs.items():
        scores = {}
        for label in sorted({actual for actual, _ in pairs}):
            tp = sum(actual == label and predicted == label for actual, predicted in pairs)
            support = sum(actual == label for actual, _ in pairs)
            predicted = sum(pred == label for _, pred in pairs)
            scores[label] = {"support": support, "predicted": predicted, "recall": fraction(tp, support),
                             "precision": fraction(tp, predicted), "f1": fraction(2 * tp, support + predicted)}
        metrics[field] = {"per_class": scores, "macro_f1": round(sum(x["f1"] for x in scores.values()) / len(scores), 6) if scores else None}
    return {"status": "ready" if n == len(rows) else "pending_human_review", "official": n == len(rows),
            "benchmark_version": gold.get("version"), "benchmark_sha256": hashlib.sha256(canonical(gold).encode()).hexdigest(),
            "total_cases": len(rows), "approved_cases": n, "missing_or_invalid_predictions": totals["abstentions"],
            "agreement": {f: fraction(totals[f + "_correct"], n) for f in (*per_field, "joint")},
            "severity_mae_on_valid_predictions": fraction(totals["severity_distance_sum"], totals["severity_observed"]),
            "class_counts": {f: dict(c) for f, c in per_field.items()}, "singleton_metrics": metrics, "cases": case_results,
            "note": "Missing, quarantined and invalid predictions count as incorrect in agreement; MAE reports valid predictions only. Pending labels prevent official use."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("profile", help="Read every full-file row and create ingestion.json")
    p.add_argument("--full", required=True, type=Path); p.add_argument("--out", required=True, type=Path)
    p = commands.add_parser("reference", help="Instructor: build one trusted reference, reused for the cohort")
    p.add_argument("--full", required=True, type=Path); p.add_argument("--analysis", required=True, type=Path); p.add_argument("--out", required=True, type=Path)
    for name in ("check", "cohort"):
        p = commands.add_parser(name)
        p.add_argument("--reference", required=True, type=Path); p.add_argument("--gold", type=Path)
        p.add_argument("--submission" if name == "check" else "--submissions", required=True, type=Path)
        p.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "profile":
        write_json(args.out, profile(args.full))
    elif args.command == "reference":
        write_json(args.out, reference(args.full, args.analysis))
    else:
        ref = read_json(args.reference)
        gold = read_json(args.gold) if args.gold else None
        if ref.get("version") != VERSION:
            raise ValueError("Unsupported reference version")
        ref_digest = reference_hash(ref)
        if args.command == "check":
            write_json(args.out, audit(args.submission, ref, gold, ref_digest))
        else:
            args.out.mkdir(parents=True, exist_ok=True)
            summary = []
            for folder in sorted(p for p in args.submissions.iterdir() if p.is_dir()):
                try:
                    result = audit(folder, ref, gold, ref_digest)
                    write_json(args.out / (folder.name + ".json"), result)
                    summary.append({"submission": folder.name, "status": result["status"], **result["coverage"],
                                    "topic_agreement": result["quality"].get("agreement", {}).get("topic"),
                                    "benchmark_official": result["quality"].get("official", False)})
                except (OSError, ValueError, KeyError, TypeError) as e:
                    summary.append({"submission": folder.name, "status": "audit_error", "error": str(e)})
            write_json(args.out / "cohort.json", summary)
            columns = sorted({key for row in summary for key in row})
            with (args.out / "cohort.csv").open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=columns); writer.writeheader(); writer.writerows(summary)
    print("Saved deterministic audit output to", args.out)


if __name__ == "__main__":
    main()
