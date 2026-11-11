#!/usr/bin/env python3
"""Audit preserved microbench runs and summarize each run separately.

Run only after all measurement processes have finished. Standard library only;
this script never launches benchmarks, rewrites raw reports or pools run medians.
"""

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics
import struct
import sys

# Operator-supplied chronology; old runs do not have timestamp metadata.
RUNS = (
    ("baseline-r1", "baseline", "topk_abba", "A1"),
    ("topk-r1", "topk", "topk_abba", "B1"),
    ("topk-r2", "topk", "topk_abba", "B2"),
    ("baseline-r2", "baseline", "topk_abba", "A2"),
    ("blocked-r1", "blocked", "later_kernel_runs", "blocked1"),
    ("amx-r1", "combined_amx", "later_kernel_runs", "amx1"),
    ("blocked-r2", "blocked", "later_kernel_runs", "blocked2"),
    ("amx-r2", "combined_amx", "later_kernel_runs", "amx2"),
    ("baseline-r3", "baseline", "later_kernel_runs", "base3"),
)
SCANS = ("visible", "filtered")
# These fields were introduced by the concurrency harness. Its micro path is
# unchanged and ignores these defaults. Unknown differences are never discarded.
SCHEMA_DEFAULTS = {
    "concurrency": [],
    "search_threads": 3,
    "duration_secs": 10.0,
    "query_pool": 128,
    "result_budget_mib": 64,
}
PROVENANCE_CONFIG_FIELDS = {
    "output", "source", "source_revision", "source_commit", "benchmark_commit",
    "binary_sha256",
}
NOTES = [
    "Scope: single-threaded in-process SQ8 plain-scan microbench, 1M x 512D, B1/B8/B64. These are not HTTP/server latencies or multi-node results.",
    "QPS is logical queries per second: B / timed scan duration. Each reported QPS is the arithmetic median of that run's sample QPS values; run medians are never pooled.",
    "Scan duration includes scoring, top-k and result construction. Query/scorer preparation is timed separately and excluded from reported scan QPS. Scan duration is per batch request, not per logical query.",
    "Duration percentiles use nearest rank, ceil(p*n)-1. With ten samples p95 is the maximum sample; these small-sample percentiles are descriptive, not a latency SLO or statistical significance test. The conventional duration median is also retained separately.",
    "Micro correctness uses untimed B1 references and untimed B1/B8/B64 preflight checks. Saved ordered B1 IDs and exact score bits are compared across all input files. Timed micro results are consumed with black_box but are not individually retained or revalidated by this script.",
    "The concurrency sweeps use a different measurement contract: queue plus per-request service includes preparation; every completed request's result is checked after timing. Do not compare micro scan QPS directly with sweep end-to-end service or request rates.",
    "TopK sequence is baseline A1, TopK B1, TopK B2, baseline A2. Later kernel sequence is blocked1, AMX1, blocked2, AMX2, baseline3. Chronology is operator-supplied; available runner timestamps are preserved, not invented for older runs.",
    "The combined_amx binary contains integrated SIMD TopK, blocked SQ8 batching and AMX dispatch/fallback. Its B1 path uses native per-query SQ8 scoring (including its existing SIMD); a B1 change is not isolated evidence of AMX acceleration. Future sweeps compare baseline against this combined patched binary, not an isolated AMX intervention.",
    "Shared-VM drift, cache state and non-adjacent runs can affect rates. This summary makes no statistical-significance or causal-speedup claim, and preserves every run separately.",
    "Unavailable hardware cycle values stay null. No frequency-derived cycles are substituted. Raw reports and companion metadata are left unchanged.",
    "All configurations must match after excluding only output/source-provenance fields and filling documented defaults absent from the older micro schema. Inactive concurrency settings remain visible in normalized configuration.",
]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, name, minimum=0):
    require(type(value) is int and value >= minimum, f"{name}: expected integer >= {minimum}")
    return value


def number(value, name):
    require(type(value) in (int, float) and math.isfinite(value), f"{name}: expected finite number")
    return value


def close(actual, expected, name):
    number(actual, name)
    require(math.isclose(actual, expected, rel_tol=1e-10, abs_tol=1e-10),
            f"{name}: {actual} != calculated {expected}")


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[math.ceil(fraction * len(ordered)) - 1]


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def normalized_config(raw, label):
    config = {key: value for key, value in raw.items() if key not in PROVENANCE_CONFIG_FIELDS}
    supplied_defaults = {}
    for key, value in SCHEMA_DEFAULTS.items():
        if key not in config:
            config[key] = value
            supplied_defaults[key] = value
    require(config["concurrency"] == [], f"{label}: concurrency report supplied as micro input")
    for field in ("points", "dim", "top", "samples"):
        integer(config[field], f"{label}.{field}", 1)
    integer(config["warmup"], f"{label}.warmup")
    integer(config["seed"], f"{label}.seed")
    require(config["batches"] == [1, 8, 64], f"{label}: expected B1/B8/B64")
    require(config["points"] == 1_000_000 and config["dim"] == 512,
            f"{label}: expected the contribution fixture of 1M x 512D")
    integer(config["filter_percent"], f"{label}.filter_percent", 1)
    require(config["filter_percent"] <= 100, f"{label}: invalid filter percentage")
    return config, supplied_defaults


def audit_reference(reference, config, label):
    require(isinstance(reference, list) and len(reference) == 2, f"{label}: expected two reference scans")
    require([row["scan"] for row in reference] == list(SCANS), f"{label}: reference scan order changed")
    for row in reference:
        require(len(row["queries"]) == max(config["batches"]), f"{label}: incorrect micro query-pool size")
        accepted = config["points"] if row["scan"] == "visible" else (
            config["points"] // 100 * config["filter_percent"]
            + min(config["points"] % 100, config["filter_percent"]))
        for query in row["queries"]:
            require(len(query) == min(config["top"], accepted), f"{label}: reference result length")
            ids = []
            previous = math.inf
            for pair in query:
                require(isinstance(pair, list) and len(pair) == 2, f"{label}: malformed (ID, score bits)")
                point_id = integer(pair[0], f"{label}.reference.id")
                score_bits = integer(pair[1], f"{label}.reference.score_bits")
                require(point_id < config["points"] and score_bits < 2**32, f"{label}: invalid reference value")
                require(row["scan"] != "filtered" or point_id % 100 < config["filter_percent"],
                        f"{label}: reference violates synthetic filter")
                score = struct.unpack("!f", struct.pack("!I", score_bits))[0]
                require(math.isfinite(score) and score <= previous, f"{label}: reference scores not finite/descending")
                previous = score
                ids.append(point_id)
            require(len(set(ids)) == len(ids), f"{label}: duplicate reference IDs")


def audit_samples(samples, config, label):
    expected_cases = [(scan, batch) for scan in SCANS for batch in config["batches"]]
    expected_keys = []
    for sample in range(config["samples"]):
        cases = expected_cases if sample % 2 == 0 else list(reversed(expected_cases))
        expected_keys.extend((scan, batch, sample) for scan, batch in cases)
    require(isinstance(samples, list), f"{label}: samples must be a list")
    keys = [(row["scan"], row["batch"], row["sample"]) for row in samples]
    require(keys == expected_keys, f"{label}: missing/duplicate/out-of-order samples")
    grouped = {case: [] for case in expected_cases}
    for row in samples:
        scan, batch, sample = row["scan"], row["batch"], row["sample"]
        context = f"{label}/{scan}/B{batch}/sample{sample}"
        elapsed = integer(row["elapsed_ns"], context + ".elapsed_ns", 1)
        integer(row["query_prepare_ns"], context + ".query_prepare_ns")
        scored = config["points"] if scan == "visible" else (
            config["points"] // 100 * config["filter_percent"]
            + min(config["points"] % 100, config["filter_percent"]))
        require(row["scored_points_per_query"] == scored, context + ": wrong scored point count")
        close(row["queries_per_second"], batch * 1e9 / elapsed, context + ".QPS")
        close(row["ns_per_query_point"], elapsed / (scored * batch), context + ".ns_per_query_point")
        cycles = row["cpu_cycles"]
        if cycles is None:
            require(row["cycles_per_query_point"] is None, context + ": invented cycle estimate")
            require(not config["require_cycles"], context + ": required cycles missing")
        else:
            integer(cycles, context + ".cpu_cycles")
            close(row["cycles_per_query_point"], cycles / (scored * batch), context + ".cycles_per_query_point")
        grouped[(scan, batch)].append(row)
    return grouped


def summarize(rows, run_id, variant, phase, phase_label, run_order, path):
    elapsed = [row["elapsed_ns"] for row in rows]
    prepare = [row["query_prepare_ns"] for row in rows]
    qps = [row["queries_per_second"] for row in rows]
    cycle_values = [row["cycles_per_query_point"] for row in rows if row["cpu_cycles"] is not None]
    result = {
        "run_order": run_order, "phase": phase, "phase_label": phase_label,
        "run_id": run_id, "variant": variant, "raw_file": str(path),
        "scan": rows[0]["scan"], "batch": rows[0]["batch"], "sample_count": len(rows),
        "logical_qps_median": statistics.median(qps),
        "logical_qps_min": min(qps), "logical_qps_max": max(qps),
        "scan_duration_p50_ns": percentile(elapsed, 0.5),
        "scan_duration_p95_ns": percentile(elapsed, 0.95),
        "scan_duration_median_ns": statistics.median(elapsed),
        "query_prepare_p50_ns": percentile(prepare, 0.5),
        "query_prepare_p95_ns": percentile(prepare, 0.95),
        "query_prepare_median_ns": statistics.median(prepare),
        "hardware_cycle_sample_count": len(cycle_values),
        "cycles_per_query_point_median": statistics.median(cycle_values) if cycle_values else None,
    }
    return result


def text_report(analysis):
    lines = ["SQ8 plain-scan microbench: separate per-run summaries", "", "AUDIT PASSED", ""]
    lines.extend(NOTES)
    lines += ["", "Per-run results (scan p50/p95 in ms, query preparation p50/p95 in us):", "",
              "run_order run_id        scan     batch   median_QPS   scan_p50_ms   scan_p95_ms   prep_p50_us   prep_p95_us"]
    for row in analysis["case_summaries"]:
        lines.append(
            f"{row['run_order']:9d} {row['run_id']:<13} {row['scan']:<8} {row['batch']:5d} "
            f"{row['logical_qps_median']:12.3f} {row['scan_duration_p50_ns']/1e6:13.3f} "
            f"{row['scan_duration_p95_ns']/1e6:13.3f} {row['query_prepare_p50_ns']/1e3:13.3f} "
            f"{row['query_prepare_p95_ns']/1e3:13.3f}")
    lines += ["", "Input provenance:"]
    for run in analysis["runs"]:
        lines.append(f"{run['run_id']}: {run['raw_file']} SHA256={run['raw_sha256']}")
    lines += ["", "No run medians have been pooled. Consult the raw sample files and complete JSON metadata before drawing performance conclusions."]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--output-prefix", type=Path)
    args = parser.parse_args()
    input_dir = args.input_dir.resolve()
    prefix = (args.output_prefix or input_dir / "micro-comparison-all-runs").resolve()
    output_paths = [Path(str(prefix) + suffix) for suffix in (".json", ".csv", ".txt")]
    protected = {(input_dir / (run[0] + suffix)).resolve() for run in RUNS
                 for suffix in (".json", ".metadata.json")}
    require(not any(path in protected for path in output_paths), "Output would replace a raw input")
    runs, summaries = [], []
    reference = None
    canonical_config = None
    for order, (run_id, variant, phase, phase_label) in enumerate(RUNS, 1):
        path = input_dir / (run_id + ".json")
        raw = path.read_bytes()
        report = json.loads(raw)
        require(report.get("mode") in (None, "micro"), f"{run_id}: invalid mode")
        config, supplied_defaults = normalized_config(report["config"], run_id)
        if canonical_config is None:
            canonical_config = config
        require(config == canonical_config, f"{run_id}: configuration differs after documented normalization")
        audit_reference(report["b1_reference_results"], config, run_id)
        if reference is None:
            reference = report["b1_reference_results"]
        require(report["b1_reference_results"] == reference, f"{run_id}: saved B1 IDs/score bits differ")
        grouped = audit_samples(report["samples"], config, run_id)
        metadata_path = path.with_suffix(".metadata.json")
        metadata = json.loads(metadata_path.read_text()) if metadata_path.exists() else None
        if metadata is not None:
            require(metadata.get("cross_build_ordered_ids_and_score_bits_match") is True,
                    f"{run_id}: companion runner recorded failed/missing parity")
        runs.append({
            "run_order": order, "run_id": run_id, "variant": variant,
            "phase": phase, "phase_label": phase_label,
            "raw_file": str(path), "raw_sha256": sha256(raw),
            "raw_config": report["config"], "schema_defaults_supplied": supplied_defaults,
            "b1_reference_sha256": sha256(canonical(report["b1_reference_results"])),
            "metadata_file": str(metadata_path) if metadata is not None else None,
            "runner_metadata": metadata,
            "sample_count": len(report["samples"]),
        })
        for rows in grouped.values():
            summaries.append(summarize(rows, run_id, variant, phase, phase_label, order, path))
    analysis = {
        "audit_passed": True, "scope": "single_thread_in_process_scan_microbenchmark",
        "configuration_equivalent": True, "normalized_config": canonical_config,
        "all_saved_b1_references_equal": True,
        "b1_reference_sha256": sha256(canonical(reference)),
        "chronology_source": "operator-provided order; available runner timestamps preserved",
        "percentile_method": "nearest rank: sorted_values[ceil(p*n)-1]",
        "qps_aggregation": "statistics.median within each run/scan/batch only",
        "notes": NOTES, "runs": runs, "case_summaries": summaries,
    }
    # Write summaries only after every raw input has passed; never modify raw files.
    output_paths[0].parent.mkdir(parents=True, exist_ok=True)
    output_paths[0].write_text(json.dumps(analysis, indent=2, allow_nan=False) + "\n")
    with output_paths[1].open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    output_paths[2].write_text(text_report(analysis))
    print(f"Audit passed: {len(runs)} runs, {len(summaries)} separate case summaries")
    for path in output_paths:
        print(path)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Audit failed: {error}", file=sys.stderr)
        sys.exit(1)
