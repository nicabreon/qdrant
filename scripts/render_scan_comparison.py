#!/usr/bin/env python3
"""Render audited sweep/micro summaries after all measurement processes finish.

Default inputs: all-sweeps-analysis.json and micro-comparison-all-runs.json next
 to this script. Outputs: comparison.md and comparison-wide.csv. This renderer
never runs benchmarks or rereads the large raw request reports.
"""

import argparse
import csv
import json
import math
from os.path import relpath
from pathlib import Path
import re
import sys
from urllib.parse import quote

LEVELS = (1, 4, 16, 32, 64, 128)
SCANS = ("visible", "filtered")
AXES = ("concurrency", "batch")
RUN_PATTERN = re.compile(r"sweep-([cb])-(baseline|amx)-r([12])")
MICRO_RUNS = (
    ("baseline-r1", "Native r1", "baseline", "topk_abba"),
    ("topk-r1", "TopK r1", "topk", "topk_abba"),
    ("topk-r2", "TopK r2", "topk", "topk_abba"),
    ("baseline-r2", "Native r2", "baseline", "topk_abba"),
    ("blocked-r1", "Blocked r1", "blocked", "later_kernel_runs"),
    ("amx-r1", "Combined AMX r1", "combined_amx", "later_kernel_runs"),
    ("blocked-r2", "Blocked r2", "blocked", "later_kernel_runs"),
    ("amx-r2", "Combined AMX r2", "combined_amx", "later_kernel_runs"),
    ("baseline-r3", "Native r3", "baseline", "later_kernel_runs"),
)
MICRO_DISPLAY = ("baseline-r1", "baseline-r2", "baseline-r3", "topk-r1", "topk-r2",
                 "blocked-r1", "blocked-r2", "amx-r1", "amx-r2")
MICRO_LABELS = {run: label for run, label, _, _ in MICRO_RUNS}
SWEEP_METRICS = (
    "logical_queries_per_second", "requests_per_second", "latency_p50_ms",
    "latency_p95_ms", "latency_p99_ms", "median_queue_ms", "median_service_ms",
    "elapsed_including_drain_seconds", "drain_seconds", "successful_requests",
    "successful_logical_queries",
)
MICRO_METRICS = (
    "logical_qps_median", "logical_qps_min", "logical_qps_max", "sample_count",
    "scan_duration_p50_ns", "scan_duration_p95_ns", "scan_duration_median_ns",
    "query_prepare_p50_ns", "query_prepare_p95_ns", "query_prepare_median_ns",
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def finite(value, label, positive=False):
    require(type(value) in (int, float) and math.isfinite(value), f"{label}: expected finite number")
    require(value > 0 if positive else value >= 0, f"{label}: invalid negative/zero value")
    return value


def close(actual, expected, label):
    finite(actual, label)
    require(math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-9), f"{label}: inconsistent value")


def table(headers, rows):
    lines = ["| " + " | ".join(headers) + " |",
             "| " + " | ".join("---" if i == 0 else "---:" for i in range(len(headers))) + " |"]
    lines.extend("| " + " | ".join(str(cell) for cell in row) + " |" for row in rows)
    return lines


def parse_sweep_run(run_id):
    match = RUN_PATTERN.fullmatch(run_id)
    require(match is not None, f"Unexpected sweep run ID: {run_id}")
    axis_code, variant, iteration = match.groups()
    return ("concurrency" if axis_code == "c" else "batch", variant, int(iteration))


def validate_sweeps(document):
    require(document["audit_passed"] is True, "Sweep input has not passed its raw-data audit")
    mask = document["operator_reported_cpu_mask"]
    require(len(mask) == 3 and len(set(mask)) == 3 and all(type(cpu) is int and cpu >= 0 for cpu in mask),
            "Expected three distinct nonnegative operator-reported logical CPUs")
    expected_reports = {(axis, variant, repeat) for axis in AXES
                        for variant in ("baseline", "amx") for repeat in (1, 2)}
    reports = {}
    for report in document["reports"]:
        key = parse_sweep_run(report["run_id"])
        require(key not in reports and report["axis"] == key[0], "Duplicate/mislabeled sweep report")
        require(report["samples_per_case"] == 1, "Renderer requires one measured sample per case in each r1/r2 run")
        reports[key] = report
    require(set(reports) == expected_reports, "Missing/unexpected sweep report; require both axes, both variants and r1/r2")
    first = next(iter(reports.values()))
    fixture = first["fixture"]
    for key, value in {"points": 1_000_000, "dim": 512, "top": 10, "seed": 42,
                       "filter_percent": 90, "search_threads": 3, "query_count": 128}.items():
        require(fixture[key] == value, f"Unexpected sweep fixture: {key}")
    for report in reports.values():
        require(report["fixture"] == fixture, "Sweep fixtures differ")
        require(report["b1_reference_sha256"] == first["b1_reference_sha256"], "Sweep reference hashes differ")
    cells = {}
    for row in document["samples"]:
        axis, variant, repeat = parse_sweep_run(row["run_id"])
        require(row["axis"] == axis and row["sample"] == 0, "Unexpected sweep axis/sample")
        require(row["scan"] in SCANS, "Unknown sweep scan")
        require(row["search_threads"] == 3, "Unexpected worker count")
        require(row["all_results_match_reference_reported_by_benchmark"] is True, "Sweep result parity not confirmed")
        batch, concurrency = row["batch"], row["concurrency"]
        level = concurrency if axis == "concurrency" else batch
        require(level in LEVELS and (batch == 1 if axis == "concurrency" else concurrency == 4),
                "Sweep cell violates fixed axis")
        key = (axis, row["scan"], level, variant, repeat)
        require(key not in cells, f"Duplicate sweep cell: {key}")
        for metric in SWEEP_METRICS:
            finite(row[metric], str(key) + "." + metric)
        finite(row["logical_queries_per_second"], str(key) + ".QPS", positive=True)
        close(row["logical_queries_per_second"], row["requests_per_second"] * batch, str(key) + ".QPS=RPS*B")
        require(row["latency_p50_ms"] <= row["latency_p95_ms"] <= row["latency_p99_ms"], "Sweep percentiles out of order")
        close(row["submission_window_seconds"], 10.0, str(key) + ".submission window")
        require(row["elapsed_including_drain_seconds"] >= 10.0, "Sweep elapsed interval excludes drain")
        close(row["drain_seconds"], row["elapsed_including_drain_seconds"] - 10.0, str(key) + ".drain")
        close(row["successful_logical_queries"], row["successful_requests"] * batch, str(key) + ".logical count")
        close(row["requests_per_second"], row["successful_requests"] / row["elapsed_including_drain_seconds"], str(key) + ".RPS")
        require(row["source"] == reports[(axis, variant, repeat)]["source"], "Sample/report source mismatch")
        cells[key] = row
    expected_cells = {(axis, scan, level, variant, repeat) for axis in AXES for scan in SCANS
                      for level in LEVELS for variant in ("baseline", "amx") for repeat in (1, 2)}
    require(set(cells) == expected_cells, "Missing/unexpected sweep comparison cells")
    return cells, fixture, mask


def validate_micro(document):
    require(document["audit_passed"] is True and document["configuration_equivalent"] is True,
            "Micro input has not passed its configuration/raw-data audit")
    require(document["all_saved_b1_references_equal"] is True, "Micro references did not match")
    config = document["normalized_config"]
    for key, value in {"points": 1_000_000, "dim": 512, "top": 10, "seed": 42,
                       "filter_percent": 90, "samples": 10, "batches": [1, 8, 64], "concurrency": []}.items():
        require(config[key] == value, f"Unexpected micro configuration: {key}")
    expected_runs = {run: (index, variant, phase) for index, (run, _, variant, phase)
                     in enumerate(MICRO_RUNS, 1)}
    runs = {}
    for run in document["runs"]:
        run_id = run["run_id"]
        require(run_id in expected_runs and run_id not in runs, "Missing/duplicate/unexpected micro run")
        index, variant, phase = expected_runs[run_id]
        require((run["run_order"], run["variant"], run["phase"]) == (index, variant, phase), "Micro chronology/variant mismatch")
        require(run["b1_reference_sha256"] == document["b1_reference_sha256"], "Micro reference hash mismatch")
        runs[run_id] = run
    require(set(runs) == set(expected_runs), "Missing micro run")
    cells = {}
    for row in document["case_summaries"]:
        run_id = row["run_id"]
        require(run_id in expected_runs, "Unexpected micro row run")
        index, variant, phase = expected_runs[run_id]
        require((row["run_order"], row["variant"], row["phase"]) == (index, variant, phase), "Micro row chronology/variant mismatch")
        key = (row["scan"], row["batch"], run_id)
        require(key not in cells and row["sample_count"] == 10, "Duplicate micro row/unexpected sample count")
        for metric in MICRO_METRICS:
            finite(row[metric], str(key) + "." + metric)
        finite(row["logical_qps_median"], str(key) + ".median QPS", positive=True)
        require(row["logical_qps_min"] <= row["logical_qps_median"] <= row["logical_qps_max"], "Micro QPS range invalid")
        require(row["scan_duration_p50_ns"] <= row["scan_duration_p95_ns"] and
                row["query_prepare_p50_ns"] <= row["query_prepare_p95_ns"], "Micro percentiles out of order")
        require(row["raw_file"] == runs[run_id]["raw_file"], "Micro row/source mismatch")
        cells[key] = row
    expected_cells = {(scan, batch, run_id) for scan in SCANS for batch in (1, 8, 64) for run_id in expected_runs}
    require(set(cells) == expected_cells, "Missing/unexpected micro comparison cells")
    return cells, config


def render(sweep_doc, micro_doc, sweep_path, micro_path, output_dir):
    sweep_link = quote(relpath(sweep_path, output_dir), safe="/.-_")
    micro_link = quote(relpath(micro_path, output_dir), safe="/.-_")
    sweeps, fixture, mask = validate_sweeps(sweep_doc)
    micro, micro_config = validate_micro(micro_doc)
    for key in ("points", "dim", "top", "seed", "filter_percent", "warmup"):
        require(fixture[key] == micro_config[key], f"Micro/sweep fixture differs: {key}")
    lines = [
        "# SQ8 plain-scan comparison", "",
        "These tables compare native Qdrant with the combined patch in an in-process scan/request-pool diagnostic. The combined patch includes SIMD TopK filtering, shared blocked SQ8 scoring and AMX dispatch with portable fallbacks. They are not HTTP service or cluster benchmarks.", "",
        "## How to read the tables", "",
        f"The fixture is 1,000,000 normalized 512D SQ8 vectors, dot distance, Top10, seed 42. Visible scans visit all IDs; filtered scans accept 90%. Each sweep uses one process, three search workers, and operator-reported affinity to logical CPUs {', '.join(map(str, mask))}. Each cell admits closed-loop requests for 10 seconds and then drains all outstanding work.", "",
        "**QPS counts logical queries; RPS counts batch requests. QPS = RPS × B.** The primary sweep rates divide the complete submitted cohort by actual elapsed time including drain. Request p99 includes queueing, scorer/query preparation, scan/result construction and completion delivery, including requests that finish during drain. It is a percentile of whole batch requests, not individual queries within a batch.", "",
        "Each r1/r2 value remains separate. **Mean QPS change (%) = 100 × (((patched QPS r1 + patched QPS r2) / 2) / ((native QPS r1 + native QPS r2) / 2) − 1).** This is a descriptive comparison of the two arithmetic mean run rates, not pooled throughput, a confidence interval, or evidence of statistical significance. p99 values are not averaged.", "",
        "C is the maximum number of outstanding batch requests; it does not increase the fixed three-worker CPU budget. Higher queue latency alone does not establish a throughput collapse. The B1/C4 cell appears in both axes from separate runs and is never pooled across axes. Run labels identify repetitions, not simultaneous matched measurements.", "",
        "The B1 path uses native per-query SQ8 scoring, including its existing SIMD; it does not execute the multi-query AMX kernel. A B1 difference therefore cannot be attributed to AMX acceleration. These runs occurred on a shared KVM VM, so drift and run order remain material limitations.", "",
    ]
    wide_rows = []
    for axis in AXES:
        if axis == "concurrency":
            title, level_label = "Concurrency sweep — B = 1", "C"
        else:
            title, level_label = "Batch sweep — C = 4", "B"
        lines += ["## " + title, ""]
        for scan in SCANS:
            lines += ["### " + scan.capitalize(), ""]
            display = []
            for level in LEVELS:
                values = {(variant, repeat): sweeps[(axis, scan, level, variant, repeat)]
                          for variant in ("baseline", "amx") for repeat in (1, 2)}
                native_mean = sum(values[("baseline", repeat)]["logical_queries_per_second"] for repeat in (1, 2)) / 2
                patched_mean = sum(values[("amx", repeat)]["logical_queries_per_second"] for repeat in (1, 2)) / 2
                change = (patched_mean / native_mean - 1) * 100
                qps = [values[(variant, repeat)]["logical_queries_per_second"]
                       for variant in ("baseline", "amx") for repeat in (1, 2)]
                p99 = [values[(variant, repeat)]["latency_p99_ms"]
                       for variant in ("baseline", "amx") for repeat in (1, 2)]
                display.append([level, *(f"{value:,.2f}" for value in qps), f"{change:+.2f}%",
                                *(f"{value:,.2f}" for value in p99)])
                wide = {"section": "sweep", "axis": axis, "scan": scan,
                        "batch": 1 if axis == "concurrency" else level,
                        "concurrency": level if axis == "concurrency" else 4,
                        "native_arithmetic_mean_run_qps": native_mean,
                        "combined_arithmetic_mean_run_qps": patched_mean,
                        "descriptive_mean_run_qps_change_percent": change}
                for (variant, repeat), row in values.items():
                    prefix = ("native" if variant == "baseline" else "combined") + f"_r{repeat}"
                    wide[prefix + "_run_id"] = row["run_id"]
                    wide[prefix + "_raw_file"] = Path(row["source"]).name
                    for metric in SWEEP_METRICS:
                        wide[prefix + "_" + metric] = row[metric]
                wide_rows.append(wide)
            lines += table([level_label, "Native QPS r1", "Native QPS r2", "Combined QPS r1", "Combined QPS r2",
                            "Mean QPS change", "Native p99 ms r1", "Native p99 ms r2", "Combined p99 ms r1", "Combined p99 ms r2"], display)
            lines += [""]
    lines += [
        "## Staged single-thread microbench", "",
        "Each cell below is the median logical QPS of ten timed scans within that specific run. Native r1/r2/r3, TopK r1/r2, blocked r1/r2 and combined AMX r1/r2 remain separate; no micro run medians are pooled. Blocked includes TopK plus the shared SQ8 path; combined AMX adds AMX dispatch/fallback. The single execution thread was pinned to logical CPU 2.", "",
        "Micro timing includes scan, scoring, TopK and result construction, while scorer/query preparation is measured separately and excluded from micro QPS. Sweep service includes preparation and queueing is included in end-to-end request latency. Micro QPS and sweep request latency therefore measure different intervals.", "",
        "Chronological order: **native r1 → TopK r1 → TopK r2 → native r2**, followed later by **blocked r1 → combined AMX r1 → blocked r2 → combined AMX r2 → native r3**. Columns are grouped by implementation for reading, not chronological adjacency.", "",
    ]
    for scan in SCANS:
        lines += ["### " + scan.capitalize() + " — median logical QPS", ""]
        display = []
        for batch in (1, 8, 64):
            display.append([batch, *(f"{micro[(scan, batch, run)]['logical_qps_median']:,.2f}" for run in MICRO_DISPLAY)])
            wide = {"section": "micro", "axis": "single_thread_micro", "scan": scan, "batch": batch, "concurrency": ""}
            for run in MICRO_DISPLAY:
                row = micro[(scan, batch, run)]
                prefix = "micro_" + run.replace("-", "_")
                wide[prefix + "_run_id"] = run
                wide[prefix + "_raw_file"] = Path(row["raw_file"]).name
                for metric in MICRO_METRICS:
                    wide[prefix + "_" + metric] = row[metric]
            wide_rows.append(wide)
        lines += table(["B", *(MICRO_LABELS[run] for run in MICRO_DISPLAY)], display)
        lines += [""]
    lines += [
        "## Correctness and evidence", "",
        "Both input analyses passed their audits and contain every required cell. Within each measurement family, saved ordered B1 reference IDs and exact score bits match across builds. The renderer also checks fixture compatibility and cell/rate consistency; it does not rerun the underlying correctness checks.", "",
        "Micro correctness checks run outside timing: B1 references and B1/B8/B64 preflight comparisons. Timed micro results are consumed but not individually revalidated. In the sweeps, the benchmark retains and checks every completed request's result after timing; raw JSON stores timestamps and a success flag, not the measured result matrices. The raw-data audit verifies that flag and the saved B1 references.", "",
        "The wide CSV retains unrounded sweep QPS/RPS, p50/p95/p99, median queue/service, elapsed/drain times, counts and source-run names, plus every micro run's QPS and scan/preparation percentiles. Sweep p99 uses the complete submitted request cohort. Micro percentiles use nearest rank; with ten samples, micro p95 is the maximum sample, not a service SLO.", "",
        f"Audited sweep summary: [{sweep_path.name}]({sweep_link}). Audited micro summary: [{micro_path.name}]({micro_link}). Wide numeric table: [comparison-wide.csv](comparison-wide.csv). These summaries point to the raw reports. Measurement values remain unchanged; publication may replace machine-local paths with portable provenance labels.", "",
        "No statistical-significance, isolated AMX B1 speedup, native-server concurrency, or distributed-cluster claim follows from these tables alone.", "",
    ]
    return "\n".join(lines), wide_rows


def main():
    directory = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sweeps", type=Path, default=directory / "all-sweeps-analysis.json")
    parser.add_argument("--micro", type=Path, default=directory / "micro-comparison-all-runs.json")
    parser.add_argument("--output-dir", type=Path, default=directory)
    args = parser.parse_args()
    sweep_path, micro_path = args.sweeps.resolve(), args.micro.resolve()
    sweep_doc = json.loads(sweep_path.read_text())
    micro_doc = json.loads(micro_path.read_text())
    markdown, rows = render(sweep_doc, micro_doc, sweep_path, micro_path, args.output_dir.resolve())
    markdown_path = args.output_dir.resolve() / "comparison.md"
    csv_path = args.output_dir.resolve() / "comparison-wide.csv"
    require(not {markdown_path, csv_path} & {sweep_path, micro_path}, "Output would replace an input")
    fields = list(dict.fromkeys(key for row in rows for key in row))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(markdown)
    with csv_path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Rendered {len(rows)} comparison rows: 24 sweep cases and 6 micro cases")
    print(markdown_path)
    print(csv_path)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Render failed: {error}", file=sys.stderr)
        sys.exit(1)
