#!/usr/bin/env python3
"""Audit completed in-process SQ8 sweep reports without pooling different runs.

Example:
  python3 analyze_scan_sweeps.py \
    --concurrency sweep-c-baseline-r1.json \
    --batch sweep-b-baseline-r1.json \
    --cpu-mask 2,3,4 --output-prefix native-sweeps-r1-analysis

Only standard-library modules are used. Supply completed reports, not live logs.
CPU affinity is operator-supplied provenance: the JSON does not record or prove it.
"""

import argparse
import csv
import hashlib
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

LEVELS = (1, 4, 16, 32, 64, 128)
SCANS = ("visible", "filtered")


class AuditError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise AuditError(message)


def integer(value, name, minimum=0):
    require(type(value) is int and value >= minimum, f"{name}: expected integer >= {minimum}")
    return value


def close(actual, expected, name):
    require(
        isinstance(actual, (int, float)) and math.isfinite(actual)
        and math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-9),
        f"{name}: stored {actual!r}, recomputed {expected!r}",
    )


def max_occupancy(intervals, limit, name):
    events = []
    for start, end in intervals:
        require(start <= end, f"{name}: reversed interval")
        if start != end:
            events.extend(((start, 1), (end, -1)))
    # Half-open intervals: simultaneous completion happens before next admission.
    events.sort()
    active = maximum = 0
    for _, delta in events:
        active += delta
        require(0 <= active <= limit, f"{name}: occupancy {active} exceeds limit {limit}")
        maximum = max(maximum, active)
    require(active == 0, f"{name}: unfinished intervals")
    return maximum


def percentile(sorted_values, percent):
    return sorted_values[(len(sorted_values) * percent + 99) // 100 - 1]


def canonical_reference(report, config):
    references = report["b1_reference_results"]
    require(len(references) == 2, "expected exactly two reference scan modes")
    query_count = max(config["query_pool"], max(config["batches"]))
    by_scan = {}
    for reference in references:
        scan = reference["scan"]
        require(scan in SCANS and scan not in by_scan, "invalid/duplicate reference scan")
        queries = reference["queries"]
        require(len(queries) == query_count, "reference query pool size mismatch")
        scored = config["points"] if scan == "visible" else (
            config["points"] // 100 * config["filter_percent"]
            + min(config["points"] % 100, config["filter_percent"])
        )
        for query, points in enumerate(queries):
            require(len(points) == min(config["top"], scored), f"{scan} reference {query}: unexpected top-k length")
            ids = []
            for pair in points:
                require(isinstance(pair, list) and len(pair) == 2, "malformed reference point")
                idx, bits = pair
                integer(idx, "reference point ID")
                integer(bits, "reference score bits")
                require(idx < config["points"] and bits < 2**32, "reference point out of range")
                if scan == "filtered":
                    require(idx % 100 < config["filter_percent"], "reference fails synthetic filter")
                ids.append(idx)
            require(len(ids) == len(set(ids)), "duplicate reference result IDs")
        by_scan[scan] = queries
    encoded = json.dumps(by_scan, sort_keys=True, separators=(",", ":")).encode()
    return query_count, hashlib.sha256(encoded).hexdigest()


def audit_sample(sample, config, query_count, axis, run_id, source):
    scan = sample["scan"]
    require(scan in SCANS, "invalid scan mode")
    batch = integer(sample["batch"], "batch", 1)
    concurrency = integer(sample["concurrency"], "concurrency", 1)
    workers = integer(sample["search_threads"], "search_threads", 1)
    sample_id = integer(sample["sample"], "sample")
    require(workers == config["search_threads"], "sample worker count differs from configuration")
    require((batch == 1 and concurrency in LEVELS) if axis == "concurrency"
            else (concurrency == 4 and batch in LEVELS), "sample outside requested fixed-axis sweep")
    require(sample["all_results_match_reference"] is True, "benchmark did not confirm exact result parity")
    window = integer(sample["submission_window_ns"], "submission_window_ns", 1)
    elapsed = integer(sample["elapsed_including_drain_ns"], "elapsed_including_drain_ns", 1)
    require(elapsed >= window, "elapsed time excludes the requested window/drain")
    require(abs(window - round(config["duration_secs"] * 1e9)) <= 1, "submission duration mismatch")
    require(sample["drain_ns"] == elapsed - window, "drain duration mismatch")
    requests = sample["requests"]
    require(requests, "no completed requests")
    require(sample["successful_requests"] == len(requests), "successful request count mismatch")
    require(sample["successful_logical_queries"] == len(requests) * batch, "logical query count mismatch")
    require(len(requests) >= concurrency, "initial client cohort incomplete")
    budget = integer(sample["result_budget_bytes"], "result_budget_bytes", 1)
    estimated = integer(sample["estimated_bytes_per_request"], "estimated_bytes_per_request", 1)
    require(budget == config["result_budget_mib"] * 1024 * 1024, "retention budget/config mismatch")
    require(len(requests) * estimated <= budget, "reported cohort exceeds retention estimate")

    last_client_received = [0] * concurrency
    ids = set()
    outstanding, services, latencies, queue, service, delivery = [], [], [], [], [], []
    last_received = 0
    in_window = 0
    for position, request in enumerate(requests):
        request_id = integer(request["request_id"], f"request {position} ID")
        client = integer(request["client"], f"request {request_id} client")
        require(client < concurrency and request_id not in ids, "invalid client or duplicate request ID")
        ids.add(request_id)
        require(request["query_offset"] == request_id % query_count, "query rotation mismatch")
        submitted, started, finished, received = (
            integer(request[field], f"request {request_id} {field}")
            for field in ("submitted_ns", "started_ns", "finished_ns", "received_ns")
        )
        require(submitted <= started <= finished <= received <= elapsed,
                f"request {request_id}: timestamps out of order or beyond elapsed interval")
        require(submitted < window, f"request {request_id}: admitted after submission deadline")
        require(received >= last_received, "response records are not in coordinator receive order")
        last_received = received
        require(submitted >= last_client_received[client], f"client {client}: overlapping requests")
        last_client_received[client] = received
        if request_id < concurrency:
            require(client == request_id and submitted == 0, "initial cohort must arrive at start")
        q, s, end_to_end = started - submitted, finished - started, received - submitted
        require(request["queue_ns"] == q, f"request {request_id}: queue duration mismatch")
        require(request["service_ns"] == s, f"request {request_id}: service duration mismatch")
        require(request["end_to_end_ns"] == end_to_end, f"request {request_id}: end-to-end duration mismatch")
        outstanding.append((submitted, received))
        services.append((started, finished))
        queue.append(q)
        service.append(s)
        delivery.append(received - finished)
        latencies.append(end_to_end)
        in_window += received < window

    require(ids == set(range(len(requests))), "request IDs are not contiguous; lost or extra requests")
    peak_outstanding = max_occupancy(outstanding, concurrency, "in-flight requests")
    require(peak_outstanding == concurrency, "requested concurrency was not reached by initial cohort")
    peak_service = max_occupancy(services, workers, "search workers")
    latencies.sort()
    for p in (50, 95, 99):
        require(sample[f"latency_p{p}_ns"] == percentile(latencies, p), f"p{p} mismatch")
    require(sample["requests_completed_in_window"] == in_window, "in-window completion count mismatch")
    rps = len(requests) * 1e9 / elapsed
    qps = rps * batch
    window_rps = in_window * 1e9 / window
    close(sample["requests_per_second"], rps, "RPS including drain")
    close(sample["logical_queries_per_second"], qps, "logical QPS including drain")
    close(sample["requests_per_second_in_window"], window_rps, "in-window RPS")
    occupancy = sum(service) / elapsed
    require(occupancy <= workers + 1e-9, "service occupancy exceeds search worker count")
    return {
        "axis": axis, "run_id": run_id, "source": str(source), "scan": scan,
        "batch": batch, "concurrency": concurrency, "sample": sample_id,
        "search_threads": workers, "successful_requests": len(requests),
        "successful_logical_queries": len(requests) * batch,
        "requests_per_second": rps, "logical_queries_per_second": qps,
        "requests_per_second_in_window": window_rps,
        "latency_p50_ms": percentile(latencies, 50) / 1e6,
        "latency_p95_ms": percentile(latencies, 95) / 1e6,
        "latency_p99_ms": percentile(latencies, 99) / 1e6,
        "median_queue_ms": statistics.median(queue) / 1e6,
        "median_service_ms": statistics.median(service) / 1e6,
        "median_response_delivery_ms": statistics.median(delivery) / 1e6,
        "submission_window_seconds": window / 1e9,
        "elapsed_including_drain_seconds": elapsed / 1e9,
        "drain_seconds": (elapsed - window) / 1e9,
        "requests_completed_in_window": in_window,
        "requests_completed_during_drain": len(requests) - in_window,
        "max_in_flight_requests": peak_outstanding, "max_overlapping_searches": peak_service,
        "mean_search_service_occupancy": occupancy,
        "all_results_match_reference_reported_by_benchmark": True,
    }


def audit_report(path, axis, args):
    raw = path.read_bytes()
    report = json.loads(raw)
    require(report["mode"] == "in_process_closed_loop", "report is not a concurrency diagnostic")
    config = report["config"]
    require(config["points"] == args.expected_points and config["dim"] == args.expected_dim,
            "dataset size/dimension differs from requested validation fixture")
    require(config["search_threads"] == args.expected_workers, "worker budget differs")
    close(config["duration_secs"], args.expected_duration_secs, "configured duration")
    samples = integer(config["samples"], "configured samples", 1)
    if axis == "concurrency":
        require(config["batches"] == [1], "C sweep must fix B=1")
        require(len(config["concurrency"]) == len(LEVELS) and set(config["concurrency"]) == set(LEVELS),
                "C sweep levels must be exactly 1,4,16,32,64,128")
    else:
        require(config["concurrency"] == [4], "B sweep must fix C=4")
        require(len(config["batches"]) == len(LEVELS) and set(config["batches"]) == set(LEVELS),
                "B sweep levels must be exactly 1,4,16,32,64,128")
    query_count, reference_hash = canonical_reference(report, config)
    require(query_count >= 128, "query pool must support B128 and match the other sweep")
    rows, observed = [], set()
    for sample in report["samples"]:
        context = f"{path.name}: {sample.get('scan')} B{sample.get('batch')} C{sample.get('concurrency')} sample {sample.get('sample')}"
        try:
            row = audit_sample(sample, config, query_count, axis, path.stem, path)
        except (AuditError, KeyError, TypeError) as error:
            raise AuditError(f"{context}: {error}") from error
        key = row["scan"], row["batch"], row["concurrency"], row["sample"]
        require(key not in observed, f"{context}: duplicate sample")
        observed.add(key)
        rows.append(row)
    expected = {
        (scan, 1 if axis == "concurrency" else level, level if axis == "concurrency" else 4, sample)
        for scan in SCANS for level in LEVELS for sample in range(samples)
    }
    require(observed == expected, f"{path.name}: missing or unexpected cases")
    fixture = {key: config[key] for key in ("points", "dim", "top", "filter_percent", "seed", "search_threads", "warmup")}
    fixture["query_count"] = query_count
    metadata = {
        "axis": axis, "run_id": path.stem, "source": str(path),
        "report_sha256": hashlib.sha256(raw).hexdigest(),
        "b1_reference_sha256": reference_hash, "fixture": fixture,
        "samples_per_case": samples, "audited_requests": sum(row["successful_requests"] for row in rows),
    }
    return metadata, rows


def trend_screens(rows, band_percent):
    groups = defaultdict(list)
    for row in rows:
        groups[row["axis"], row["run_id"], row["source"], row["scan"]].append(row)
    screens = []
    for (axis, run_id, source, scan), group in groups.items():
        by_level = defaultdict(list)
        for row in group:
            by_level[row["concurrency"] if axis == "concurrency" else row["batch"]].append(row)
        metrics = {
            level: {field: statistics.median(row[field] for row in level_rows)
                    for field in ("requests_per_second", "logical_queries_per_second", "latency_p95_ms",
                                  "median_queue_ms", "median_service_ms")}
            for level, level_rows in by_level.items()
        }
        screen = {"axis": axis, "run_id": run_id, "source": source, "scan": scan,
                  "aggregation": "median of sample summaries within this file and case only"}
        if axis == "concurrency":
            peak = max(value["requests_per_second"] for value in metrics.values())
            high = metrics[128]
            ratio = high["requests_per_second"] / peak
            high_range = [metrics[level]["requests_per_second"] for level in (16, 32, 64, 128)]
            high_range_floor_over_peak = min(high_range) / max(high_range)
            if ratio < 1 - band_percent / 100:
                description = (
                    f"C128 retains {ratio:.1%} of this run's observed peak RPS; flag the lower high-concurrency throughput for repeated runs before calling it a collapse."
                )
            elif high_range_floor_over_peak >= 1 - band_percent / 100:
                description = (
                    f"C128 retains {ratio:.1%} of this run's observed peak RPS and C16–C128 stays within a descriptive {band_percent:g}% band; this run is compatible with a throughput plateau."
                )
            else:
                description = (
                    f"C128 retains {ratio:.1%} of this run's observed peak RPS, but C16–C128 spans more than the descriptive {band_percent:g}% band; report the individual rates rather than labeling a plateau or collapse."
                )
            screen.update({
                "c128_rps_over_observed_peak": ratio,
                "c16_through_c128_min_rps_over_max": high_range_floor_over_peak,
                "c128_rps_over_c4": high["requests_per_second"] / metrics[4]["requests_per_second"],
                "c128_p95_ms": high["latency_p95_ms"], "c4_p95_ms": metrics[4]["latency_p95_ms"],
                "c128_median_queue_ms": high["median_queue_ms"],
                "c4_median_queue_ms": metrics[4]["median_queue_ms"],
                "c128_median_service_ms": high["median_service_ms"],
                "c4_median_service_ms": metrics[4]["median_service_ms"],
                "screening_band_percent": band_percent,
                "description": description,
            })
        else:
            screen.update({
                "b128_qps_over_b1": metrics[128]["logical_queries_per_second"] / metrics[1]["logical_queries_per_second"],
                "b128_rps_over_b1": metrics[128]["requests_per_second"] / metrics[1]["requests_per_second"],
                "b128_p95_ms": metrics[128]["latency_p95_ms"], "b1_p95_ms": metrics[1]["latency_p95_ms"],
                "description": "C is fixed at 4; compare logical QPS together with request latency. Larger B changes work per request, so lower RPS alone is not a throughput collapse.",
            })
        screens.append(screen)
    return screens


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--concurrency", nargs="+", type=Path, default=[])
    parser.add_argument("--batch", nargs="+", type=Path, default=[])
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--cpu-mask", default="2,3,4", help="operator-reported external taskset mask, not verified from JSON")
    parser.add_argument("--expected-points", type=int, default=1_000_000)
    parser.add_argument("--expected-dim", type=int, default=512)
    parser.add_argument("--expected-workers", type=int, default=3)
    parser.add_argument("--expected-duration-secs", type=float, default=10.0)
    parser.add_argument("--screening-band-percent", type=float, default=10.0)
    args = parser.parse_args()
    require(args.concurrency or args.batch, "supply at least one completed report")
    require(0 <= args.screening_band_percent < 100, "screening band must be in [0,100)")
    cpu_mask = [int(cpu) for cpu in args.cpu_mask.split(",")]
    require(len(cpu_mask) == len(set(cpu_mask)) == args.expected_workers,
            "declared CPU mask must contain one distinct logical CPU per configured worker")
    inputs = [(axis, path.resolve()) for axis, paths in (("concurrency", args.concurrency), ("batch", args.batch)) for path in paths]
    require(len({path for _, path in inputs}) == len(inputs), "duplicate input files")
    reports, rows = [], []
    for axis, path in inputs:
        try:
            metadata, audited = audit_report(path, axis, args)
        except (AuditError, KeyError, TypeError, ValueError, OSError) as error:
            raise AuditError(f"{path}: {error}") from error
        reports.append(metadata)
        rows.extend(audited)
    first = reports[0]
    for report in reports[1:]:
        require(report["fixture"] == first["fixture"], "input reports use different fixtures, query pools, warmups or worker budgets")
        require(report["b1_reference_sha256"] == first["b1_reference_sha256"], "cross-report ordered B1 IDs/score bits differ")
    rows.sort(key=lambda row: (row["axis"], row["run_id"], row["source"], row["scan"], row["batch"], row["concurrency"], row["sample"]))
    screens = trend_screens(rows, args.screening_band_percent)
    notes = [
        "One in-process scanner/request-pool diagnostic; not Qdrant HTTP, a distributed cluster, or the production server scheduler.",
        "The worker count is audited from configuration and overlapping service intervals. External CPU affinity is operator-reported, not present in the benchmark JSON.",
        "RPS and logical QPS use actual elapsed time including drain. In-window RPS is separate. Latency percentiles include the entire submitted cohort, including drain completions.",
        "Median queue, service and response delivery are computed from raw request timestamps. Service occupancy is wall-time overlap, not measured CPU utilization.",
        "With three fixed workers, C above three adds waiting work. Higher queue/p95 latency alone is not a throughput collapse; compare RPS and service duration at the same B.",
        "Per-request result equality was asserted by the benchmark before it wrote the report. The script verifies that success flag and compares saved B1 reference bits; raw measured result matrices are not in JSON.",
        "Every run, axis and sample remains separate. The B1/C4 overlap between sweeps is not silently pooled as a replicate.",
        "Trend screens are descriptive, not statistical significance tests. Repeat baseline/candidate runs in alternating order before assigning a speedup or collapse to an implementation.",
    ]
    analysis = {"audit_passed": True, "operator_reported_cpu_mask": cpu_mask,
                "reports": reports, "samples": rows, "within_run_trend_screens": screens, "notes": notes}
    prefix = args.output_prefix
    prefix.parent.mkdir(parents=True, exist_ok=True)
    Path(str(prefix) + ".json").write_text(json.dumps(analysis, indent=2) + "\n")
    with Path(str(prefix) + ".csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    lines = [f"Audit PASS: {len(reports)} reports, {len(rows)} samples, {sum(row['successful_requests'] for row in rows):,} requests.",
             "Throughput includes drain; latencies are milliseconds. Each line preserves file/run, axis and sample.", "",
             "axis | run | scan | B | C | sample | RPS | logical QPS | p50 | p95 | p99 | median queue | median service | drain s"]
    for row in rows:
        fields = [row["axis"], row["run_id"], row["scan"], str(row["batch"]), str(row["concurrency"]), str(row["sample"])]
        fields.extend(f"{row[key]:.3f}" for key in ("requests_per_second", "logical_queries_per_second", "latency_p50_ms",
                      "latency_p95_ms", "latency_p99_ms", "median_queue_ms", "median_service_ms", "drain_seconds"))
        lines.append(" | ".join(fields))
    lines.extend(["", "Descriptive within-run screens:"])
    lines.extend(f"{item['run_id']} / {item['axis']} / {item['scan']}: {item['description']}" for item in screens)
    lines.extend(["", *notes])
    text = "\n".join(lines) + "\n"
    Path(str(prefix) + ".txt").write_text(text)
    print(text, end="")


if __name__ == "__main__":
    try:
        main()
    except (AuditError, KeyError, TypeError, ValueError, OSError) as error:
        print(f"Audit FAILED: {error}", file=sys.stderr)
        sys.exit(1)
