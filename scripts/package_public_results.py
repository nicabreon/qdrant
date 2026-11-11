#!/usr/bin/env python3
"""Package completed full-size scan measurements; never run during timed work.

Usage: python3 package_public_results.py --input-dir EVIDENCE --output-dir PUBLIC
Optional --analysis FILE arguments accept reviewed derived JSON/CSV files only.
The output directory must not exist. No Git, network, subprocesses or dependencies.
"""

import argparse
import csv
import hashlib
import io
import json
import re
import shutil
import tempfile
from pathlib import Path


VARIANTS = ("baseline", "topk", "blocked", "amx")
LEVELS = (1, 4, 16, 32, 64, 128)
MICRO_NAME = re.compile(r"(baseline|topk|blocked|amx)-r([1-9][0-9]*)\.json\Z")
JSON_STRING = re.compile(r'"(?:[^"\\]|\\.)*"')
OUTPUT_VALUE = re.compile(r'"output"\s*:\s*("(?:[^"\\]|\\.)*"|null)')
PRIVATE_PATH = re.compile(r"(?:/(?:home|Users|root|tmp|var|mnt|media)/|file:/{2}|[A-Za-z]:\\)")
IDENTITY_KEYS = {"host", "hostname", "host_name", "user", "username", "user_name", "home", "cwd", "workspace", "working_directory"}
CONFIG_KEYS = {
    "points", "dim", "batches", "top", "filter_percent", "samples", "warmup", "seed",
    "output", "require_cycles", "bench", "concurrency", "search_threads", "duration_secs",
    "query_pool", "result_budget_mib",
}
CPU_FIELDS = {
    "Architecture", "CPU op-mode(s)", "Address sizes", "Byte Order", "CPU(s)",
    "On-line CPU(s) list", "Vendor ID", "Model name", "CPU family", "Model",
    "Thread(s) per core", "Core(s) per socket", "Socket(s)", "Stepping", "Flags",
    "Virtualization", "Hypervisor vendor", "Virtualization type", "L1d cache",
    "L1i cache", "L2 cache", "L3 cache", "NUMA node(s)",
}
RECORDED_LEGACY_REVISIONS = {
    "baseline": ("d6c63678d", "environment.json benchmark_commit"),
    "topk": ("856817eae", "recorded build revision in task session; no per-run metadata was captured"),
    "blocked": ("9eddb223c", "progress.json blocked_binary_commit"),
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def read_stable(path):
    require(path.is_file() and not path.is_symlink(), f"Expected regular input file: {path.name}")
    before = path.stat()
    data = path.read_bytes()
    after = path.stat()
    require((before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns),
            f"Input changed while reading: {path.name}")
    return data


def parsed(data):
    def reject_constant(value):
        raise ValueError(f"Non-finite JSON number: {value}")
    return json.loads(data, parse_constant=reject_constant)


def json_bytes(value):
    return (json.dumps(value, indent=2, allow_nan=False) + "\n").encode()


def public_text(data, name):
    text = data.decode("utf-8")
    require(not PRIVATE_PATH.search(text), f"Private/local path remains in {name}")
    return text


def check_strings(value, allowed=None):
    if isinstance(value, dict):
        for key, item in value.items():
            require(key.lower() not in IDENTITY_KEYS, f"Identity field is not publishable: {key}")
            check_strings(item, allowed)
    elif isinstance(value, list):
        for item in value:
            check_strings(item, allowed)
    elif isinstance(value, str):
        require(not PRIVATE_PATH.search(value), "Private path remains in JSON value")
        require(not value.startswith("/"), "Absolute path remains in JSON value")
        if allowed is not None:
            require(value in allowed, "Unexpected string field in raw benchmark report")


def report_data(path, axis):
    original = read_stable(path)
    report = parsed(original)
    require(set(report) <= {"mode", "config", "b1_reference_results", "samples"},
            f"Unknown report fields: {path.name}")
    config = report["config"]
    require(set(config) <= CONFIG_KEYS, f"Unknown configuration fields: {path.name}")
    require(config["points"] == 1_000_000 and config["dim"] == 512,
            f"Not the full-size fixture: {path.name}")
    require(type(config["samples"]) is int and config["samples"] > 0, "Invalid sample count")
    require(config["top"] == 10 and config["filter_percent"] == 90 and config["seed"] == 42,
            f"Unexpected fixture settings: {path.name}")
    if axis == "micro":
        require(report.get("mode") is None and not config.get("concurrency"), "Expected microbenchmark")
        require(config["batches"] == [1, 8, 64], "Unexpected microbenchmark batches")
        expected = {(scan, batch, sample) for scan in ("visible", "filtered")
                    for batch in (1, 8, 64) for sample in range(config["samples"])}
        actual = [(s["scan"], s["batch"], s["sample"]) for s in report["samples"]]
        reference_count = 64
    else:
        require(report["mode"] == "in_process_closed_loop", "Expected closed-loop sweep")
        require(config["search_threads"] == 3 and config["duration_secs"] == 10,
                "Unexpected sweep worker budget or duration")
        batches, concurrency = (([1], list(LEVELS)) if axis == "c" else (list(LEVELS), [4]))
        require(config["batches"] == batches and config["concurrency"] == concurrency,
                "Unexpected sweep axes")
        expected = {(scan, batch, clients, sample) for scan in ("visible", "filtered")
                    for batch in batches for clients in concurrency for sample in range(config["samples"])}
        actual = [(s["scan"], s["batch"], s["concurrency"], s["sample"]) for s in report["samples"]]
        reference_count = max(config["query_pool"], max(config["batches"]))
        require(reference_count >= 128, "Sweep query pool is too small")
        require(all(s["all_results_match_reference"] is True and s["requests"]
                    for s in report["samples"]), "Sweep result validation was not successful")
    require(len(actual) == len(expected) and set(actual) == expected,
            f"Incomplete or duplicate sample grid: {path.name}")
    references = report["b1_reference_results"]
    require(len(references) == 2 and {r["scan"] for r in references} == {"visible", "filtered"},
            "Incomplete reference scan modes")
    require(all(len(r["queries"]) == reference_count for r in references), "Incomplete B1 query references")
    portable_output = "raw/" + path.name
    text = original.decode("utf-8")
    matches = list(OUTPUT_VALUE.finditer(text))
    require(len(matches) == 1, f"Expected exactly one config.output token: {path.name}")
    match = matches[0]
    require(json.loads(match.group(1)) == config["output"], "Output token does not match config.output")
    published = (text[:match.start(1)] + json.dumps(portable_output) + text[match.end(1):]).encode()
    clean = parsed(published)
    # Deep equality plus byte splicing preserves every numeric token and all sample order.
    report["config"]["output"] = portable_output
    require(clean == report, "Unexpected change to raw report")
    check_strings(clean, {portable_output, "visible", "filtered", "in_process_closed_loop"})
    return original, published, report


def portable_command(command, output, variant):
    require(isinstance(command, list) and all(isinstance(arg, str) for arg in command), "Invalid command argv")
    require(len(command) >= 6 and Path(command[0]).name == "taskset" and command[1] == "-c",
            "Unexpected recorded command wrapper")
    require(re.fullmatch(r"[0-9]+(?:,[0-9]+)*", command[2]), "Unexpected affinity mask")
    require(Path(command[3]).name == "sq8-" + variant, "Recorded executable does not match variant")
    result = ["taskset", *command[1:3], "bin/sq8-" + variant, *command[4:]]
    require(result.count("--output") == 1, "Expected exactly one --output option")
    index = result.index("--output") + 1
    require(index < len(result), "Missing --output argument")
    result[index] = output
    check_strings(result)
    return result


def reproduction_command(config, output, variant, axis):
    command = ["taskset", "-c", "2" if axis == "micro" else "2,3,4", "bin/sq8-" + variant]
    fields = ("points", "dim", "batches", "top", "filter_percent", "samples", "warmup", "seed",
              "concurrency", "search_threads", "duration_secs", "query_pool", "result_budget_mib")
    for field in fields:
        if field in config and config[field] != []:
            value = config[field]
            value = ",".join(map(str, value)) if isinstance(value, list) else str(value)
            command.extend(("--" + field.replace("_", "-"), value))
    if config.get("require_cycles"):
        command.append("--require-cycles")
    command.extend(("--output", output))
    return command


def metadata_data(root, path, report, axis, hash_artifacts):
    metadata_path = root / (path.stem + ".metadata.json")
    label = MICRO_NAME.fullmatch(path.name)
    variant = label.group(1) if label else ("baseline-concurrency" if "-baseline-" in path.name else "amx")
    metadata_source_hash = None
    if metadata_path.exists():
        source_bytes = read_stable(metadata_path)
        source = parsed(source_bytes)
        metadata_source_hash = sha256(source_bytes)
        require(source["variant"] in (*VARIANTS, "baseline-concurrency"), "Unknown metadata variant")
        # A later baseline micro repeat may use the baseline-concurrency executable in micro mode.
        require(source["variant"] == variant or (variant == "baseline" and source["variant"] == "baseline-concurrency"),
                "Report/metadata variant mismatch")
        variant = source["variant"]
        metadata = {field: source.get(field) for field in (
            "variant", "source_revision", "binary_sha256", "started_utc", "finished_utc",
            "cross_build_ordered_ids_and_score_bits_match",
        )}
        require(metadata["started_utc"] and metadata["finished_utc"], "Per-run metadata is not complete")
        metadata["command"] = portable_command(source["command"], "raw/" + path.name, variant)
        metadata["metadata_status"] = "captured by measurement runner; command paths made portable"
        metadata["source_revision_provenance"] = "per-run metadata"
    else:
        require(axis == "micro" and variant in RECORDED_LEGACY_REVISIONS,
                f"Missing required per-run metadata: {metadata_path.name}")
        revision, provenance = RECORDED_LEGACY_REVISIONS[variant]
        metadata = {
            "variant": variant, "source_revision": revision, "source_revision_provenance": provenance,
            "binary_sha256": None, "command": None, "started_utc": None, "finished_utc": None,
            "cross_build_ordered_ids_and_score_bits_match": None,
            "metadata_status": "reconstructed from recorded session/environment; missing contemporaneous fields remain null",
        }
        if hash_artifacts:
            binary = root / ("sq8-" + variant)
            require(binary.is_file() and not binary.is_symlink(), "Missing preserved legacy executable")
            # This hash describes the artifact present at packaging time, not its identity at measurement time.
            digest = hashlib.sha256()
            before = binary.stat()
            with binary.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            after = binary.stat()
            require((before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns), "Executable changed while hashing")
            metadata["current_preserved_artifact_sha256"] = digest.hexdigest()
            metadata["current_preserved_artifact_label"] = "bin/sq8-" + variant
            metadata["artifact_hash_provenance"] = "hashed at packaging time; not a contemporaneous measurement hash"
    require(re.fullmatch(r"[0-9a-f]{7,40}", metadata["source_revision"]), "Invalid source revision")
    if metadata["binary_sha256"] is not None:
        require(re.fullmatch(r"[0-9a-f]{64}", metadata["binary_sha256"]), "Invalid executable hash")
    metadata["reproduction_command"] = reproduction_command(report["config"], "raw/" + path.name, variant, axis)
    metadata["reproduction_command_note"] = "expanded from recorded configuration; not claimed to be the original invocation"
    check_strings(metadata)
    return metadata, metadata_source_hash


def environment_data(root):
    source = parsed(read_stable(root / "environment.json"))
    cpu = parsed(source["cpu"]) if isinstance(source["cpu"], str) else source["cpu"]
    fields = {entry["field"].rstrip(":"): entry["data"] for entry in cpu["lscpu"]}
    cpu_public = {key: value for key, value in fields.items()
                  if key in CPU_FIELDS or re.fullmatch(r"NUMA node[0-9]+ CPU\(s\)", key)}
    rustc_lines = source["rustc"].splitlines()
    rustc = {"version": rustc_lines[0]}
    rustc_keys = {"commit-hash": "commit_hash", "commit-date": "commit_date", "host": "target",
                  "release": "release", "LLVM version": "llvm_version"}
    for line in rustc_lines[1:]:
        key, sep, value = line.partition(": ")
        if sep and key in rustc_keys:
            rustc[rustc_keys[key]] = value
    result = {
        "cpu": cpu_public,
        "toolchain": {"rustc": rustc, "cargo_profile": source["profile"],
                      "cargo_incremental": source["cargo_incremental"]},
        "affinity": {"micro_logical_cpus": [2], "sweep_logical_cpus": [2, 3, 4],
                     "provenance": "operator-recorded taskset masks; JSON reports do not independently prove affinity"},
        "cpu_cycles_available": source["cpu_cycles_available"],
    }
    check_strings(result)
    return result


def derived_data(path, replacements):
    original = read_stable(path)
    if path.suffix == ".json":
        def replace(match):
            value = json.loads(match.group())
            return json.dumps(replacements[value]) if value in replacements else match.group()
        data = JSON_STRING.sub(replace, original.decode("utf-8")).encode()
        check_strings(parsed(data))
    elif path.suffix == ".csv":
        rows = list(csv.reader(io.StringIO(original.decode("utf-8"))))
        require(rows and not (set(cell.lower() for cell in rows[0]) & IDENTITY_KEYS),
                "Empty CSV or identity-bearing CSV header")
        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer)
        for row in rows:
            portable_row = [replacements.get(cell, cell) for cell in row]
            check_strings(portable_row)
            writer.writerow(portable_row)
        data = buffer.getvalue().encode()
    else:
        raise ValueError("Optional derived artifacts must be reviewed JSON or CSV; plots require a separate publication review")
    public_text(data, path.name)
    return original, data


README = """# SQ8 plain-scan measurement evidence

This package contains the full 1,000,000 × 512D reports: every microbenchmark
sample, ordered B1 reference ID/score-bit pair, and every sweep request timestamp.
Tiny protocol checks, executables, build logs and private workspace files are excluded.

`raw/` reports retain their original bytes except the single `config.output`
string, which becomes a portable `raw/<name>.json` label. Numeric tokens, array
order, result bits and timestamps are unchanged. `manifest.json` records both
original and published SHA-256 hashes and the exact transformation. Any included
derived analysis retains pre-sanitization report hashes; match those to the
manifest's `original_sha256`, not the sanitized report's hash.

`metadata/` records the variant, source revision, timing metadata and portable
command where captured. Earlier manually launched micro runs have explicitly
partial reconstructed metadata: unknown command, timestamps and measurement-time
binary hash remain null. An optional `current_preserved_artifact_sha256` hashes
the executable retained at packaging time and does not prove its earlier identity.
Every run also has an expanded reproduction command reconstructed from its config.
`bin/` command labels describe executable names to build; binaries are not shipped.

Build each recorded source revision separately with Rust 1.98.0 and the `perf`
profile (CARGO_INCREMENTAL=0), using:

```sh
cargo +1.98.0 bench -p segment --bench sq8_full_scan --profile perf --no-run -j8
```

Use the executable path printed by Cargo, place it at the metadata's `bin/` label,
then run the recorded or reconstructed command from this package directory.
Use isolated target directories for source revisions to prevent stale incremental
artifact reuse. Keep the fixture, profile and available CPU affinity consistent.
Select an allowed CPU mask on a different host; the recorded masks are 2 for the
microbenchmark and 2,3,4 for sweeps. `environment.json` includes only whitelisted
CPU/model/topology/features, compiler/build profile and affinity information.

The microbenchmark measures one scanner on one logical CPU; query preparation is
recorded separately. Sweeps use three search workers and a coordinator, and include
query preparation in request service time. C denotes outstanding requests, B the
queries within each request. Throughput uses actual elapsed time including drain;
latencies include queueing and drain completions. These are in-process scanner
measurements, not HTTP, distributed-cluster or production-scheduler benchmarks.
Hardware CPU cycles were unavailable. This VM did not provide physical CPU isolation.
No native ARM performance claim is made. Retain all repeats and both axes when
interpreting results; the overlapping B1/C4 cases are not silently pooled.

The packager checks full-size fixtures, complete sample grids and ordered B1
cross-report equality separately for micro runs and sweeps. It does not replace
the timestamp/protocol audit. If included, `scripts/analyze_scan_sweeps.py` can
audit the published sweep reports. Derived analysis JSON/CSV files are optional;
plots require separate review and are not automatically copied.
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--analysis", action="append", default=[], type=Path)
    parser.add_argument("--hash-preserved-legacy-binaries", action="store_true")
    args = parser.parse_args()
    root = args.input_dir.resolve()
    destination = args.output_dir.absolute()
    require(not destination.exists(), "Refusing to replace an existing output directory")
    micro = sorted(path for path in root.iterdir() if MICRO_NAME.fullmatch(path.name))
    required_micro = {f"{variant}-r{repeat}.json" for variant in VARIANTS for repeat in (1, 2)}
    required_micro.add("baseline-r3.json")
    require(required_micro <= {path.name for path in micro}, "Missing one or more required full-size micro runs")
    inputs = [(path, "micro") for path in micro]
    inputs.extend((root / f"sweep-{axis}-{variant}-r{repeat}.json", axis)
                  for axis in ("b", "c") for variant in ("baseline", "amx") for repeat in (1, 2))
    files, records, references, replacements = {}, [], {}, {}
    for path, axis in inputs:
        original, published, report = report_data(path, axis)
        group = "micro" if axis == "micro" else "sweeps"
        if group in references:
            require(report["b1_reference_results"] == references[group], f"Cross-build B1 mismatch: {path.name}")
        else:
            references[group] = report["b1_reference_results"]
        raw_name = "raw/" + path.name
        files[raw_name] = published
        metadata, metadata_source_hash = metadata_data(root, path, report, axis, args.hash_preserved_legacy_binaries)
        metadata["packaging_verified_cross_report_ordered_b1_equality"] = True
        metadata_name = "metadata/" + path.stem + ".metadata.json"
        files[metadata_name] = json_bytes(metadata)
        replacements[str(path.resolve())] = raw_name
        replacements[str((root / (path.stem + ".metadata.json")).resolve())] = metadata_name
        records.append({"report": raw_name, "metadata": metadata_name, "axis": axis,
                        "original_sha256": sha256(original), "published_sha256": sha256(published),
                        "original_metadata_sha256": metadata_source_hash,
                        "sample_count": len(report["samples"]),
                        "transformation": "replace config.output string token only"})
    for variant in (*VARIANTS, "baseline-concurrency"):
        replacements[str((root / ("sq8-" + variant)).resolve())] = "bin/sq8-" + variant
    files["environment.json"] = json_bytes(environment_data(root))
    derived = []
    for requested in args.analysis:
        path = requested if requested.is_absolute() else root / requested
        name = "analysis/" + path.name
        require(name not in files, "Duplicate derived output name")
        original, published = derived_data(path, replacements)
        files[name] = published
        derived.append({"file": name, "original_sha256": sha256(original),
                        "published_sha256": sha256(published),
                        "transformation": "replace recognized report, metadata and executable paths only; CSV quoting may be normalized",
                        "report_hashes_refer_to": "original raw reports before config.output sanitization"})
    for name in ("analyze_scan_sweeps.py", "analyze_scan_micro.py"):
        auditor = root / name
        if auditor.exists():
            data = read_stable(auditor)
            public_text(data, auditor.name)
            files["scripts/" + auditor.name] = data
    files["scripts/package_public_results.py"] = read_stable(Path(__file__).resolve())
    files["README.md"] = README.encode()
    for name, data in files.items():
        public_text(data, name)
    manifest = {"format_version": 1, "reports": records, "derived_artifacts": derived,
                "raw_numeric_tokens_results_timestamps_unchanged": True,
                "cross_report_ordered_b1_equality": {"micro": True, "sweeps": True},
                "files": {name: {"sha256": sha256(data), "bytes": len(data)} for name, data in sorted(files.items())}}
    files["manifest.json"] = json_bytes(manifest)
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".scan-publication-", dir=destination.parent))
    try:
        for name, data in files.items():
            output = staging / name
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_bytes(data)
        require(not destination.exists(), "Output directory appeared during packaging")
        staging.rename(destination)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    print(f"Prepared {len(records)} full-size reports and {len(derived)} derived artifacts in {destination}")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError) as error:
        raise SystemExit(f"Packaging failed: {error}") from error
