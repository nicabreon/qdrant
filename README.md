# SQ8 plain-scan measurement evidence


[Complete comparison tables](comparison.md) · [Wide numeric CSV](comparison-wide.csv)

All requested sweeps are complete: native and the combined patch, two repetitions,
B1/C1–128 and C4/B1–128, visible and 90%-accepted filtered scans. The timestamp
and accounting audit passed 96 sweep cases covering 42,717 successful requests.
The nine micro runs contain 540 timed scans; all configurations and saved B1
reference result bits match within each measurement family.

The combined patch improves large-batch throughput on this fixture. At B64 the
single-thread AMX stage reaches 458–478 logical QPS versus 177–190 for the blocked
stage with the same TopK. At B1 there is no established improvement: in the visible
concurrency sweep the combined patch is lower than native at every tested C;
the separate batch sweep's B1 repetitions overlap. These observations must not
be attributed to AMX, which is not used for B1. SIMD TopK alone has no established
whole-scan gain. This is shared-VM scanner evidence, not server scalability proof.


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
