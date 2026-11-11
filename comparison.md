# SQ8 plain-scan comparison

These tables compare native Qdrant with the combined patch in an in-process scan/request-pool diagnostic. The combined patch includes SIMD TopK filtering, shared blocked SQ8 scoring and AMX dispatch with portable fallbacks. They are not HTTP service or cluster benchmarks.

## How to read the tables

The fixture is 1,000,000 normalized 512D SQ8 vectors, dot distance, Top10, seed 42. Visible scans visit all IDs; filtered scans accept 90%. Each sweep uses one process, three search workers, and operator-reported affinity to logical CPUs 2, 3, 4. Each cell admits closed-loop requests for 10 seconds and then drains all outstanding work.

**QPS counts logical queries; RPS counts batch requests. QPS = RPS × B.** The primary sweep rates divide the complete submitted cohort by actual elapsed time including drain. Request p99 includes queueing, scorer/query preparation, scan/result construction and completion delivery, including requests that finish during drain. It is a percentile of whole batch requests, not individual queries within a batch.

Each r1/r2 value remains separate. **Mean QPS change (%) = 100 × (((patched QPS r1 + patched QPS r2) / 2) / ((native QPS r1 + native QPS r2) / 2) − 1).** This is a descriptive comparison of the two arithmetic mean run rates, not pooled throughput, a confidence interval, or evidence of statistical significance. p99 values are not averaged.

C is the maximum number of outstanding batch requests; it does not increase the fixed three-worker CPU budget. Higher queue latency alone does not establish a throughput collapse. The B1/C4 cell appears in both axes from separate runs and is never pooled across axes. Run labels identify repetitions, not simultaneous matched measurements.

The B1 path uses native per-query SQ8 scoring, including its existing SIMD; it does not execute the multi-query AMX kernel. A B1 difference therefore cannot be attributed to AMX acceleration. These runs occurred on a shared KVM VM, so drift and run order remain material limitations.

## Concurrency sweep — B = 1

### Visible

| C | Native QPS r1 | Native QPS r2 | Combined QPS r1 | Combined QPS r2 | Mean QPS change | Native p99 ms r1 | Native p99 ms r2 | Combined p99 ms r1 | Combined p99 ms r2 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 24.59 | 23.83 | 20.09 | 23.62 | -9.72% | 46.21 | 51.82 | 88.02 | 44.73 |
| 4 | 71.75 | 70.61 | 61.48 | 61.19 | -13.83% | 85.47 | 86.32 | 99.87 | 103.85 |
| 16 | 71.22 | 71.06 | 61.42 | 60.47 | -14.33% | 263.76 | 257.23 | 299.74 | 318.58 |
| 32 | 71.72 | 70.83 | 61.74 | 61.64 | -13.45% | 469.48 | 592.07 | 553.07 | 542.58 |
| 64 | 72.24 | 68.35 | 61.25 | 62.68 | -11.85% | 920.28 | 968.09 | 1,111.36 | 1,072.74 |
| 128 | 66.96 | 62.33 | 62.36 | 61.38 | -4.30% | 2,057.06 | 2,091.36 | 2,097.58 | 2,114.15 |

### Filtered

| C | Native QPS r1 | Native QPS r2 | Combined QPS r1 | Combined QPS r2 | Mean QPS change | Native p99 ms r1 | Native p99 ms r2 | Combined p99 ms r1 | Combined p99 ms r2 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 20.75 | 19.81 | 19.38 | 18.71 | -6.08% | 50.70 | 56.12 | 85.27 | 66.97 |
| 4 | 61.05 | 56.49 | 57.98 | 56.45 | -2.65% | 100.25 | 111.77 | 106.53 | 110.48 |
| 16 | 61.44 | 53.98 | 58.07 | 57.82 | +0.41% | 297.34 | 347.27 | 312.59 | 317.50 |
| 32 | 61.18 | 53.81 | 59.12 | 58.58 | +2.35% | 547.03 | 647.27 | 569.89 | 575.99 |
| 64 | 60.55 | 55.58 | 58.33 | 57.13 | -0.57% | 1,106.63 | 1,191.51 | 1,142.92 | 1,173.92 |
| 128 | 58.52 | 55.04 | 58.84 | 57.97 | +2.86% | 2,217.43 | 2,404.91 | 2,209.20 | 2,279.68 |

## Batch sweep — C = 4

### Visible

| B | Native QPS r1 | Native QPS r2 | Combined QPS r1 | Combined QPS r2 | Mean QPS change | Native p99 ms r1 | Native p99 ms r2 | Combined p99 ms r1 | Combined p99 ms r2 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 63.59 | 70.90 | 61.44 | 71.66 | -1.04% | 143.92 | 87.16 | 121.62 | 85.41 |
| 4 | 152.47 | 161.21 | 195.85 | 196.79 | +25.17% | 160.77 | 156.83 | 129.97 | 126.48 |
| 16 | 263.93 | 275.58 | 696.05 | 715.65 | +161.66% | 376.06 | 353.73 | 148.23 | 139.39 |
| 32 | 294.47 | 304.17 | 1,070.66 | 1,055.91 | +255.23% | 666.90 | 632.87 | 194.39 | 171.97 |
| 64 | 323.37 | 318.27 | 1,415.65 | 1,471.63 | +349.99% | 1,169.81 | 1,184.88 | 262.79 | 268.92 |
| 128 | 328.35 | 321.48 | 1,711.32 | 1,737.82 | +430.78% | 2,224.61 | 2,243.51 | 435.74 | 433.18 |

### Filtered

| B | Native QPS r1 | Native QPS r2 | Combined QPS r1 | Combined QPS r2 | Mean QPS change | Native p99 ms r1 | Native p99 ms r2 | Combined p99 ms r1 | Combined p99 ms r2 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 55.29 | 63.72 | 58.02 | 59.83 | -0.97% | 113.90 | 100.26 | 113.86 | 105.99 |
| 4 | 148.86 | 149.02 | 175.03 | 171.84 | +16.45% | 164.12 | 160.21 | 136.97 | 144.96 |
| 16 | 237.00 | 238.21 | 633.63 | 645.87 | +169.25% | 393.02 | 383.80 | 152.34 | 154.28 |
| 32 | 261.22 | 260.10 | 1,053.98 | 1,018.72 | +297.59% | 653.18 | 721.00 | 192.04 | 199.55 |
| 64 | 276.47 | 275.98 | 1,463.23 | 1,477.15 | +432.24% | 1,278.97 | 1,326.35 | 280.79 | 275.65 |
| 128 | 281.76 | 279.17 | 1,823.02 | 1,834.54 | +552.05% | 2,469.24 | 2,518.01 | 419.23 | 420.28 |

## Staged single-thread microbench

Each cell below is the median logical QPS of ten timed scans within that specific run. Native r1/r2/r3, TopK r1/r2, blocked r1/r2 and combined AMX r1/r2 remain separate; no micro run medians are pooled. Blocked includes TopK plus the shared SQ8 path; combined AMX adds AMX dispatch/fallback. The single execution thread was pinned to logical CPU 2.

Micro timing includes scan, scoring, TopK and result construction, while scorer/query preparation is measured separately and excluded from micro QPS. Sweep service includes preparation and queueing is included in end-to-end request latency. Micro QPS and sweep request latency therefore measure different intervals.

Chronological order: **native r1 → TopK r1 → TopK r2 → native r2**, followed later by **blocked r1 → combined AMX r1 → blocked r2 → combined AMX r2 → native r3**. Columns are grouped by implementation for reading, not chronological adjacency.

### Visible — median logical QPS

| B | Native r1 | Native r2 | Native r3 | TopK r1 | TopK r2 | Blocked r1 | Blocked r2 | Combined AMX r1 | Combined AMX r2 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 19.48 | 18.66 | 21.94 | 19.69 | 18.66 | 20.01 | 22.44 | 21.49 | 22.24 |
| 8 | 72.82 | 68.24 | 77.81 | 70.63 | 71.95 | 91.64 | 97.46 | 95.65 | 92.50 |
| 64 | 112.78 | 107.25 | 113.53 | 109.21 | 107.39 | 176.99 | 177.86 | 474.52 | 458.24 |

### Filtered — median logical QPS

| B | Native r1 | Native r2 | Native r3 | TopK r1 | TopK r2 | Blocked r1 | Blocked r2 | Combined AMX r1 | Combined AMX r2 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 17.85 | 16.90 | 20.01 | 17.24 | 16.83 | 18.01 | 20.41 | 19.19 | 19.93 |
| 8 | 72.97 | 69.52 | 77.89 | 70.79 | 68.34 | 89.08 | 94.49 | 93.62 | 93.62 |
| 64 | 122.72 | 116.87 | 121.99 | 120.88 | 120.05 | 189.85 | 184.87 | 477.91 | 471.31 |

## Correctness and evidence

Both input analyses passed their audits and contain every required cell. Within each measurement family, saved ordered B1 reference IDs and exact score bits match across builds. The renderer also checks fixture compatibility and cell/rate consistency; it does not rerun the underlying correctness checks.

Micro correctness checks run outside timing: B1 references and B1/B8/B64 preflight comparisons. Timed micro results are consumed but not individually revalidated. In the sweeps, the benchmark retains and checks every completed request's result after timing; raw JSON stores timestamps and a success flag, not the measured result matrices. The raw-data audit verifies that flag and the saved B1 references.

The wide CSV retains unrounded sweep QPS/RPS, p50/p95/p99, median queue/service, elapsed/drain times, counts and source-run names, plus every micro run's QPS and scan/preparation percentiles. Sweep p99 uses the complete submitted request cohort. Micro percentiles use nearest rank; with ten samples, micro p95 is the maximum sample, not a service SLO.

Audited sweep summary: [all-sweeps-analysis.json](analysis/all-sweeps-analysis.json). Audited micro summary: [micro-comparison-all-runs.json](analysis/micro-comparison-all-runs.json). Wide numeric table: [comparison-wide.csv](comparison-wide.csv). These summaries point to the raw reports. Measurement values remain unchanged; publication may replace machine-local paths with portable provenance labels.

No statistical-significance, isolated AMX B1 speedup, native-server concurrency, or distributed-cluster claim follows from these tables alone.
