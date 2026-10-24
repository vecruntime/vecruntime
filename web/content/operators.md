---
layout: default
title: Supported operators
description: Which Spark physical operators VecRuntime runs on its own columnar engine, which it runs partly, and which fall back to Spark.
---

# Supported operators

VecRuntime walks the physical plan after Spark's planner and replaces an operator with its own only
when the operator's input already produces columnar batches of supported types **and** every
expression it carries compiles. Anything else is left as Spark's operator, with the reason recorded —
there is no silent fallback. The Vector Acceleration UI tab colours each node by the engine that runs
it and shows the reason on hover; `spark.vecruntime.explainFallback.enabled=true` logs it.

Every converted operator has a `spark.vecruntime.exec.<name>.enabled` switch (all default **on**);
`spark.vecruntime.enabled=false` turns the whole engine off.

The detailed page — every requirement, config key and exact fallback string — is the
[detailed operators page](https://github.com/vecruntime/vecruntime/blob/main/docs/operators.md).

---

## Supported types

The engine computes on these lanes: `boolean`; `int`/`date`; `bigint`/`timestamp`/`timestamp_ntz` and
`decimal(p ≤ 18)`; `double`; `string` and `binary`; and `decimal(p > 18)` (a 128-bit lane, carried,
computed, grouped, sorted and joined). Nested `array`, `map` and `struct` columns are **carried** by
filters, projections, sorts, joins, unions, limits, the window and the shuffle, but an operator that
*computes* on one still falls back on the type. `float`, `short` and `byte` have no lane.

## What runs

| Spark operator | Status | Main conditions |
|---|---|---|
| `FilterExec` | native | Columnar child; the condition compiles. Lane-less columns are forwarded, not refused. |
| `ProjectExec` | native | Columnar child; every computed expression compiles with a supported result type. Bare column/literal slots of any type pass through. |
| `HashAggregateExec` (all modes) | native | Columnar child; `count`/`sum`/`min`/`max`/`avg`/`first`/`last`/`bit_*`, `FILTER`, keys of supported types. Decimal sum/avg past 18 digits run in 128 bits. |
| `SortExec` | native | Columnar child only (not over Spark's row shuffle); keys of any lane type, or array/struct by their order-preserving bytes. Spills to disk past `sort.spillBytes`. |
| `TakeOrderedAndProjectExec`, `LocalLimitExec`, `GlobalLimitExec`, `CollectLimitExec` | native | Columnar child; `OFFSET` falls back. Global limit needs a columnar (single-partition) input. |
| `UnionExec`, `CoalesceExec` | native | At least one columnar child; pass-through, reads nothing. |
| `ExpandExec` (`ROLLUP`/`CUBE`/`GROUPING SETS`) | native | Columnar child; every slot a column reference or a literal of a lane type. |
| `GenerateExec` | native | `explode`/`posexplode` (± `_outer`) over an array column with a lane element type. Other generators (`inline`, `stack`, `json_tuple`, UDTFs) and explode-over-map fall back. |
| `SampleExec` | native | Columnar child, without replacement. Poisson (with replacement) falls back. |
| `WindowExec`, `WindowGroupLimitExec` | native | Ranking, whole-partition, running and `ROWS`/`RANGE` offset frames; decimal aggregates over whole-partition and running frames. Falls back: sliding frames of functions other than sum/avg/count/min/max, decimals in sliding frames, `RANGE` offsets over decimal/timestamp/string keys, `IGNORE NULLS`, double keys. |
| `RangeExec` (`spark.range`) | native | Leaf; Spark's exact semantics and partitioning. Streaming range falls back. |
| `BroadcastHashJoinExec`, `ShuffledHashJoinExec`, `SortMergeJoinExec` | native | Equi keys of a lane type (or array/struct by bytes), never double build-side keys. Inner/outer/semi/anti/existence; full outer on the shuffled/merge paths only. Sort-merge runs as a hash rewrite or a real merge join (`sortMergeJoin.mode`). Hash/merge joins spill; broadcast joins refuse a build side past `join.maxBuildSize`. |
| `BroadcastNestedLoopJoinExec` | native | Non-equi inner/cross/semi/anti/existence/outer (build-right or build-left as noted). Full outer, and an outer join whose preserved side is the broadcast one, fall back. |
| `BroadcastExchangeExec`, `ShuffleExchangeExec` | native | Columnar broadcast; the `vecruntime-shuffle` jar (or Comet's native shuffle) carries the exchange with no row conversion. Otherwise Spark's exchange stays. |
| `MergeRowsExec` (`MERGE INTO`) | native | Columnar child; today gated by the merge join carrying the struct `_partition`. |
| `ObjectHashAggregateExec` | native | `bloom_filter_agg`, `collect_list`, `collect_set` (Spark's own object per group). Percentiles, `collect_top_k` fall back. |
| `FileSourceScanExec` (Parquet) | native scan | `VectorParquetScanExec` reads flat and nested columns straight into Arrow buffers. On by default; off when Comet's scan is active. A file it cannot decode fails over to Spark's reader per file. |
| `BatchScanExec` (Iceberg) | native scan | `VectorIcebergScanExec` reads unencrypted Parquet data files, applying position/deletion-vector/equality deletes natively; other formats/encrypted files use Iceberg's reader inside the node. |
| Spark/Comet/Iceberg columnar scans | columnar input | Counted as inputs, not conversions — the engine builds on top of them. |

## Not implemented / falls back

These keep Spark's own operator, with the same result:

- **`SortAggregateExec`** (a string `min`/`max` buffer) — planned, not yet converted.
- **`BatchScanExec`** over DSv2 sources beyond Iceberg (Delta, Hudi) — verified per source; unknown
  columnar sources already work, copied once per batch.
- **Structured Streaming** operators, `CartesianProductExec`, pickled (non-Arrow) **Python UDFs**,
  and command/DDL operators — not planned (a decision, not an omission).
- **Arrow-based Python UDF** operators — not yet converted.
- Any operator over a **lane-less column** it must read (an aggregate, join, sort, window or exchange
  over a `float`, `array`, `map` or `struct` it computes on) — records `unsupported column type …`.
- A scan whose reader is **not columnar** (row-based Parquet reader, CSV/JSON/text/Avro/JDBC, a cache
  holding a string/date/decimal/nested column) — nothing above it converts until a shuffle.

The full matrix, with the `Planned` and `Not planned` lists, is on the
[detailed operators page](https://github.com/vecruntime/vecruntime/blob/main/docs/operators.md).
