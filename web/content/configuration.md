---
layout: default
title: Configuration
description: The VecRuntime keys a user sets, grouped, with their defaults; the full reference lists every key.
---

# Configuration

Every key is a Spark session config (`--conf` or `spark.conf.set`). This page lists the ones
a user actually sets; defaults are from
[`VectorConf.scala`](https://github.com/vecruntime/vecruntime/blob/main/spark/src/main/scala/io/vecruntime/spark/VectorConf.scala).
Each operator also has its own `spark.vecruntime.exec.<op>.enabled` switch (all default `true`
except `localTableScan`); the per-operator table is in the full reference. For every key —
the optimizer rewrites, the internal memory and tuning knobs — see the
[Configuration reference on GitHub](https://github.com/vecruntime/vecruntime/blob/main/docs/configuration.md).

## Main switch

| Key | Default | What it does |
|---|---|---|
| `spark.vecruntime.enabled` | `true` | The whole plugin. `false` leaves the plan entirely to Spark. |
| `spark.vecruntime.explainFallback.enabled` | `false` | Log why each operator was left to Spark. |
| `spark.vecruntime.exec.strictFloatingPoint` | `true` | Double `sum`/`avg` round exactly like Spark (one accumulator per group, in order). `false` is faster but can differ in the last bits. |

## Columnar shuffle

Takes effect only with the `vecruntime-shuffle` jar and
`spark.shuffle.manager=org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager`.

| Key | Default | What it does |
|---|---|---|
| `spark.vecruntime.shuffle.enabled` | `true` | Our columnar shuffle exchange over Arrow IPC / Arrow Flight. |
| `spark.vecruntime.shuffle.backend` | `flight` | How a reducer fetches a remote map output: `flight` (one Flight server per executor), `block` (Spark's block transfer — use this under `spark.ssl.rpc.enabled`, where Flight has no TLS). |
| `spark.vecruntime.shuffle.compression` | `zstd` | Record-batch body compression: `zstd` (native), `lz4`, or `none`. |

## Memory and spill

| Key | Default | What it bounds |
|---|---|---|
| `spark.vecruntime.agg.spillThreshold` | `1g` | One grouped aggregate table; past it a partial aggregate restarts and a final one spills into buckets. `0` = never spill. |
| `spark.vecruntime.sort.spillBytes` | `1g` (no cap when off-heap memory is enabled) | The sort's runs; past it they spill to local disk and merge back. `0` = never spill. |
| `spark.vecruntime.join.spillBytes` | `256m` | A shuffled hash join's build side before it buckets to disk. `0` = never. |
| `spark.vecruntime.join.maxBuildSize` | per-core off-heap share, else `1g` | The largest build side the hash joins hold in memory per task. |
| `spark.vecruntime.sort.runRows` | `1048576` | Rows per sorted run (the sort's scratch is bounded by the run). |

## Joins

| Key | Default | What it does |
|---|---|---|
| `spark.vecruntime.exec.sortMergeJoin.mode` | `auto` | What a sort-merge join becomes: `auto` (hash rewrite where a side's statistics fit and the order cannot show, the merge join otherwise), `merge`, `hash`, or `off`. The boolean `...sortMergeJoin.enabled` is an alias (`true`→`auto`, `false`→`off`). |
| `spark.vecruntime.join.spillBuckets` | `32` | Buckets a split shuffled hash join writes each side into and joins one at a time. |

## Parquet

The native reader and writer are on by default; see [Apache Parquet](parquet.html) for the
full set and what each falls back on.

| Key | Default | What it does |
|---|---|---|
| `spark.vecruntime.scan.nativeParquet.enabled` | `true` (off when Comet's scan is active) | The native Parquet reader. `false` uses Spark's vectorized reader under our operators. |
| `spark.vecruntime.write.columnar.enabled` | `true` | The columnar Parquet writer. `false` uses Spark's writer. |
| `parquet.filter.columnindex.enabled` | `false` | Parquet-java's own key: column-index page skipping in the native reader. |

## Iceberg

All on by default; set to `false` to turn a path off. See [Apache Iceberg](iceberg.html).

| Key | Default | What it does |
|---|---|---|
| `spark.vecruntime.scan.nativeIceberg.enabled` | `true` | Native read of an Iceberg table's data files. |
| `spark.vecruntime.iceberg.writer.enabled` | `true` | Columnar write (INSERT, CTAS, copy-on-write). |
| `spark.vecruntime.iceberg.dvWriter.enabled` | `true` | Merge-on-read deletion-vector / position-delete writer. |

## Comet

See [Comet as the scan](comet.html).

| Key | Default | What it does |
|---|---|---|
| `spark.vecruntime.comet.shuffle.enabled` | `true` | Feed Comet's native shuffle from our operators when Comet's shuffle is configured. |
| `spark.vecruntime.comet.mixed.enabled` | `false` | Allow Comet's native operators above ours through the sink leaf. |
| `spark.vecruntime.comet.preferComet` | empty | Allowlist of operator kinds offered to Comet first (empty = none requested). |

## UI

| Key | Default | What it does |
|---|---|---|
| `spark.vecruntime.ui.enabled` | `true` | Attach the Vector Acceleration tab to the Spark UI. |
| `spark.vecruntime.ui.retainedExecutions` | `100` | Queries that tab keeps. |

## JVM system properties

Set in `extraJavaOptions`, not as Spark confs:

| Property | What it does |
|---|---|
| `vecruntime.vectorBits=128\|256\|512` | Force a vector shape (default: the platform's preferred one). |
| `vecruntime.platform=neon\|sve\|avx2\|avx512` | Override the probed SIMD platform the kernels dispatch on. |

The full list — the optimizer rewrites, read-ahead and decode knobs, the shuffle internals,
and every default and unit — is the
[Configuration reference on GitHub](https://github.com/vecruntime/vecruntime/blob/main/docs/configuration.md).
