---
layout: default
title: How it works
description: How VecRuntime converts a Spark plan to columnar operators, and the pieces behind them.
---

# How it works

VecRuntime is a Spark session extension. A planner rule (`VectorColumnarRule`) walks the
physical plan bottom-up and replaces a Spark operator with a columnar one when its child is
already columnar — a vectorized Parquet scan, a Comet scan, or another VecRuntime operator —
and every expression compiles to the kernel IR. Everything else stays Spark's, and the
reason is recorded on the node (`spark.vecruntime.explainFallback.enabled=true` logs it).

Each operator takes a columnar child in and produces Arrow-layout batches out. The SIMD
kernels run over Arrow `MemorySegment`s with the Java Vector API; output vectors are wrapped
as Spark `ColumnarBatch`es for the next operator. Between two of our operators a filter or
projection need not compact: when at least half the rows survive it forwards the child's
columns with a selection bitmap, and the consumer folds the bitmap into its own work.
Compaction happens once, at the boundary back to Spark.

## Semantics follow Spark

Results equal Spark's, including the corners: NaN-safe double ordering (`NaN = NaN`, NaN
sorts last, `-0.0 = 0.0`), three-valued `AND`/`OR`, `WHERE` treating null as false, and ANSI
mode (Spark 4's default). Double division by zero raises `DIVIDE_BY_ZERO`; integer
arithmetic that needs ANSI overflow checks falls back. ANSI errors are raised only for rows
Spark would have evaluated, because the predicate kernels skip blocks with no live row.

## The pieces

- **Aggregation.** The partial aggregate emits exactly Spark's buffer schema, so any
  exchange and final aggregate run unchanged. Group ids come from a hash table; a
  low-cardinality dictionary-encoded string key is memoised so a batch probes it at most once
  per distinct key. Past `spark.vecruntime.agg.spillThreshold` a partial aggregate emits its
  table and starts over, a final one spills into hash buckets and merges them one at a time,
  the budget arbitrated by Spark's task memory manager.
- **Sort.** Every batch is copied into native memory and radix-sorted key by key, least
  significant first, in runs of `spark.vecruntime.sort.runRows`; runs past
  `spark.vecruntime.sort.spillBytes` spill to local disk as Arrow IPC and merge back on
  output, so a partition larger than the budget still sorts.
- **Joins.** Broadcast and shuffled hash joins, an order-preserving merge join over Spark's
  sorted inputs, and broadcast nested-loop joins. A sort-merge join becomes the hash rewrite
  where a side's statistics fit the budget and the row order cannot show, the merge join
  otherwise (`spark.vecruntime.exec.sortMergeJoin.mode`, `auto` by default). Keys of
  int/long/boolean/string/date/byte/short/decimal and double (by bits); double keys as bits
  because Spark normalises NaN and zero first.
- **Window.** The ranking functions, offset functions, and whole-partition, running and
  sliding aggregate frames (`ROWS`, and `RANGE` with value offsets). The operator does not
  spill yet: a whole-partition frame holds its partition until it ends.
- **Decimals.** A decimal of ≤ 18 digits travels as its unscaled value in long lanes; a 19–38
  digit decimal travels as two long limbs on a 128-bit lane — filtered, projected, arithmetic,
  keys, and decimal `sum`/`avg` all on our operators in both stages.

The [How it works detail on GitHub](https://github.com/vecruntime/vecruntime/blob/main/docs/how-it-works.md)
carries the kernel-level notes, the Vector API lessons, and the module layout.

## The Vector Acceleration tab

`spark.plugins` attaches a **Vector Acceleration** tab to the Spark UI
(`spark.vecruntime.ui.enabled`). It lists every SQL execution and how much of it was
accelerated, and draws each plan as a DAG coloured by engine — green for our kernels, blue for
Comet's native operators, purple for the zero-copy hand-off to Comet, teal for a vectorized
Spark scan, grey for anything left to Spark. A **Fully Accelerated** badge shows when no
operator is grey. Operators the rule tried but could not convert are listed with the reason
it recorded.

## Columnar shuffle and Comet

With the `vecruntime-shuffle` jar and its shuffle manager, a shuffle exchange above one of
our operators becomes a columnar exchange: a map task writes one Arrow IPC record batch per
reduce partition into Spark's own data-file layout, and a reducer fetches remote blocks over
Arrow Flight (one `DoGet` per remote executor), one batch live at a time. It is a
`ShuffleExchangeLike`, so AQE's coalescing, skew splitting and local reads all apply. See
[Flight shuffle](flight-shuffle.html).

With Comet on the classpath, VecRuntime can sit directly on Comet's native Parquet scan
(zero copy) and feed Comet's native shuffle through one bridge node that exports each column
over the Arrow C Data Interface. See [Comet as the scan](comet.html).
