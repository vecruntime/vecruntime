---
layout: default
title: Compatibility matrix (tested against Spark)
description: What runs on vecruntime and what falls back, taken row by row from the ported DataFusion Comet test matrices.
---

# Compatibility matrix (tested against Spark)

Each row is a test that runs the same query **with vecruntime on and off** and compares the results
with Spark's — identical rows (doubles at tolerance `1e-9`, since SIMD reductions reorder
floating-point additions), and the expected vecruntime operator asserted in the final post-AQE plan.
A row marked **✓** means the query ran on a vecruntime operator and matched Spark; a row marked
**falls back** means the planner declined it, the query stayed on Spark, and the test asserts the
recorded fallback reason. A **known gap** is a case the suite marks `ignore`.

The rows below come **only** from the five ported test suites named in each section (and, for the
`range()` leaf, from its own suite) — nothing is inferred for a case that is not tested. For the full per-operator and per-expression reference (every
condition and every fallback reason), see [Supported operators](operators.html) and
[Supported expressions](expressions.html); this page is the tested-against-Spark subset.

Every matrix here is **ported from Apache DataFusion Comet**
([github.com/apache/datafusion-comet](https://github.com/apache/datafusion-comet)); each section
names the upstream Comet suite it follows, from the port's own header comment.

---

## Expressions

Ported from Apache DataFusion Comet's `CometBitwiseExpressionSuite`, `CometMathExpressionSuite` and
`CometExpressionSuite`. Every case runs a `SELECT` over a mixed Parquet table and asserts the
projection ran on `VectorProjectExec`, in **non-ANSI (legacy) arithmetic** so overflow and
divide-by-zero rows null rather than raise.
Source: [`VectorPortedCometExprSuite.scala`](https://github.com/vecruntime/vecruntime/blob/main/spark/src/test/scala/io/vecruntime/spark/VectorPortedCometExprSuite.scala) (#497).

| Expression family | Functions / operators covered | Accelerated | Notes |
|---|---|:---:|---|
| Bitwise logical | `&` `\|` `^` `~`, column–column, column–literal, literal–column | ✓ | INT32 (`i`) and INT64 (`l`) lanes; nulls in `l` pass through as Spark's |
| Bit shifts | `shiftleft` `shiftright` `shiftrightunsigned`, `<<` `>>` `>>>` | ✓ | literal and column amounts; shift amount masked to lane width (33→1 on int); `>>>` computed in 32 bits, sign bit exercised over negatives |
| Bit count | `bit_count` | ✓ | widened to a long over int and long; `bit_count(CAST(-1 AS INT))` = 64, per Spark |
| Sign / magnitude | `abs`, `signum` | ✓ | over int, long, double; `abs(NaN)=NaN`, `abs(-Inf)=+Inf`, `signum(-0.0)=-0.0` |
| Unary transcendental | `sqrt` `cbrt` `exp` `expm1` `sin` `cos` `tan` `asin` `acos` `atan` `sinh` `cosh` `tanh` `asinh` `acosh` `atanh` `cot` `degrees` `radians` | ✓ | bit-identical to Spark; NaN / ±Inf / null propagated over the `d` column |
| Logarithms | `ln` `log10` `log2` `log1p` `log(base, x)` | ✓ | **null (not NaN)** at or below the asymptote, matching Spark |
| Binary math | `pow`/`power` `atan2` `hypot` `log(base,x)` | ✓ | signed-zero folding (`atan2(0.0,-0.0)`); `pow(NaN,0)=pow(Inf,0)=1` |
| Modulo / selection / rounding | `%` `pmod` `greatest` `least` `rint` | ✓ | `pmod` non-negative for negatives; `greatest`/`least` — NaN is greatest, nulls ignored; a **zero divisor nulls the row** in non-ANSI mode |
| Composition | `CASE` over bitwise + math results | ✓ | mixed expression tree stays on `VectorProjectExec` |

*ANSI overflow / divide-by-zero raises are a separate suite's concern (the golden files under
`nonansi/`); these cases pin the arithmetic, not the raise.*

---

## Casts

Ported from Apache DataFusion Comet's `CometCastSuite` — one test per `(from, to)` pair over
generated columns with edge values (min/max, zero, NaN, ±Infinity, nulls, valid/invalid string
spellings). A `CAST` is a projection, so **✓** = the result matched Spark and a `VectorProjectExec`
is in the plan; a target with no lane in this project falls back. Session zone fixed to UTC.
Source: [`VectorPortedCometCastSuite.scala`](https://github.com/vecruntime/vecruntime/blob/main/spark/src/test/scala/io/vecruntime/spark/VectorPortedCometCastSuite.scala) (#500).

The grid, from-type (rows) × to-type (columns). ✓ = accelerated and equal to Spark; **fb** = falls
back with a reason; **n/a** = not exercised as a cast pair by the suite.

| from ↓ / to → | int | bigint | double | boolean | string | date | timestamp | decimal(≤18) | float | binary |
|---|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| **int** | ✓ (identity) | ✓ | ✓ | ✓ | ✓ | n/a | n/a | ✓ | n/a | n/a |
| **bigint** | ✓ (legacy wraps) | n/a | ✓ | ✓ | ✓ | n/a | n/a | ✓ | n/a | n/a |
| **double** | ✓ (legacy trunc / ANSI in-range) | ✓ | ✓ (identity) | ✓ | ✓ | n/a | n/a | ✓ | **fb** | n/a |
| **boolean** | ✓ | ✓ | ✓ | n/a | ✓ | n/a | n/a | n/a | n/a | n/a |
| **string (numeric)** | ✓ (legacy: invalid→null) | ✓ | ✓ | n/a | n/a | n/a | n/a | **fb** | n/a | **fb** |
| **string (bool)** | n/a | n/a | n/a | ✓ (legacy: else null) | n/a | n/a | n/a | n/a | n/a | n/a |
| **string (date)** | n/a | n/a | n/a | n/a | n/a | ✓ (invalid→null) | ✓ (invalid→null) | n/a | n/a | n/a |
| **date** | n/a | n/a | n/a | n/a | ✓ | n/a | ✓ | n/a | n/a | n/a |
| **timestamp** | n/a | n/a | n/a | n/a | ✓ | ✓ | n/a | n/a | n/a | n/a |
| **decimal(≤18)** | ✓ | ✓ | ✓ | n/a | n/a | n/a | n/a | ✓ | n/a | n/a |

**Notes.**
- **Narrowing** (`bigint→int`, `double→int/bigint`): legacy wraps/truncates toward zero (NaN→0, ±Inf
  saturates) and matches Spark's null path; ANSI is run over an **in-range** column so no raise is
  expected (the ANSI raise itself is out of scope).
- **Boolean:** `v != 0`, NaN is true; `boolean→number` is 1/0.
- **To string:** Java `toString`, including the `NaN`/`Infinity`/`-Infinity` spellings.
- **String→number/date/timestamp:** Spark's own parser per row; invalid spellings null in legacy.
- **Decimal(≤18):** rescale-and-check; a value out of range nulls in legacy (`ci→decimal(18,2)`,
  `cl→decimal(18,0)`, `cd→decimal(10,2)`).
- **Fallbacks (contract, not a wrong answer):** `→ float` — `unsupported cast target float`;
  `string → decimal(10,2)` — `unsupported cast string -> decimal(10,2)` (Spark's `Decimal.fromString`
  not reproduced yet); `→ binary` — `unsupported cast target binary`.
- `tinyint`/`smallint` are deliberately **not** a clean cast-target fallback here: #327 compiles
  `int → byte/short` onto the int lane via `NarrowIntExpr`, so they are not asserted in this grid.

---

## Aggregates

Ported from Apache DataFusion Comet's `CometAggregateSuite`. Every case runs the aggregate twice and
asserts a `VectorHashAggregateExec` in the accelerated plan — all cases in this suite are expected to
stay on our operator.
Source: [`VectorPortedCometAggregateSuite.scala`](https://github.com/vecruntime/vecruntime/blob/main/spark/src/test/scala/io/vecruntime/spark/VectorPortedCometAggregateSuite.scala) (#501).

| Aggregate / shape | Grouped | Ungrouped | Accelerated | Notes |
|---|:--:|:--:|:--:|---|
| `count(DISTINCT c)`, single column | ✓ | ✓ | ✓ | int and string (with nulls); Spark's distinct rewrite (Expand) compiles onto our operator |
| `count(DISTINCT c1, c2)`, multi-column | ✓ | ✓ | ✓ | with `count(*)` alongside |
| `count` `sum` `min` `max` `avg` | ✓ | ✓ | ✓ | int, bigint (nullable `l`), double; one and several keys, incl. a string key |
| `min`/`max` over strings and booleans | ✓ | ✓ | ✓ | with `bool_and`/`bool_or` |
| `min`/`max` floating point | ✓ | — | ✓ | negative zero and NaN ordering follows Spark |
| `first` / `last` | ✓ | ✓ | ✓ | with and without `IGNORE NULLS` (`first(l,true)`) |
| `bit_and` / `bit_or` / `bit_xor` | ✓ | ✓ | ✓ | over int and bigint |
| `sum`/`avg`/`min`/`max` over `decimal(15,2)` | ✓ | ✓ | ✓ | within the 18-digit lane; nulls, all-null measure, empty (ungrouped) input |
| `count_if`, aggregate `FILTER (WHERE …)` | ✓ | ✓ | ✓ | |
| group-by on variable-length (string) keys | ✓ | — | ✓ | several aggregates over the key |

*A **grouped** aggregate over zero rows is folded to `EmptyRelation` by AQE and has no operator to
assert — deliberately not tested. An ungrouped aggregate over zero rows still emits one row and is
tested.*

---

## Joins

Ported from Apache DataFusion Comet's `CometJoinSuite`, over Comet's two-column `(k, v)` tables built
so keys repeat on both sides. Each case asserts the specific vector join operator, or a fallback with
its reason.
Source: [`VectorPortedCometJoinSuite.scala`](https://github.com/vecruntime/vecruntime/blob/main/spark/src/test/scala/io/vecruntime/spark/VectorPortedCometJoinSuite.scala) (#507).

| Join operator | Join types covered | Build side | Filter | Accelerated | Notes |
|---|---|---|---|:--:|---|
| Broadcast hash (`VectorBroadcastHashJoinExec`) | inner, right outer | left & right | none, non-equi | ✓ | the shapes Spark broadcasts |
| Shuffled hash (`VectorShuffledHashJoinExec`) | inner, left, right, full, semi, anti | left & right | none, non-equi | ✓ | `preferSortMergeJoin=false` picks it at this size |
| Sort-merge → hash rewrite (`VectorShuffledHashJoinExec`) | inner, left, right, full, semi, anti | — | none, non-equi | ✓ | `sortMergeJoin.mode=hash`; the `SortMergeJoinExec` and its two sorts are gone |
| Real merge join (`VectorSortMergeJoinExec`) | inner + three outer, composite, nullable | — | equi | ✓ | `mode=merge`; timestamp keys, `(string, ts)` composite, nullable-ts key |
| NOT IN null-aware anti (`VectorBroadcastHashJoinExec`) | anti | broadcast | subquery | ✓ | all four null regimes; all-null / empty build folded to `EmptyRelation` under AQE (operator asserted with AQE off) |
| LEFT ANTI, non-null-aware (`VectorBroadcastHashJoinExec`) | anti | broadcast | equi | ✓ | nulls on both sides |
| Broadcast nested loop (`VectorBroadcastNestedLoopJoinExec`) | inequality inner, cross, left outer/semi/anti, right outer | broadcast | inequality / none | ✓ | NULL key operand → predicate unknown |
| Nested loop, literal condition | inner, left/right outer, semi | broadcast | `ON true` / `ON false` | ✓ | `ConstBoolExpr` (#513 for `ON true`); `false`/semi may be pruned by Spark, rows compared only |
| Boolean literal predicate / projection | — | — | `WHERE true`, `true`/`false` columns | ✓ | Spark prunes `WHERE true`; rows/constant column compared |
| Full outer (all-null side, filtered) | full | shuffled hash | equi + side filter | ✓ | count grouped by a nullable full-outer key (with `VectorHashAggregateExec`) |
| **Full outer nested loop** | full outer | broadcast | inequality | **falls back** | `full outer nested loop join` — needs a matched bitmap over the shared broadcast side |
| **Nested loop, preserved broadcast side** | left outer | broadcast (preserved side) | inequality | **falls back** | `preserved side broadcast` |

---

## Window functions

Ported from Apache DataFusion Comet's `CometWindowExecSuite`, over Comet's `(a, b, c)` fixture plus a
`decimal(10,2)` measure `d` (partitions, an order key with ties, a null partition, a null value).
Each case asserts a `VectorWindowExec`, or a fallback with its reason.
Source: [`VectorPortedCometWindowSuite.scala`](https://github.com/vecruntime/vecruntime/blob/main/spark/src/test/scala/io/vecruntime/spark/VectorPortedCometWindowSuite.scala) (#510).

| Window function / frame | Covered | Accelerated | Notes |
|---|---|:--:|---|
| `count(*)` no frame; `sum`/`avg`/`min`/`max` with `PARTITION BY` (± `ORDER BY`) | whole-partition & ordered | ✓ | null partition is its own group; null value skipped by sum/avg/min/max, counted by `count(*)` |
| Decimal `sum`/`avg`, whole partition & running `ROWS UNBOUNDED PRECEDING…CURRENT ROW` | decimal(10,2) | ✓ | ours since #259; RANGE default too |
| Decimal `avg` over a running `ROWS` frame, native decimal column | decimal(10,2) | ✓ | ours since #513 (sees through Spark's `Cast(Divide(…))` wrapper) |
| `ROWS BETWEEN` frame family — `UNBOUNDED PRECEDING…CURRENT ROW`, `CURRENT ROW…UNBOUNDED FOLLOWING`, `n PRECEDING…m FOLLOWING`, `n PRECEDING…CURRENT ROW`, `CURRENT ROW…n FOLLOWING`, `UNBOUNDED…UNBOUNDED`, preceding/following-only | sum/count/avg/min/max | ✓ | empty frames near partition edges included |
| `row_number` `rank` `dense_rank` `percent_rank` `ntile` `cume_dist` | with `PARTITION BY` + `ORDER BY` | ✓ | ASC and DESC order keys with ties |
| `lag` / `lead` | default offset, offset+default, beyond-partition default | ✓ | literal default and default null |
| `first_value`, `last_value` over a `ROWS` frame, `nth_value` | offset functions | ✓ | |
| **`RANGE` frame with a value offset** (`RANGE BETWEEN 5 PRECEDING AND CURRENT ROW`) | — | **falls back** | `RANGE frames with value offsets` |
| **`RANGE` frame, lower bound `FOLLOWING`** | — | **falls back** | `RANGE frames with value offsets` |
| **`IGNORE NULLS` on `lag`/`lead`** | — | **falls back** | `IGNORE NULLS not supported` |
| **`stddev` over a sliding frame** | — | **falls back** | `over a sliding frame not supported` |
| **Decimal `sum` over a sliding `ROWS` frame** (`1 PRECEDING AND CURRENT ROW`) | — | **falls back** | `over decimals in a sliding frame not supported` |

**Analysis errors, not fallbacks (both engines raise the same):** a non-literal `lag`/`lead` offset
(`lag(b, c)`) is a Spark analysis error; `nth_value` without an `ORDER BY` is a Spark analysis error.
The suite asserts both engines raise, so these are neither accelerated nor a recorded fallback.

---

## Range (`spark.range`, the `range()` table-valued function)

Not a Comet port: `VectorRangeExec` replaces Spark's `RangeExec` leaf, so every shape below runs the
same `range()` with the plugin on and off and compares the rows -- and, since the leaf decides the
partitions, the partition-dependent results too. Each case asserts `VectorRangeExec` in the final
plan, that Spark's `RangeExec` is gone, and that no `RowToColumnarExec` sits above ours.
Source: [`VectorRangeSuite.scala`](https://github.com/vecruntime/vecruntime/blob/main/spark/src/test/scala/io/vecruntime/spark/VectorRangeSuite.scala)
(lines 67-291) and, for the batch lifecycle without a Spark job,
[`VectorRangeIteratorSuite.scala`](https://github.com/vecruntime/vecruntime/blob/main/spark/src/test/scala/org/apache/spark/sql/vecruntime/VectorRangeIteratorSuite.scala).

| Shape | Covered | Accelerated | Notes (suite lines) |
|---|---|:--:|---|
| `range(n)`, `range(start, end)`, positive and negative steps, explicit `numSlices` (1, 4, 32 -- more slices than rows) | 12 ranges, plus 7-row batches (`spark.sql.inMemoryColumnarStorage.batchSize=7`) | ✓ | `numOutputRows` equals Spark's count (67-92) |
| Empty ranges (`start = end`, step against the direction) | 5 shapes | ✓ | an RDD with no partitions, `UnknownPartitioning(0)` as Spark's (94-109) |
| Ranges ending at `Long.MaxValue` / starting at `Long.MinValue`, one and several slices, huge steps | 13 ranges | ✓ | the last partition's end is clamped as `RangeExec.getSafeMargin` clamps it; for a step so large that a 1000-row batch of it overflows a Long, Spark's *generated* range loses the last partition's rows (`range(MinValue, MaxValue, MaxValue / 2)` returns 2 rows under codegen, 5 without) -- ours returns the 5, compared against Spark with codegen off, and the suite pins Spark's 2 so the divergence is visible when it changes (111-150) |
| The per-partition split | 11 `(start, end, step, slices)` shapes, every slice | ✓ | `VectorRangeExec.partition` against `RangeExec.doExecute`'s arithmetic element by element (152-193) |
| Filter, projection, ungrouped and grouped aggregate, a filter dropping every row, `LIMIT` over one slice | — | ✓ | `VectorFilterExec` / `VectorProjectExec` / `VectorHashAggregateExec` directly on the leaf (195-209) |
| `ORDER BY id` (sort elided by the planner), `GROUP BY id` (no exchange: the leaf is range-partitioned on `id`) | — | ✓ | `outputOrdering` / `outputPartitioning` are Spark's; `RangePartitioning(id, 4)`, `SinglePartition` for one slice (211-225) |
| `monotonically_increasing_id()` over a range, with a filter, with empty partitions | — | ✓ | same partition prefixes and row numbers as over Spark's leaf (227-243) |
| `spark_partition_id()` over a range | — | ✓ (leaf) | the expression is not compiled: the projection stays Spark's over a `ColumnarToRow` of our leaf; the values name the same partitions (227-243) |
| Joins with a range on both sides: shuffled hash, sort-merge (`mode=merge`), broadcast, join + aggregate | inner, left outer | ✓ | `VectorShuffledHashJoinExec`, `VectorSortMergeJoinExec`, `VectorBroadcastHashJoinExec` (245-267) |
| **`numSlices < 1`** | planner unit test | **falls back** | `range with 0 slices` -- Spark raises its own execution error (269-290) |
| **Streaming range** | planner unit test | **falls back** | `streaming range` (269-290) |
| `spark.vecruntime.exec.range.enabled=false` | switch | Spark's leaf | Spark's `RangeExec` and Spark's partial aggregate stay; only the Final aggregate over the shuffle is ours, behind a `RowToColumnarExec` (269-290) |
