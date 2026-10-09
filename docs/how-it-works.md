---
layout: default
title: How it works
---

# How it works

```
Driver                                           Executor (per batch)
------                                           --------------------
FilterExec        -> VectorFilterExec            ColumnarBatch -> VectorBuffers (adapter)
ProjectExec       -> VectorProjectExec           kernels over MemorySegment (compare, arith, reduce)
HashAggregateExec -> VectorHashAggregateExec     Arrow vectors -> ArrowColumnVector -> next operator
(Partial and Final)                              or a selection bitmap over the child's columns
Sort, Window, Expand, Generate, Limit, Sample,   the same contract: columnar child in, Arrow out
Union, joins (hash, nested loop, sort-merge)     (see "Supported today")
ShuffleExchange   -> VectorShuffleExchange       Arrow IPC record batches per reduce partition ->
(vecruntime-shuffle jar)                       Spark's data file; reducers fetch over Arrow Flight
ShuffleExchange   -> CometShuffleExchange        Arrow C Data export -> CometVector (zero copy)
(with Comet)         over VectorToComet
```

`VectorColumnarRule` runs in Spark's `preColumnarTransitions`, bottom-up. An operator is converted
when its child is already columnar with supported types (a vectorized Parquet scan, a Comet scan, or
another VecRuntime operator) and every expression compiles to the kernel IR. Otherwise the reason
is stored as a tree-node tag; `VectorFallback.reasons(plan)` lists them.

## The Vector Acceleration tab

`spark.plugins` also attaches a **Vector Acceleration** tab to the Spark UI (disable with
`spark.vecruntime.ui.enabled=false`). It lists every SQL execution and how much of it was accelerated:

![The Vector Acceleration tab listing TPC-H Q1 executions, each 89% accelerated](https://github.com/vecruntime/vecruntime/raw/main/images/vector-ui.png)

Per execution, it draws the final physical plan as a DAG with each operator coloured by the engine
that runs it. This is TPC-H Q1 over Comet's scan with Comet's shuffle between our Partial and Final
aggregates, taken before the columnar sort existed: the final `Sort` was the one operator left to
Spark, which is why the query shows 89% rather than the badge (with `VectorSortExec` it is fully
accelerated):

![The plan of one Q1 execution: Comet scan, Vector filter, project and aggregates, the bridge into Comet's shuffle, and Spark's Sort](https://github.com/vecruntime/vecruntime/raw/main/images/query-accel-details.png)

The colours:

| Colour | Engine | Counts as accelerated |
|---|---|---|
| green | our SIMD kernels (`VectorFilter`, `VectorProject`, `VectorHashAggregate`) | yes |
| blue | Comet's native operators (any class under `org.apache.spark.sql.comet` / `org.apache.comet`) | yes |
| purple | `VectorToComet`, the zero-copy hand-off to Comet's shuffle | yes |
| teal | a Spark scan that already emits batches, i.e. the vectorized Parquet reader | plumbing |
| grey, dashed | left to Spark | **no** |
| yellow | `ColumnarToRow` / `RowToColumnar`, the only nodes that convert between rows and batches | plumbing |
| light grey | `AQEShuffleRead` (adaptive execution's shuffle reader) and `ReusedExchange`/`ReusedSubquery`; they hand over whatever the exchange wrote, columnar when it is | plumbing |

A **Fully Accelerated** badge is shown when no operator is grey: every operator runs on the kernels
or on Comet, with only supported columnar sources and unavoidable row transitions around them. A
vectorized scan and a transition are plumbing rather than missed operators, so they do not block the
badge; a Spark exchange or sort does, because those are operators we do not implement (without
Comet's shuffle, any query with a stage boundary is therefore only partly accelerated).

Grey operators that the planner rule *tried* to convert are listed under "Why operators were not
accelerated" with the reason it recorded — the same information `spark.vecruntime.explainFallback.enabled`
logs, and it reads as a cascade, since one uncompilable expression makes every operator above it
non-columnar:

```
Filter          unsupported expression RLike: lineitem.l_comment RLIKE 'special.*requests'
Project         child Filter is not columnar
HashAggregate   child Project is not columnar
```

One conversion the colours alone would hide: Comet's JVM shuffle (`CometColumnarExchange`, used when
its native writer is not) reads its child through `execute()`, so over one of our operators it turns
batches into rows and re-encodes them as Arrow. The node stays blue (it is Comet's), but its tooltip
says so. Comet's native shuffle (`CometExchange`), which the plugin selects for every partitioning
it can bridge, including range partitioning for global sorts, takes the batches directly.

Node classification comes from the operator's identity, not from a tag: a `VectorExec` is ours, a
class in Comet's packages is Comet's, and anything we declined to convert is the original Spark class
carrying the fallback tag. The tab prefers the final plan objects (captured from
`SparkListenerSQLExecutionEnd`) and falls back to matching node names on the listener event's
serialised plan for queries still running, which it labels *approximate*.

Between two of our operators a filter (or projection) does not compact: when at least half of the
rows survive it forwards the child's columns with a selection bitmap (`SelectedColumnarBatch`), and
the consumer folds the bitmap into its validity masks or group assignment. Inside a predicate,
`AND`/`OR` evaluate their right operand only where the left one leaves the row undecided, so the
compare kernels skip 64-row blocks with no live row and ANSI errors are only raised for rows Spark
would have evaluated too. Compaction happens once, at the boundary to Spark.

Semantics follow Spark, including the corners: NaN-safe double ordering (`NaN = NaN`, NaN sorts
last), three-valued `AND`/`OR`, `WHERE` treating null as false, and ANSI mode (Spark 4's default):
double division by zero raises `DIVIDE_BY_ZERO`, while integer arithmetic under ANSI (which needs
overflow checks) falls back.

## Aggregation

The partial aggregate emits exactly Spark's buffer schema (`sum`, `count`, `min`, `max`,
`(sum, count)` for `avg`), so any exchange and Final aggregate run unchanged. Our own Final
aggregate merges those buffers per group (Spark's or ours) and evaluates the result expressions
with each aggregate's `evaluateExpression` substituted (`sum / count` for `avg`) through the
projection kernels; over Spark's row shuffle its input arrives through `RowToColumnarExec`, over
Comet's shuffle it is read zero copy. Without grouping keys, one buffer row per partition. With keys, a hash table assigns dense group ids across the
task's batches. When every key is a dictionary-encoded string (the usual case for low-cardinality
Parquet columns) the ids are memoised per combination of dictionary indices, so a batch probes the
table at most once per distinct key tuple. Rows are then scattered into per-group accumulators;
the alternative, one masked SIMD reduction per group, only wins for one group on 128-bit vectors
(`vecruntime.agg.maskPathMaxGroups` sets the cut-over: default 4 where the platform has mask
registers and the double species has at least 4 lanes, 1 otherwise). The scatter rotates over
`vecruntime.agg.interleave` independent accumulator copies (default 1 on AVX-512, 4 elsewhere) so
consecutive rows of the same group do not serialise on one `sum[g] += x` chain (+40% at 4 groups);
the price is that double sums are rounded in a different order than Spark's sequential loop (12th
significant digit on TPC-H Q1) -- strict mode uses one copy -- and that the accumulators take that
many times the memory (see "Memory tuning").

## Columnar shuffle (Arrow IPC over Arrow Flight)

With the `vecruntime-shuffle` jar on the classpath and
`spark.shuffle.manager=org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager`, the rule replaces
a `ShuffleExchangeExec` above one of our operators with `VectorShuffleExchangeExec` (#288) -- a
`ShuffleExchangeLike`, so AQE's coalescing, skew splitting and local reads apply -- for hash,
round-robin, single and range partitioning (range samples the child once with Spark's own
`RangePartitioner`), whenever every output column has a lane. Stage boundaries stop going through
`ColumnarToRowExec` / `RowToColumnarExec`, and the final aggregate, the sort and the merge join read
columns straight from the exchange.

**Write.** A map task computes the partition id per row with Spark's exact hash (`Pmod(Murmur3, n)`,
so a hash-partitioned side matches what Spark's own exchange would produce) and appends rows into one
Arrow builder per reduce partition and column. A partition is flushed as one IPC record batch at
`batchRows` (8192) or `batchBytes` (1 MB), or when the task holds more than `bufferBytes` (64 MB)
across partitions -- measured on the allocator, not estimated (#340). Each record batch is its own
IPC stream, so a string column is dictionary-encoded per batch and only when the dictionary pays
(`writer.dictionaryMaxRatio`, #356); a partition's batches may alternate between encodings. Bodies
are compressed with zstd through zstd-jni (native; Arrow's own zstd codec overran its buffer by up
to 8 bytes, GH-1116, hence `SafeZstdCodec`), or LZ4, or not at all. Up to 200 partitions each write
straight to a file; above that a heap staging buffer of `flushBytes` per partition is used. At the
end the streams are concatenated into Spark's data file with an index of `(offset, length, rows)`
per partition and committed through `IndexShuffleBlockResolver`, so `MapStatus`, the index file and
Spark's block transfer all work as they do for a row shuffle. AQE's `dataSize` is fed the
uncompressed Arrow bytes (compressed sizes made it mis-size broadcast sides, #355), and the write
time counts only the writer's own work. Byte and short columns travel as `Int(32)`, decimals up to
18 digits as `Int(64)`, wider ones as `Decimal128`; every field carries its Spark type as metadata.

**Read.** A reducer reads local blocks straight from the files into Arrow memory and remote ones
through the backend: `flight` (default) opens one `DoGet` per remote executor carrying all of that
executor's blocks for the reducer (#347), and the executor's Flight server streams the blocks' IPC
bytes back to back as raw 4 MB chunks (Flight's own record-batch framing would break the per-batch
dictionaries, #338); the client decodes them with the same reader the local path uses, one batch
live at a time. `block` uses Spark's block transfer over the same files. A remote fetch that fails is
raised as Spark's `FetchFailedException` with the block's map index, so the scheduler recomputes the
lost executor's map outputs instead of failing the query (#364); an Arrow out-of-memory stays a memory
error. Files are deleted when the shuffle is unregistered (#358 -- before it, never).

**The Flight server.** One per executor, started by the executor plugin on the block manager's host
and an ephemeral port (`flight.bindHost`, `flight.threads`), registered with the driver plugin and
looked up once per executor. Its security is described under the configuration table above: with
`spark.authenticate` the shuffle secret is a bearer token on every call; TLS is not wired (the server
refuses to start under `spark.ssl.rpc.enabled` -- use `backend=block` there). Both backends assume
executors that stay up for the job; a push-based service for disposable executors plugs into the
`VectorShuffleBackend` seam. Numbers: TPC-H SF10 0.59x the row shuffle over 22 queries; TPC-DS at
1 TB, all 103 queries, 1.08x Spark with our operators on Spark's Parquet reader and 1.19x with
Comet's scan feeding our operators and shuffle (`docs/results.md`).

## Comet shuffle

Comet's planner only gives a Comet shuffle to children it recognises, but at run time its native
shuffle writer accepts any columnar child whose batches hold `CometVector`s. `VectorToCometExec`
exports each of our columns through the Arrow C Data Interface, written directly with the FFM API
(two C structs and an upcall release stub; no `arrow-c-data`, no JNI on our side, which matters
because Comet ships that library's classes under their original names with shaded signatures), and
Comet's `ArrowImporter` wraps the same memory in a shaded vector, releasing ours when it is done.
The rule replaces a Spark exchange, or Comet's row-based columnar exchange, above one of our
operators with the native `CometShuffleExchangeExec` over the bridge, for hash, single, round-robin
and range partitioning (the last one when Comet's own
`spark.comet.shuffle.native.partitioning.range.enabled` is on: Comet samples the child for the
bounds with Spark's `RangePartitioner`, which is what Spark's exchange does too). Requires `spark.shuffle.manager` set to
Comet's shuffle manager and `spark.comet.exec.shuffle.enabled=true`; see [docs/comet.md](comet.html).

## Supported today

| Area | Supported | Falls back |
|---|---|---|
| Types | Int, Long, Double, Date, Timestamp, Boolean, String (strings pass through and serve as group keys), Decimal of at most 18 digits (unscaled long lanes); Decimal of 19 to 38 digits as a two-limb DECIMAL128 lane: filtered, projected, `+ - * /`, abs, negation and casts (#257/#258), grouping and join keys (#259), `CASE WHEN` results, scalar-subquery literals and `round` (#326); Byte and Short (TINYINT/SMALLINT) on INT32 lanes with the declared type on output -- casts in every mode, arithmetic narrowed, keys, the shuffle (#327); columns of any other type pass through a filter or projection untouched as Spark's vectors | Float, Binary, nested -- as computed values or keys |
| Strings | `instr`/`locate`/`position`, `replace`, `translate`, `substring_index`, `split_part`, `find_in_set` (one byte-search primitive); `upper`/`lower`/`initcap` (ASCII in lanes, the rest through Spark's own case mapping), `trim`/`ltrim`/`rtrim`/`btrim` with a literal trim set; `concat`, `concat_ws`, `elt` (lengths summed across the inputs, one buffer); `length`/`len`/`char_length`, `octet_length`, `bit_length`, `ascii`, `chr`/`char` (measured once per dictionary entry); `substring`/`substr`, `left`, `right`, `lpad`, `rpad`, `repeat`, `space`, `overlay` with Spark's code-point semantics (a two-pass UTF8 writer: lengths and source ranges, then one sized buffer); literals or lanes as arguments; a per-batch output cap declines a runaway `repeat`/`space` | collated columns; column trim sets and `translate` tables; regular expressions; binary and array subjects |
| Nested columns | struct, array and map columns pass through filters and projections as Spark's own vectors; struct fields (`st.a`, `st.c.d`) are read from the struct vector's children, with the struct's nulls | `arr[i]`, `map[key]`, building a struct/array/map, the array/map/lambda function families (#50) |
| Hashes | `hash` (Spark's exact Murmur3 chain), `xxhash64`, `md5`, `sha1`, `sha2`, `crc32` (per-row digests inside the operator) | binary columns; non-literal `sha2` bit lengths |
| Predicates | `=`, `!=`, `<`, `<=`, `>`, `>=` on numeric/date/decimal/string columns vs literal or column (strings in `UTF8_BINARY` order, once per dictionary entry on dictionary pages); `IN (literals)`; `LIKE` against a literal with any number of `%` wildcards (`'p%'`, `'%p%'`, `'%a%b%'`: a multi-token matcher) and `startswith` / `endswith` / `contains`; `InSet` (long `IN` lists, one binary search per row); scalar subquery results as literals (incl. Spark's merged struct-valued ones); Spark's runtime bloom-filter probe (`might_contain(filter, xxhash64(key))`, through Spark's own `BloomFilter`); `xxhash64`; `<=>`; `isnan`; `BETWEEN`; `AND`/`OR`/`NOT`; `IS [NOT] NULL`; boolean columns and their comparisons | `LIKE` with `_` or an escape character, `rlike`, an `IN` set holding `NULL`, functions |
| Arithmetic | `+ - *` on Int/Long/Double/Decimal (integers overflow-checked in ANSI mode, raising Spark's error only for rows that survive earlier filters), `/` on Double and Decimal (Spark's half-up rounding; null or ANSI error past the precision), wide decimals (`decimal(p > 18)`, #257 / #258) as two `long` limbs: comparisons, `IN`, `+ - * /`, `abs`, negation and casts to and from the lane with Spark's rescale and overflow semantics, the exact `BigInteger` path for a row whose intermediate leaves 128 bits, unary minus (ANSI-checked on integers), casts: widening, narrowing (Spark's truncation and saturation; ANSI `CAST_OVERFLOW` on active rows), decimals, booleans both ways, date <-> timestamp under a fixed offset, number/boolean/date/timestamp -> string and string -> number/boolean/date/timestamp with Spark's own parsers and formatters per row (ANSI `CAST_INVALID_INPUT` on active rows); `abs`, `sign`, the transcendental and trigonometric family (`exp`/`log*`/`pow`/`sqrt`/`cbrt`, trig, hyperbolic, `atan2`, `hypot`, `degrees`/`radians` -- bit-identical to Spark: the kernel makes exactly Spark's `Math`/`StrictMath` call per lane), `%` / `pmod` / `div` on Int/Long/Double (zero divisors raise in ANSI mode, null otherwise, active rows only), `try_add` / `try_subtract` / `try_multiply` / `try_divide` / `try_mod` / `try_cast` (the ANSI masks null the row instead of raising), `try_sum` (the whole group nulled on overflow, as Spark) and `try_avg`, `greatest` / `least`, `nanvl`, `ceil` / `floor` / `rint` / `round` / `bround` with Spark's exact definitions (incl. negative scales and decimals on the unscaled value), bitwise `& | ^ ~`, the three shifts and `bit_count` on Int/Long, widening casts, casts between decimals, integers and doubles, literal columns | `try_*` on decimals, `try_to_number` / `try_to_binary`, `%` / `div` on decimals, `round`-family functions and a string source for a cast over a wide decimal, casts to binary, intervals, nested types and the lane-less tinyint/smallint/float |
| Dates | `year`, `month`, `dayofmonth`, `dayofyear`, `quarter`, `dayofweek`, `weekday`, `extract`, `trunc(date, year/quarter/month/week)`, `date_add`, `date_sub`, `datediff`, `last_day`, `add_months`, `next_day`, `weekofyear`, `make_date`, `unix_date`/`date_from_unix_date`, `timestamp_seconds`/`millis`/`micros` and `unix_seconds`/`millis`/`micros`; `date_format`/`from_unixtime` with literal patterns (Spark's own formatter per row); `cast(timestamp AS date)`, `hour`, `minute`, `second`, `months_between`, `date_trunc`, `unix_timestamp`/`to_unix_timestamp` under a UTC or fixed-offset session zone | zone-dependent arithmetic under a zone with rules (DST) -- see the Not planned table in `docs/expressions.md` |
| Conditionals | `CASE WHEN ... [ELSE] END`, `IF`, `COALESCE`, `NVL`, `NVL2`, `NULLIF`, `IFNULL` over any supported type, with `NULL`, numeric, string and boolean literal branches; a null condition counts as false, later branches are evaluated only where earlier ones did not match | conditionals producing wide decimals or nested types |
| Aggregates | `sum` (ANSI bigint sums overflow-checked), `count`, `count_if`, `min`, `max` (incl. booleans and strings), `avg`, `first`/`last`, `bool_and`/`bool_or`, `bit_and`/`bit_or`/`bit_xor`, `max_by`/`min_by`, the statistical family (`stddev`/`variance` pop and samp, `skewness`, `kurtosis`, `covar_*`, `corr`, `regr_*`; Spark's Welford update and merge, agreement to a relative tolerance) in every aggregate mode (`Partial`, `PartialMerge`, `Final`, `Complete`), with `FILTER` clauses, `DISTINCT` (Spark's rewrites, incl. TPC-H Q16's `count(distinct)`) and keys-only aggregates (`SELECT DISTINCT`, `UNION`); `sum`/`avg` of decimals up to 8/11 digits through Spark's own rewrite to long/double sums, wider ones through 128-bit accumulators emitting Spark's own wide buffers (a decimal `avg`'s result is Spark's own division over the merged buffer); keys of Int/Long/Boolean/String/Date/Byte/Short/Decimal (wide DECIMAL128 included) and Double (compared by bits, as Spark's `NormalizeNaNAndZero` makes them); Spark's `SortAggregateExec` for string buffers converted too. Past `spark.vecruntime.agg.spillThreshold` a partial aggregate emits its table and starts over, a final one spills hash-partitioned and merges bucket by bucket, the budget arbitrated by Spark's task memory manager (#363, #367, #376). `ObjectHashAggregateExec`'s `collect_list`, `collect_set` and `bloom_filter_agg` are converted (#57): driven through Spark's own function object per group, so the partial buffer and result are byte-identical (a Spark Final or the runtime bloom probe reads them unchanged); their buffers are counted against the task budget and spill past `spark.vecruntime.agg.spillThreshold` (serialized buffers, UTF8-carried through the grace-hash path), so a grouped `collect_*` over many keys is bounded | `ObjectHashAggregateExec` functions we do not carry (`percentile_*`, `collect_top_k`, `listagg`, ...) |
| Sort | `SORT BY`/`ORDER BY` over a columnar child, every supported type as key, spilling past its budget (#416) | sorts over Spark's row shuffle (kept by Spark), spilling |
| Generate | `explode`, `posexplode`, `explode_outer`, `posexplode_outer` over an array column or a struct field of one (elements of any lane type; nulls, empty arrays and arrays longer than a batch) | `inline`, `stack`, `json_tuple`, generators over maps, user-defined generators, computed arrays |
| Windows | `row_number`, `rank`, `dense_rank` over `PARTITION BY ... ORDER BY ...` (one walk over the sorted input; a partition longer than a batch is one partition); whole-partition `sum`/`avg`/`count`/`min`/`max` and the rest of the aggregate family over non-decimal inputs (the default frame without `ORDER BY`, or `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`); running `sum`/`avg`/`count`/`min`/`max` (`ROWS` or `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, the latter the default with `ORDER BY`); the child may be Spark's row sort; the per-partition top-k Spark inserts under a `rank <= k` filter (`WindowGroupLimitExec`, both modes) ; `lag`/`lead` (literal offset and default), `first_value`/`last_value`/`nth_value` over the same frames ; `percent_rank`/`cume_dist`/`ntile`; sliding `sum`/`avg`/`count`/`min`/`max` over `ROWS BETWEEN a AND b` and over `RANGE BETWEEN a AND b` with value offsets (one integral or date order key, `ASC`/`DESC`, either null ordering; the frame kernels' two-pointer walk) | decimal window aggregates over sliding frames (#28), `IGNORE NULLS` (#58), `RANGE` offsets over decimal or timestamp keys |
| Joins | broadcast and shuffled hash joins: inner, left/right/full outer, left semi, left anti (including the null-aware anti join Spark plans for `NOT IN (subquery)` over nullable columns), existence (`EXISTS` as a value), each with an optional non-equi condition; keys of Int/Long/Boolean/String/Date/Byte/Short/Decimal (wide included) and Double (by bits); sort-merge joins as an order-preserving merge join over Spark's sorted inputs (`spark.vecruntime.exec.sortMergeJoin.mode`, `auto` by default, #286/#311); broadcast nested-loop joins | a columnar broadcast exchange (the build side is read from Spark's `HashedRelation`, once per executor) |


## Vector API lessons

Three things silently turned SIMD code into something slower than a scalar loop on this machine
(Apple M3, NEON, 128-bit vectors), and only JMH revealed them:

1. `Vector.compare(op, ...)` / `lanewise(op, ...)` is only intrinsified when `op` is a compile-time
   constant. Every kernel loop spells its operator out; the others are complements or swapped
   operands.
2. `Vector.compress(mask)` is native only on AVX-512/SVE. For species of up to 8 lanes we index a
   precomputed `VectorShuffle` table by the selection bits and use `rearrange`.
3. `VectorMask.fromLong` is slow on NEON. Masks are built with a broadcast-AND-compare against lane
   bit constants, and fully valid 64-row blocks skip masks entirely.

Three more came out of profiling TPC-H rather than microbenchmarks (see
[docs/results.md](results.html)):

4. Two 64-bit lanes are not worth a shuffle. Compacting doubles by `rearrange` on NEON lost to a
   scalar walk over the selection bits; 64-bit compaction uses the scalar walk when the species
   has two lanes, and every kernel bulk-copies 64-row blocks whose selection word is all ones.
5. Heap segments are slow inputs. Wrapping Spark's `double[]` with `MemorySegment.ofArray` instead
   of copying it into native memory doubled the filter's time; one copy into native memory wins.
6. Per-group masked reductions only pay off for one or two groups on 128-bit vectors; grouped
   aggregation scatters into per-group accumulators otherwise.
7. A lane nobody computes on still pays. The 128-bit decimal lane has no SIMD path (two-limb scalar
   loops throughout), yet carrying it through every operator moved TPC-DS from 65% to 73% of operators
   ours: the wins are the chains that stayed columnar around a wide column -- a `decimal(27,2)` running
   total no longer sends the window, the join on it and the aggregate above back to rows.

## Sort

`SortExec` over a columnar child becomes `VectorSortExec`: every batch of the partition is copied into
operator-owned native memory (Spark lets the producer reuse a batch once the next one is requested),
joined into one column per attribute, and a permutation is computed key by key, least significant
first. Each pass maps the key to an order-preserving unsigned 32- or 64-bit value (`int32` and
booleans in one pass, `int64` and doubles in one 64-bit pass, strings of at most 8 bytes in a length
pass and a 64-bit pass over their zero-padded big-endian prefix, longer strings through a rank from
one stable merge sort) and radix-sorts the positions by it, one counting sort per 8-bit digit with
the uniform digits and the already-ordered passes skipped, so every pass is stable and the passes
compose.
Nulls take one final pass per key. The partition is sorted in runs of `spark.vecruntime.sort.runRows`
rows (default 1M) sealed as it arrives and k-way merged on output, ties by run then position, so the
sort's scratch is bounded by the run (#285). The output is gathered through the permutation into Arrow vectors
in batches of 4096 rows. Double ordering is Spark's (`-0.0 = 0.0`, NaN greatest and equal to itself),
strings compare as unsigned bytes like `UTF8String`.

Runs whose bytes pass `spark.vecruntime.sort.spillBytes` (default 1 GiB, #416) are written to local disk
as Arrow IPC and merged back in on output, so a partition larger than the budget spills rather than
failing. One deliberate limit remains: the sort is planned only over a
columnar child. A global `ORDER BY` over Spark's row shuffle keeps `SortExec`: converting rows to
columns just to sort them gains nothing. Over Comet's columnar shuffle (or one of our operators, for
`SORT BY`) the sort is ours, which makes TPC-H Q1 with Comet's scan and shuffle fully accelerated.

`ORDER BY ... LIMIT n` (`TakeOrderedAndProjectExec`) over a columnar child becomes
`VectorTakeOrderedAndProjectExec`, which runs the same per-partition sort with a limit and gathers only
the first `n` rows of each partition -- the part that touches every row stays columnar. Those at most
`n` rows per partition then go as rows through Spark's own single-partition shuffle, where Spark's
ordering takes the final top `n`, Spark's projection applies the select list and the result is
materialised as one columnar batch: the merge sees at most `n x partitions` rows. `OFFSET` falls back.
This is the operator TPC-H Q2, Q3, Q10, Q18 and Q21 end in; `spark.vecruntime.exec.takeOrdered.enabled`
turns it off.

Plain `LIMIT n` is the same idea without the sort: `VectorLocalLimitExec` and `VectorGlobalLimitExec` let
whole batches through until the boundary and compact only the batch that crosses it (no further batch
is pulled from the child), and `VectorCollectLimitExec` -- the operator a query ending in `LIMIT` plans
to -- does that cut per partition and then takes the first `n` rows through Spark's single-partition
shuffle. `spark.vecruntime.exec.limit.enabled` turns the three off; `OFFSET` falls back.

Two structural operators keep a columnar chain whole without computing anything. `VectorUnionExec`
concatenates its children's batches and is columnar as soon as one child is -- Spark's own union only is
when every child is, so a `VALUES` side or a row shuffle used to drop the whole union to rows; Spark's
transitions convert such a child through `RowToColumnarExec` below us. `VectorCoalesceExec` forwards the
child's batches through a shuffle-free `coalesce(n)`. `spark.vecruntime.exec.union.enabled` and
`spark.vecruntime.exec.coalesce.enabled` turn them off. `VectorSampleExec` (`TABLESAMPLE`, `df.sample` without
replacement) is a selection producer like the filter: it runs Spark's own Bernoulli sampler per partition
over the live rows, so the same seed returns exactly Spark's rows, and forwards the bitmap. `VectorLocalTableScanExec`
turns a `VALUES` relation into batches; it is off by default (`spark.vecruntime.exec.localTableScan.enabled`)
because there is nothing to accelerate -- it only removes the row-to-columnar transition for small-table tests.
`VectorRangeExec` (`spark.range`, the `range()` table function) is the other leaf: it writes Spark's rows, in
Spark's partitions, straight into native INT64 batches -- one vector per task, refilled by a Vector API
kernel for every batch -- so the filter, projection and partial aggregate over `range()` are ours from the leaf, where over Spark's row
leaf they stayed Spark's until the first exchange (`spark.vecruntime.exec.range.enabled`).

Window functions start with the ranking layer (#58): `VectorWindowExec` computes `row_number`, `rank`
and `dense_rank` in one walk over the input Spark already sorted by partition and order keys -- a new
partition where a partition key changes, a new peer group where an order key changes, counters carried
across batches -- and forwards every input column. The child may be Spark's row sort: `Window` sits
above `Sort` above an exchange, and without a columnar shuffle that sort stays Spark's, so Spark
converts rows to columns below us and the filter on the rank and everything above it run columnar.
Whole-partition aggregates (`sum(x) OVER (PARTITION BY k)`: the default frame without `ORDER BY`) reuse
the grouped aggregate functions with each partition as a group -- the value is computed exactly as the
Final aggregate computes it -- and hold a partition's rows in memory until it ends, since the value is
known only then. A filter `rank <= k` on a ranking window makes Spark plan a per-partition top-k
(`WindowGroupLimitExec`) below the window in two modes -- before the shuffle over whatever produced the
rows, and after the sort -- and both are ours: the same ranking walk, keeping a row while its rank is at
most k, so the operators feeding the shuffle stay columnar. Running frames (`sum(x) OVER (PARTITION BY k
ORDER BY d)`, whose default frame is `RANGE ... CURRENT ROW`, and the `ROWS` form) make each peer group
or row a group and combine its buffers with the running buffers before it -- sums and counts add,
`min`/`max` compare -- so the same result expression yields the running value. The offset functions
(`lag`, `lead`, `first_value`, `last_value`, `nth_value`) are one row of the partition each, read from
the held rows across batch boundaries; `percent_rank`, `cume_dist` and `ntile`, which need the partition
size, take the same path, as do sliding frames (`sum(x) OVER (... ROWS BETWEEN 2 PRECEDING AND 1
FOLLOWING)`), re-aggregated per row over the frame's rows in order exactly as Spark's sliding frames are.
`RANGE` frames with value offsets (`sum(x) OVER (... ORDER BY d RANGE BETWEEN 5 PRECEDING AND CURRENT
ROW)`) are computed per partition by the frame kernels: the sorted order keys are walked once with two
monotone pointers to find every row's frame (Spark's own buffer walk, so a null key's frame is the null
peer group and the bound arithmetic wraps or raises exactly as Spark's `Add` does), and the aggregate is
re-run over each frame in row order -- bit-identical double sums -- without a per-row object.
Decimal window aggregates run over the 128-bit lane for whole-partition and running frames (#259); a sliding frame over a decimal and `RANGE` offsets over decimal or timestamp keys are refused.

`ROLLUP`, `CUBE` and `GROUPING SETS` (and the rewrite Spark applies to `count(distinct)`) go through
`ExpandExec`, which duplicates every row once per grouping set with the unused keys nulled and a
grouping id appended. `VectorExpandExec` does that without copying a byte: for each grouping set the
output batch borrows the retained columns of the input batch, nulled keys are all-invalid constant
columns and the grouping id is a constant column, so an `n`-set expand emits `n` batches per input
batch and the aggregate above it -- the expensive part -- stays ours. The input batch is held until
its last projection has been consumed. `spark.vecruntime.exec.expand.enabled` turns it off.

## Decimals

A `Decimal(p, s)` with `p <= 18` travels through the kernels as its unscaled value in long lanes,
which is how Spark's own vectors and the Parquet reader hold small decimals (ints for `p <= 9`,
widened once on the way in). Comparisons are long comparisons: Spark's analyzer already casts both
sides to one type. Additions and subtractions rescale the operands to the result scale,
multiplications multiply the unscaled values; Spark's result precision always has room for them, so
none can overflow. Division follows Spark exactly: one long division with a half-up correction when
the scaled dividend fits (with a divisor below 10^18 Spark's two roundings cannot disagree with
it), `BigDecimal` otherwise; a quotient past the result precision is null in legacy mode and an
error in ANSI mode, like a cast that does not fit. Results wider than 18 digits (`Decimal(12,2) *
Decimal(12,2)` is `Decimal(25,4)`) fall back with a reason -- except directly under a decimal `sum`,
or `avg`, where products, sums and differences (and their nestings) are computed speculatively in 64 bits,
checked per row with `Math.multiplyHigh` and a sign-trick overflow test, and the rare row that overflows
is added to the 128-bit accumulator exactly (#26): TPC-H's `sum(l_extendedprice * (1 - l_discount))` and
`sum(x * (1 - d) * (1 + t))` stay ours. Output decimals are `BigIntVector`s
behind `VectorDecimalColumnVector`, which gives Spark's row conversion `getDecimal` over the lanes;
into Comet they are widened to the 128-bit C Data layout.

Spark's optimizer rewrites `sum` over a decimal of up to 8 digits into `MakeDecimal(sum(UnscaledValue(x)))`,
an ANSI `sum(bigint)`, and `avg` over one of up to 11 digits into a double average; both are compiled,
which is also why bigint sums are now overflow-checked in ANSI mode (`AggKernels.sumLongExact`, a
sign-trick overflow lane carried alongside the accumulator) instead of falling back. A wider decimal
sum (`Decimal(12,2)` and up) is accumulated in 128 bits per group on the Partial side and emitted as
Spark's `(sum, isEmpty)` buffer with the `sum` a wide Arrow decimal column; the Final merges those
buffers with Spark's `isEmpty` rules and applies its overflow check at emission (an error in ANSI
mode, null otherwise), so a wide decimal sum runs on our operators in both stages.

## Joins

`BroadcastHashJoinExec` becomes `VectorBroadcastHashJoinExec` when the streamed side is columnar or
an exchange (adaptive execution re-plans a shuffled join as a broadcast join over the bare shuffle
read; Spark converts it below us as for the shuffled hash join),
and `BroadcastNestedLoopJoinExec` (a join with no equi-keys) becomes `VectorBroadcastNestedLoopJoinExec`:
the same iterator with every broadcast row a candidate, the streamed rows chunked to a fixed pair
budget so the condition is evaluated over gathered pairs and the product is never materialised.
The build side of the hash join is our `VectorBroadcastExchangeExec` (#325) when its input is one of
our operators or a columnar source: the build side's batches travel as Arrow IPC streams and are
broadcast as they are, and the join builds its `GroupKeyTable` from them once per executor, with no
row conversion below the exchange. A Spark join over the same (reused) exchange, or dynamic partition
pruning, still gets Spark's own relation, built from the batches on first use. Otherwise the build
side is left as Spark planned it, a `BroadcastExchangeExec` producing a
`HashedRelation`: each executor reads its rows once into columns and builds a `GroupKeyTable` over the
keys (with a lookup-only probe), and a Spark join over the same
broadcast keeps working. `ShuffledHashJoinExec` becomes `VectorShuffledHashJoinExec`; like the
Final aggregate it accepts exchanges as inputs, so it runs over Spark's row shuffle (a
`RowToColumnarExec` on each side) as well as over Comet's. Probing evaluates the streamed keys with
the kernels, looks every row up in one pass, expands the match chains into two index arrays and
gathers the output with `GatherKernels` (`-1` pads the unmatched rows of outer joins); semi and anti
joins are a selection over the streamed batch, compacted once; an inner join's non-equi condition is
evaluated on the joined batch and the failing rows compacted away. Semi, anti and outer joins with a
condition gather the candidate pairs of each streamed row first, evaluate the condition over them
and only then decide what the row becomes (kept or dropped, its passing pairs or one padded row);
a full outer join remembers which build rows were paired and emits the rest after the last
streamed batch. Null keys never match. Double
keys are refused because Spark compares them after NaN/zero normalisation and the key table by bits.
Sort-merge joins -- Spark's default for large equi joins -- have two columnar forms, chosen by
`spark.vecruntime.exec.sortMergeJoin.mode`. `merge` (the default under `auto`, #286/#311) is a real,
order-preserving merge join over the sorted inputs Spark already placed: the right side is read run
by run (the rows sharing one key), the current run is the only buffered state, equal runs emit their
cross product in Spark's order, every join type is covered and no statistics are needed. `hash`
(#10) re-expresses the join as the shuffled hash join, dropping the sorts -- same rows, but tied rows
can come out in another order than Spark's, so it is never chosen where the order can show. Under
`auto` (the default) a sort-merge join whose order no parent relies on is judged by what its smaller
side weighs per task, by statistics (#416): within `spark.vecruntime.join.spillBytes` it is the hash join
built in memory; within `spark.vecruntime.join.hashMaxBuildSize` (default one bucketing pass,
`spillBuckets` x `spillBytes`) it is the hash join splitting both sides into buckets on disk (a grace
hash join); past that, or without an estimate, it is our merge join over the spilling sort, whose
memory the sort's budget bounds whatever the inputs weigh. One whose order can show (a limit, a sort,
a window above) is the merge join regardless. Nothing is left to Spark's own operator (#311's size
gate, set when Spark's row sort fed our join, is gone).

## Spark's SQL test suite

Comet validates itself by running Spark's own SQL golden-file tests with its extension injected;
so can this plugin. The `spark-sql-tests` module (only built under the `spark-sql-tests` profile,
never by `mvn verify`: the whole suite takes about 15 minutes) subclasses `SQLQueryTestSuite` from
the `spark-sql` tests jar, unpacks its `sql-tests/` golden files and `test-data/`, sets
`spark.sql.extensions` to ours and requires every golden result to match whether an operator was
converted or not. Run it on demand, whole or by a regex over test-case names:

The site documents this end to end: [Testing & correctness](https://vecruntime.github.io/vecruntime/testing.html)
covers the golden suite, the ported Comet matrices and the benchmark checksums, and the
[Compatibility matrix](https://vecruntime.github.io/vecruntime/compatibility.html) lists what runs on
VecRuntime versus falls back, row by row.

```bash
benchmarks/scripts/run-spark-sql-tests.sh                 # everything
benchmarks/scripts/run-spark-sql-tests.sh '^(group-by|join|decimal)'
SQL_TESTS_EXCLUDE='^$' benchmarks/scripts/run-spark-sql-tests.sh   # include the excluded files too
SQL_TESTS_UPDATE_BASELINE=true benchmarks/scripts/run-spark-sql-tests.sh   # full run; rewrite the coverage floor
```

A few files are excluded by default (`VectorSQLQueryTestSuite.defaultExclude`): `explain*.sql`,
whose golden output is Spark's own physical plan, the DataSketches files (`hll`, `kllquantiles`,
`thetasketch`), whose library refuses to start on any JDK newer than 21, and `udtf/udtf.sql`, which
needs `pyspark` installed (the Python UDF variants skip themselves without it and count as ignored).
Everything else passes: 642 test cases, 111 ignored, with 4231 of the 33856 query executions running
at least one VecRuntime operator. Passing is the low bar -- a file passes just as well when every
operator falls back -- so the run also prints a per-test-case table (executions, executions that ran
one of our operators, operators) split into the 164 cases that run our operators and the 420 that never
can (analyzer-only cases, DDL, files with no supported operator), and a full run compares every case
with the checked-in floor `spark-sql-tests/src/test/resources/vector-sql-coverage.tsv`: a case that
lost accelerated executions fails the suite, naming the case, because a fallback introduced by a planner
change is otherwise invisible; cases above the floor are listed, and `SQL_TESTS_UPDATE_BASELINE=true`
records them. The golden files pin Spark's summation order for doubles, so the suite's JVM runs with
`-Dvecruntime.agg.interleave=1`, the mode in which our double sums add in Spark's order (the default
rotates accumulators and can differ in the last digits; `docs/results.md`). The suite earns its keep:
its first run found a bare literal projection (`SELECT 1 FROM ... HAVING max(id) > 0`) that compiled
but could not be materialised, and the run that introduced the table found four more -- `nanvl`
evaluating its second argument eagerly (`nanvl(c, 1/c)` raised where Spark keeps `c`), `count(DISTINCT
3, 2)` evaluating a literal as a column, a collated string accepted as a lane (Spark's own
`RowToColumnarExec` cannot convert one), and a cross join whose build side was pruned to no columns
returning nothing. The test JVM runs with
`-Dspark.testing=true` (the golden files assume Spark's test-mode defaults, such as the TIME type)
and `-XX:-OmitStackTraceInFastThrow`: after enough ANSI overflows in one JVM, HotSpot's preallocated
`ArithmeticException` carries no message and Spark's error formatting fails on the null (in Spark's
own operators, not ours).

## Code layout

| Module | Language | Contents |
|---|---|---|
| `kernels/` | Java 25 | `VectorBuffers` (Arrow-layout `MemorySegment`s), SIMD kernels: compare, bitmap logic, compaction, arithmetic, decimal rescaling and division, casts, reductions (plain and overflow-checked), group hashing and key table, grouped accumulators, sort, gather, column builder; scalar references used as test oracles |
| `spark/` | Scala 2.13 + Java | `VectorPlugin`, session extension, `VectorColumnarRule`, expression compiler, the operators (`VectorFilterExec`, `VectorProjectExec`, `VectorHashAggregateExec` with its spill, `VectorSortExec`, `VectorTakeOrderedAndProjectExec`, the limit family, `VectorUnionExec` / `VectorCoalesceExec`, `VectorExpandExec`, `VectorWindowExec`, `VectorGenerateExec`, `VectorSampleExec`, `VectorRangeExec`, `VectorMergeRowsExec`, `VectorBroadcastHashJoinExec` / `VectorShuffledHashJoinExec` / `VectorBroadcastNestedLoopJoinExec` / `VectorSortMergeJoinExec`, `VectorShuffleExchangeExec`, `VectorToCometExec`, `VectorPrefetchScanExec`), Arrow output, input adapters (Spark vectors, Arrow, Comet, Iceberg), the Vector Acceleration UI tab |
| `shuffle/` | Scala 2.13 | the columnar shuffle (#288): `VectorShuffleManager` (writer, reader, file cleanup), `PartitionedIpcWriter` / `PartitionedIpcFile` (Arrow IPC record batches per reduce partition in Spark's data-file layout, adaptive dictionaries, zstd), the Flight data plane (`FlightShuffle`: one server per executor, one `DoGet` per executor and reducer) and the `block` backend over Spark's block transfer |
| `benchmarks/` | Java + Scala | JMH kernel microbenchmarks; the TPC-H (22 queries) and TPC-DS (103 queries) runners, local and on a cluster; `submit-cluster.sh` and the Kubernetes `SparkApplication` manifest under `benchmarks/k8s/` (the image build and the run matrix of the EKS campaign arrive with #247); `profile-query.sh` (one query under JFR) |
| `spark-sql-tests/` | Scala 2.13 | Spark's own SQL golden-file suite run with the plugin (profile `spark-sql-tests`, on demand only; see below) |
