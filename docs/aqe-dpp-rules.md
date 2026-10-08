# Planner rules: optimizer rewrites, AQE and DPP

vecruntime plans most of its work in the columnar rule that runs after Spark's planner. A few speedups
need a different logical plan, not a different physical operator for the same plan. They are
optimizer rules injected through `SparkSessionExtensions.injectOptimizerRule` (or, when Spark's own
push-down would undo them, added to its last optimizer batch). This page lists every
such rule and every change we make to Spark's adaptive execution (AQE) or dynamic partition pruning
(DPP): what each one rewrites, when it applies and when it does not, why the result is the same,
how to switch it off, and what it was measured to do.

Each rule here follows the same contract:

- It is general SQL, not tuned to particular queries. TPC-DS queries are the evidence, not the
  pattern the rule matches.
- It applies only where the rewrite is exact, and declines everything else. The tests cover both
  sides.
- It has its own `spark.vecruntime.optimizer.*` switch, and it is also off when the plugin is off
  (`spark.vecruntime.enabled=false`). Tests compare against Spark that way.
- It runs the gate, the SQL golden suite, and TPC-DS SF1 checksums against Spark. A performance claim
  carries a 1 TB A/B with control queries (AGENTS.md section 10).

The rules came out of a 1 TB TPC-DS comparison with EMR Serverless (emr-spark-8.1.0, 6 x 16 vCPU,
2026-10-05). EMR's own Spark extension rewrites the same shapes; its plans are the reference for
which shapes are worth handling.

| Rule | Switch (default `true`) | Queries it changes in TPC-DS |
|---|---|---|
| `SelfJoinToAggregate` | `spark.vecruntime.optimizer.selfJoinToAggregate.enabled` | q95 |
| `MergeFilteredAggregates` | `spark.vecruntime.optimizer.mergeFilteredAggregates.enabled` | q9, q28, q88, q90 |
| `SharedAggregateInputs` | `spark.vecruntime.optimizer.sharedAggregateInputs.enabled` | q1, q65 |
| `DppThroughAggregate` | `spark.vecruntime.optimizer.dppThroughAggregate.enabled` | q2, q59 |
| `TransitiveDpp` | `spark.vecruntime.optimizer.transitiveDpp.enabled` | q72 |
| `FactBloomFilter` | `spark.vecruntime.optimizer.factBloomFilter.enabled` | q16, q50, q93, q94 |
| `NarrowBelowJoin` | `spark.vecruntime.optimizer.narrowBelowJoin.enabled` | q23a, q23b |

The plugin changes no AQE setting. `DppThroughAggregate` and `TransitiveDpp` add dynamic partition pruning
filters where Spark's DPP does not reach; Spark's own DPP settings are unchanged.

## `SelfJoinToAggregate`: existence-only self-joins as a min/max aggregate

`spark/src/main/scala/org/apache/spark/sql/vecruntime/SelfJoinToAggregate.scala`, PR #630.

**Shape.** An inner self-join whose condition compares one or more key columns for equality and
exactly one more column for inequality:

```sql
SELECT t1.k FROM t t1, t t2 WHERE t1.k = t2.k AND t1.v <> t2.v
```

The rule replaces it with:

```sql
SELECT k FROM t WHERE k IS NOT NULL GROUP BY k HAVING min(v) <> max(v)
```

It keeps the other side's keys as aliases of the first side's, so nothing above needs renaming.

**Why it is exact.** Two rows of one key with different non-null `v` exist exactly when that key's
smallest and largest non-null `v` differ. A null `v` or a null key never satisfies the join, and it
never moves a `min` or a `max`. The rewrite returns each key once, where the join returned it once
per matching pair. So the rule applies only where duplicates cannot be observed:

- on the build side of a left semi or anti join, or inside an `IN` / `NOT IN` / `EXISTS` /
  `NOT EXISTS` subquery;
- reached from there through projections, filters and inner joins only, with no aggregate in
  between;
- and nothing above reads a column of the self-join other than its keys.

**Declined:**

- sides that are not the same plan (`sameResult`);
- any other conjunct in the condition;
- two or more `<>` columns;
- a `v` whose `=` does not agree with `min` / `max` ordering: floating point, collated strings, and
  complex types (supported: integers, decimals, dates, timestamps, binary-collated strings);
- a self-join whose rows are counted, or whose other columns are read above.

**Measured.**

- TPC-DS q95's `ws_wh` CTE is this join, used twice under `IN`, and both uses are rewritten.
- SF1 checksums match Spark.
- At 1 TB on 2026-10-06, in one leg against `main` 13ea9ec, q95 went from 27.8 s to 13.4 s.

## `MergeFilteredAggregates`: global aggregates with different filters in one pass

`spark/src/main/scala/org/apache/spark/sql/vecruntime/MergeFilteredAggregates.scala`, PR #631.

**Shape.** Two or more global aggregates (no `GROUP BY`) over the same input plan that differ only in
their `Filter`s. They can appear in two ways:

- as a condition-less inner join of single-row aggregates (`SELECT * FROM (SELECT count(*) ... WHERE
  a) x, (SELECT count(*) ... WHERE b) y`, folded over any number of them);
- as uncorrelated scalar subqueries of one operator.

Spark's `MergeScalarSubqueries` merges only identical subqueries, and it does not cover the join form.

**Rewrite.** The rule walks both inputs together through projections, filters and inner joins with
equal conditions; the leaves must be the same plan.

- Where the filters differ, the merged plan keeps the rows of either side, `Filter(c1 OR c2)`. The
  predicates' columns are carried up through the projections.
- Each aggregate keeps only its own side's rows through `FILTER (WHERE p)`.
- A `DISTINCT` aggregate gets the predicate in its argument instead, `IF(p, x, NULL)`. That works for
  `count`, `sum`, `avg`, `min` and `max`, which ignore the null the same way.
- When a third or later input is folded in, an aggregate whose filter already implies the new
  predicate keeps its filter unchanged. Without this, q9's predicates grew to tens of KB each.

The two forms are then finished differently:

- **Cross join:** it becomes one aggregate. Each side yields exactly one row, so the join is the
  concatenation of the two rows.
- **Scalar subqueries:** each mergeable group is replaced by projections of one merged aggregate, which
  `MergeScalarSubqueries` then computes once.

**Declined:**

- different leaves;
- outer, semi or anti joins between the filter and the aggregate;
- grouped aggregates;
- non-deterministic expressions;
- a `DISTINCT` aggregate other than the five above.

**Measured.**

- SF1 checksums match Spark.
- At 1 TB on 2026-10-06, in one leg against `main` 13ea9ec:

  | Query | `main` (s) | With the rule (s) |
  |---|---:|---:|
  | q9 | 48.0 | 10.9 |
  | q28 | 35.5 | 24.3 |
  | q88 | 32.4 | 9.3 |
  | q90 | 7.5 | 11.3 |

- **q90:** the merge applies and the single `web_sales` scan is identical to each of the two it
  replaces, but its tasks waited on S3 3-4x longer than in the main leg. That turned out to be S3
  variance. A recheck alternating the two images (main, rules, main, rules; 3 iterations each)
  measured, in seconds:

  | Query | `main` round 1 | rules round 1 | `main` round 2 | rules round 2 |
  |---|---:|---:|---:|---:|
  | q90 | 8.8 | 5.4 | 9.5 | 5.5 |
  | q88 | 121.3 | 6.9 | 121.1 | 6.3 |

  Checksums were identical. In both rounds `main`'s q88 had two iterations near 121 s and one near
  37 s.

**Fix shipped with it.** A grouped aggregate with `FILTER` failed with `ArrayIndexOutOfBoundsException`
when a batch in which no row passed the filter brought new groups. The filtered function now sees
every batch.

## `SharedAggregateInputs`: one input for aggregates that differ only by an inferred `IS NOT NULL`

`spark/src/main/scala/org/apache/spark/sql/vecruntime/SharedAggregateInputs.scala`, issue #632.

**Shape.** A CTE or view with a `GROUP BY` used twice is inlined twice, and Spark optimizes each copy
on its own. `InferFiltersFromConstraints` adds `IS NOT NULL(k)` to the copy that is later joined on a
grouping key `k` and pushes it down to the scan; the other copy (say, one aggregated again by a coarser
key) does not get it. The two inputs no longer match, so `ReuseExchange` cannot share them, and the
input -- typically a fact scan, a dimension join and a shuffle -- runs twice. TPC-DS q65 (`store_sales`)
and q1 (`store_returns`) have this shape.

**Rewrite.** For two aggregates with the same grouping (by position in the input), the rule removes
`IS NOT NULL` conjuncts on grouping keys from each input, walking projections that pass the key
through, filters and inner joins. If the stripped inputs are the same plan, each aggregate gets the
stripped input and the removed predicates as a `Filter` above it, adding the key to its output first
when the parent had pruned it. Below an aggregate grouping by `k`, `IS NOT NULL(k)` removes exactly the
rows of the null group, so filtering the null group out afterwards gives the same result.

**Placement.** The rule runs in Spark's last optimizer batch, `User Provided Optimizers`
(`spark.experimental.extraOptimizations`). Every earlier batch is followed by Spark's own filter
push-down (the last one is `Pushdown Filters from PartitionPruning`), which moves the predicates back
below the aggregates. `SparkSessionExtensions` cannot inject into that batch, so a post-hoc analyzer
rule, `RegisterLateOptimizerRules`, adds it to the session's `extraOptimizations` once, before the
session's first optimization. A rewritten pair has the same input, so the rule does nothing on the
batch's next pass.

**Declined:**

- aggregates with different groupings, or grouping by expressions rather than columns;
- inputs that still differ after the `IS NOT NULL`s are removed (another filter, another join);
- `IS NOT NULL` of anything other than a grouping column, or below an outer, semi or anti join, an
  aggregate, a window, or a projection that computes the key.

**Measured.**

- SF1 checksums match Spark on q1, q65, q30, q81, q24a and q18.
- At SF1, q65 and q1 plan 4 Parquet scans instead of 6: the fact scan and its `date_dim` join are
  planned once and reused.
- At 1 TB on 2026-10-06, the first version (PR #636) did not fire on q65 or q1. The fact table is partitioned
  there, and Spark's DPP gives each copy its own pruning subquery, which broke the comparison in two ways:
  - the subquery sits in a different place among each copy's filters, and canonicalization does not
    reorder an `AND` that holds a subquery;
  - `DynamicPruningSubquery` canonicalizes its build keys on their own, keeping their expression ids.

  The rule now rebuilds the remaining conjuncts in one canonical order. It compares inputs with the pruning
  subqueries' build keys normalized against their build plan. A partitioned-fact test covers this case.
- The 1 TB A/B of the fixed rule is pending.

## `DppThroughAggregate`: dynamic partition pruning through an aggregate

`spark/src/main/scala/org/apache/spark/sql/vecruntime/DppThroughAggregate.scala`, issue #633.

**Shape.** `Join(Inner | LeftSemi, A, F, A.k = F.k')` where:
- `F` has a selective filter (`isLikelySelective`, the test Spark's DPP uses);
- `A` reaches, through projections, filters and inner or semi joins, an `Aggregate` with `k` among its
  grouping columns;
- below the aggregate, `k` comes from a dimension `D` that is inner-joined to a fact on `fact.p = D.d`;
- `p` traces to a partition column of a file scan, through projections, filters and unions of fact branches.

TPC-DS q59 (`store_sales` grouped by `d_week_seq`, each use joined to a `d_month_seq` range of `date_dim`) and
q2 (`web_sales` union `catalog_sales`, the same way) have it.

**Why Spark does not prune it.** `PartitionPruning` follows the join key down through the aggregate to `D.k`.
That is not a partition column, so it stops. The inner fact-to-dimension join gets no pruning either: `D`
has no selective filter of its own, and a dynamic filter does not count as one.

**Rewrite.** The fact scan gets `Filter(DynamicPruningSubquery(p, D LEFT SEMI JOIN F ON D.k = F.k', D.d))`,
with `onlyInBroadcast = false`. Spark plans it like its own DPP: a broadcast reuse where one matches, else
a one-off subquery over the small dimension, and the partition filter reaches `FileSourceStrategy`. For a
union, every branch whose scan is partitioned on the matching column gets its own filter.

**Why the result is the same.** The aggregate groups by `k`, and above it an inner or semi join keeps only
the `k` values `F` produces. A fact row whose dimension row has a `k` outside that set only feeds groups the
join drops. Every dimension row whose `k` is in the set keeps its `d`, so no row of a surviving group is
removed. Null keys never join, on either side.

**Placement.** Runs once in the Pre-CBO batch (`injectPreCBORule`): after filters were pushed down, and
before Spark's `PartitionPruning`, which skips a join that already has a pruning filter on its side.

**Declined:**
- a join key that is not a grouping column of the aggregate (an aggregate output such as `max(...)`);
- an outer join that preserves the aggregate's side;
- an `F` with no selective filter;
- a scan that already has a pruning filter on `p`;
- a key computed by an expression above the aggregate rather than passed through;
- non-deterministic filters or aggregates on the path.

**Measured.** `DppThroughAggregateSuite` covers the q59 and q2 shapes, which read fewer files than with the
rule off, plus the two declined shapes and the switch. Results are compared with Spark with the plugin off.

At 1 TB on 2026-10-06, `main` 35e797d against the rule, with legs alternated (3 iterations each). Times are
median seconds, and checksums are identical:

| Query | `main` | With the rule | EMR Serverless |
|---|---:|---:|---:|
| q59 | 13.7 / 13.3 | 6.4 / 6.2 | 6.0 |
| q2 | 14.1 / 13.2 | 4.5 / 4.3 | 11.0 |
| q67 (control) | 58.4 / 59.7 | 57.5 / 58.4 | |
| q18 (control) | 4.15 / 4.12 | 4.17 / 4.09 | |

## `TransitiveDpp`: dynamic partition pruning through a second join key

`spark/src/main/scala/org/apache/spark/sql/vecruntime/TransitiveDpp.scala`, issue #634.

**Shape.** An inner `Join(X, D)` on `X.p = D.d AND X.x = D.y`, where:
- `p` traces to a partition column of a file scan inside `X`, through projections, filters, unions and
  inner or semi joins;
- `D` has no selective filter of its own;
- `x` traces, through projections, filters and inner joins, to a join-free subtree `S` of `X` that has one.

TPC-DS q72 is this shape. `inventory` joins `date_dim d2` on `inv_date_sk = d2.d_date_sk AND
d1.d_week_seq = d2.d_week_seq`, and `d1` is restricted to one `d_year`. EMR Serverless prunes `inventory`
there; we did not.

**Why Spark does not prune it.** `PartitionPruning` finds `inv_date_sk` as a partition column, but the
filtering side, `d2`, has no selective predicate (`hasPartitionPruningFilter`). What makes `d2` selective
is the second equality, which comes from `d1` on the other side of the same join.

**Rewrite.** The scan gets `Filter(DynamicPruningSubquery(p, D LEFT SEMI JOIN S ON D.y = S.x, D.d))`,
with `onlyInBroadcast = false`, planned like Spark's own DPP.

**Why the result is the same.** Every row the join keeps has `p = D.d` for a `D` row whose `y` equals an
`x`. That `x` comes from a row of `S` that passed `S`'s filter: only inner joins lie between `S` and the
join. So that `D` row's `d` is in the pruning set, and no row the join keeps is removed.

**Placement.** Pre-CBO, once, before Spark's `PartitionPruning`, like `DppThroughAggregate`.

**Declined:**
- a selective `D` (Spark's own DPP covers it);
- a non-selective `S`;
- a `D` or `S` larger than `spark.sql.autoBroadcastJoinThreshold`, since the subquery scans both once
  more. This also keeps a filtered fact from ever being the source;
- a non-inner join at the top;
- a source key computed by an expression;
- a scan that already has a pruning filter on `p`.

**Measured.** `TransitiveDppSuite` runs over two partitioned facts and a date dimension, and compares
results with Spark with the plugin off:
- the q72 shape prunes `inventory` and reads fewer files than with the rule off;
- an unfiltered source, an outer join, and the switch leave the plan alone.

The 1 TB numbers come with the PR.

## `FactBloomFilter`: a runtime bloom filter from a smaller fact onto a larger one

`spark/src/main/scala/org/apache/spark/sql/vecruntime/FactBloomFilter.scala`, issue #641.

**Shape.** A shuffle equi-join (neither side broadcastable) that can prune the application side (inner, left
semi, or the null-supplying side of an outer join), where:
- the application key traces, through projections, filters and inner or semi joins, to a file scan of at
  least `spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold` (10 GB by default);
- the application side is at least `spark.vecruntime.optimizer.factBloomFilter.sizeRatio` (10) times larger
  in estimated bytes than the creation side;
- that scan has no dynamic pruning or bloom filter on the key yet.

TPC-DS q93 is this shape: `store_sales` (2.9G rows at 1 TB) joined to `store_returns` (~290M) on ticket and
item. EMR Serverless filters `store_sales` by `store_returns`' keys before the shuffle; we and Spark did not.

**Why Spark does not add it.** `InjectRuntimeFilter.extractSelectiveFilterOverScan` requires a selective
`Filter` directly over the creation-side scan. `store_returns` has none of its own, so Spark builds nothing
for q93, q50, q16 or q94. Raising Spark's bloom caps does not help and makes q75, q78 and q80 worse (a
20-query 1 TB run; the issue has the numbers).

**Rewrite.** The application scan gets `Filter(might_contain(ScalarSubquery(BloomFilterAggregate(
xxhash64(creationKey))), xxhash64(applicationKey)))` -- the same `BloomFilterAggregate` and
`BloomFilterMightContain` Spark's own rule uses, so the aggregate and the probe run vectorised. A composite
join key gets one filter per key. The filter's expected items come from the creation side's row estimate, or
its bytes over its row width when the row count is unknown (as at 1 TB); the bit size is capped by
`spark.sql.optimizer.runtime.bloomFilter.maxNumItems` and `maxNumBits`.

**Evidence of selectivity (#650).** Size alone says nothing about pruning: at 1 TB the rule also built filters
from whole dimension tables (`customer`, `customer_address`, `item`), whose keys every fact row matches, and each
such filter only delayed its scan (q19 2.2 -> 4.8 s, q45 2.8 -> 5.7, q64 23.8 -> 71.9). The rule now needs evidence:
- **Distinct counts, when they exist** -- `CatalogTable.stats.colStats` from `ANALYZE TABLE ... COMPUTE STATISTICS
  FOR COLUMNS` (read directly, so `spark.sql.cbo.enabled` is not needed), or `attributeStats` of a DSv2 relation
  that reports column statistics (Iceberg, from Puffin theta sketches). Each key is traced through projections,
  filters and joins to its base relation (`KeyStats`). At most `ndv(creation) / ndv(application)` of the
  application rows can match; the filter is added when that is at most `maxSelectivity` (0.5), sized by the
  creation key's distinct count (capped by the creation side's row estimate).
- **Otherwise, a reduced creation side** -- a filter on it other than the `IS NOT NULL` of a column the optimizer
  infers from join keys. A whole, unfiltered table gets no filter.

**Two-level merge (#646).** `bloom_filter_agg` makes one partial filter per creation-side task, each the whole bit
array (up to `maxNumBits`/8 bytes), and a single Final task would merge all of them: at 1 TB q17's filters merged 595
and 801 partials in one task each (3.8-6.6 GB read, 44-59 s). The subquery is therefore
`bloom_filter_merge(bloom_filter_agg(xxhash64(key)) GROUP BY pmod(spark_partition_id(), mergeBuckets))`: each task's
partial goes to one of `mergeBuckets` (32) groups merged in parallel, and `bloom_filter_merge` ORs the group filters in
one small task. Both levels use the same bits and hash count, so the OR is the single-level filter bit for bit
(`FactBloomFilterSuite`); a group with no key gives null and is skipped.

**Right-sized, partitioned filters (#653).** One `bloom_filter_agg` is capped by Spark at `maxNumItems` (4M) and
`maxNumBits` (64M bits): for tens of millions of keys it is saturated and prunes nothing, and raising the caps makes
every creation-side map task ship a full-size partial. Beyond `maxNumItems / 2` keys the filter is therefore
partitioned: `h = xxhash64(key)` is repartitioned by `b = pmod(h, B)`, one `bloom_filter_agg(h)` per bucket runs after
that shuffle (its distribution is already satisfied, so there is no per-map-task partial), and
`partitioned_bloom_filter` packs the `B` sub-filters into one value. The probe `might_contain_partitioned` tests
sub-filter `pmod(h, B)`; a key the creation side has always lands in its bucket, so there is no false negative.
`B = ceil(keys / (maxNumItems / 2))` rounded up to a power of two, so the vectorised probe picks the sub-filter as
`h & (B - 1)` (equal to `pmod(h, B)`) instead of a long division per row (#659); 8 bits a key, fewer down to 4 to
stay within `maxTotalBits` (512M bits); a
filter that would need more is declined. The probe is vectorised and its sub-filters are deserialised once per
executor (`BloomFilterCache`). EMR Serverless sizes its own filters the same way (q93: `GenerateBloomFilter` for ~110M
rows on `store_returns`' keys, shuffle 2.8 GB against our 41.8 GB).

**Split-block sub-filters (#659).** Each sub-filter of a partitioned filter is a split-block bloom filter, the layout
of Parquet and Impala: 256-bit blocks of eight 32-bit words, a key setting one bit in each word of the one block its
remixed hash picks. A probe reads one 32-byte block -- a cache line -- where Spark's `BloomFilterImpl` reads its `k`
bits anywhere in a multi-megabyte array. At 1 TB the probe was the filter's cost: q93's `store_sales` scan probes
2.9G rows, and with Spark's layout spent 27 % more task time than without the filter, more than the 41 GB of
shuffle it saved. At 8 bits a key the false-positive rate is about 3 % (Spark's: about 2 %).

**Vectorised probe (#664).** The vectorised filter probes a partitioned filter a batch at a time
(`BloomKernels.probe`). The probed rows' hashes are remixed in the widest long vectors (eight per op on AVX-512),
then each key's block is tested in one 256-bit op: its eight bit masks from one multiply-and-shift by the salts, one
compare with the block. There is no branch per key, so consecutive keys' block loads overlap. In JMH on an AVX-512
host it is 2.7 to 4 times the per-key probe, including against a 16 MB filter. On a host without 256-bit vectors the
same loop runs as branch-free scalar code. The layout and the results are unchanged.

**Why the result is the same.** A bloom filter has no false negatives, and a row whose key the creation side
does not hold cannot survive the join. False positives only let a non-matching row through to the join, which
then drops it.

**Placement.** The session's last optimizer batch, after Spark's DPP and runtime-filter rules, so it does not
duplicate a filter they already placed and can see an existing one. At most
`spark.sql.optimizer.runtimeFilter.number.threshold` filters per query, counting Spark's.

**Declined:**
- two sides of similar size (the ratio test);
- no evidence of selectivity: distinct counts give a match share above `maxSelectivity`, or, without them, the
  creation side is a whole, unfiltered table (#650);
- a broadcastable creation side (a broadcast join needs no extra filter -- this also excludes q75, q78 and
  q80, which regressed when Spark injected one);
- an application side below the scan-size threshold;
- the preserved side of an outer join;
- a key already carrying a dynamic pruning or bloom filter.

**Measured.** `FactBloomFilterSuite` covers the q93 shape (the larger fact gets the filters, results match
Spark) and the declined shapes. The 1 TB numbers come with the PR.

**Related.** `BloomProbeExpr` now skips rows an earlier conjunct already rejected (#635), so a probe this rule
adds costs nothing on rows the scan's other filters drop.


**Run-time evidence (#659).** Plan-time estimates of the creation side -- a filtered dimension, a join -- are
what made q18, q61 and q23b slower with the rule at 1 TB. The filter's build therefore also counts the creation
side's actual rows, and yields a null filter when they are not `sizeRatio` times fewer than the application
side's estimated rows; the probe is `coalesce(might_contain(...), true)`, so a declined filter keeps every row.
A second run-time limit compares the same count with the creation key's base table: at most
`runtimeMaxReduced` (0.2) of its rows. q18's `customer` filtered to six birth months keeps half its keys; its filter
was built and applied, and the `catalog_sales` scan stage's task time doubled.
No probe goes on a relation the plan reads more than once (q23b's CTE): it would cost the copies' exchange reuse.
Off with `spark.vecruntime.optimizer.factBloomFilter.runtimeEvidence=false`.

**Transitive reduction (#659).** A small, selectively filtered relation `D` broadcast-joined above a shuffle join,
on a column of one of its inputs `C`, first gets a bloom filter of its key on `C`'s scan. `C` is then reduced and
is the creation side of a filter onto the shuffle join's larger input -- the chain EMR Serverless applies in q93
(a run-time filter on `sr_reason_sk` from one `reason`, then bloom filters onto `store_sales`). The inner join with
`D` above keeps exactly the rows the filter can keep, so results are unchanged. Spark's own runtime filter leaves
it out because the join with `D` is a broadcast. With run-time evidence, the plan-time size check only requires
the application side to be the larger: the estimate of a reduced creation side is its base table's size.
Off with `spark.vecruntime.optimizer.factBloomFilter.transitive=false`.

**Shared creation-side exchange (#659).** The build subquery reads `BloomCreationRef`, a reference to the join
input that the optimizer leaves alone. The query-stage preparation rule `ShareBloomCreationExchange` replaces its
physical placeholder with that join input's own shuffle exchange, carrying the reference's logical link so the
subquery's re-planning keeps it. AQE's stage cache then materialises the exchange once, for the join and for the
filter, and the build is a read of a stage the join computes anyway (EMR Serverless's `GenerateBloomFilter` over a
`ReusedExchange`). A reference no shuffle join claims turns its filter into null, which every row passes. Off with
`spark.vecruntime.optimizer.factBloomFilter.shareExchange=false`.

## `NarrowBelowJoin`: a substring or length of one join side's string computed below the join

`spark/src/main/scala/org/apache/spark/sql/vecruntime/NarrowBelowJoin.scala`, issue #635.

**Shape.** A projection directly over a join computes `substr(x, pos, len)` (also `left`/`right`), `length(x)` or
`octet_length(x)`, with `pos`/`len` literal and `x` a string or binary column of the join's smaller side by
estimated size.

TPC-DS q23a groups `store_sales JOIN item` by `substr(i_item_desc, 1, 30)`. Spark's `PullOutGroupingExpressions`
leaves the substring in a projection above the join, so at 1 TB each of ~1.66G joined rows gathered the whole
description from the broadcast side and then cut 30 characters of it.

**Rewrite.** The smaller side becomes `Project(columns still needed above or by the join condition, f(x) AS a, side)`
and the projection above reads `a`: the join carries the short result, and `f` runs once per row of that side.

**Why the result is the same.** `f` is deterministic and cannot fail, and a join only repeats or drops each side's
rows without changing their values. On the null-supplying side of an outer join an unmatched row has `x` null, and
`f(null)` is null, the value the padded row gets anyway.

**Placement.** The session's last optimizer batch, after column pruning and filter push-down.

**Declined:**
- an expression over the larger side (the join already carries that side row for row);
- an expression over both sides;
- a non-narrowing expression (`upper` keeps the width);
- the right side of a semi or anti join (not in the output).

**Measured.** `NarrowBelowJoinSuite` covers the q23a shape, a plain projection with a length and a substring, the
null-supplying side of an outer join, the declined shapes and the switch; results are compared with Spark with the
plugin off. The 1 TB numbers come with the PR.
## `AggregateBelowJoin`: the fact aggregated by its join keys before a star join

TPC-DS q4, q11 and q74 sum `store_sales` (and the catalog and web facts) per customer and year, grouped by seven
`customer` strings and `d_year`, after joining `customer` and `date_dim`. Spark aggregates after the joins, so
every fact row is joined and then hashed on the strings: at 1 TB each of q4's fact stages shuffled 6.1 GB
(537M rows) against 0.7 GB on EMR Serverless, which aggregates `(ss_customer_sk, ss_sold_date_sk)` directly
above the scan.

Shape (logical, in the late optimizer batch):

```
Aggregate(G, aggs, path)   path: Project / Filter / Join(inner, or left semi with F on the left) down to F
```

where every aggregate function is a non-distinct, unfiltered `sum`, `count`, `min` or `max` over columns of `F`,
and every column of `F` the plan above needs apart from those inputs is a join key on the path (`G` groups by the
other sides). `F` becomes `Aggregate(P, P ++ partials, F)` with `P` those join keys, and the top aggregate
combines: `sum` of partial sums cast to the original type, `coalesce(sum(partial counts), 0)`, `min`/`max` of
the partial minima and maxima.

The result is unchanged: an inner join keeps or repeats an `F` row by its join keys alone, and every row of a
pre-aggregated group has the same keys, so the group meets the same rows each of its rows would; `sum`, `count`,
`min` and `max` distribute over that. A null key forms one group, which the join drops as it would drop its rows.
Decimal sums are exact; a floating-point sum can differ in the last digits, as any change in summation order.

Declined: a `DISTINCT` or filtered aggregate, any other function (`avg`, `stddev`, ...), aggregate inputs from
more than one side, a column of `F` grouped by or used above other than as a join key, outer joins, and an `F`
that is already an aggregate. Off with `spark.vecruntime.optimizer.aggregateBelowJoin.enabled=false`.

Evidence of reduction is required: the rule fires only when statistics put the pre-aggregate's groups -- the
product of its keys' distinct counts, an upper bound -- at most `1 / minReduction` (default 4) of the fact's
rows, and declines without statistics. Measured at 1 TB without that check, the rewrite made q4, q11 and q74
34-104 % slower: their `(customer_sk, sold_date_sk)` keys barely reduce the fact rows, so the pre-aggregate only
added a stage and a shuffle (q23b, whose keys do reduce, was 20 % faster).

