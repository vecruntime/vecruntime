---
layout: default
title: Planner rules
description: The optimizer, AQE and DPP rewrites VecRuntime adds, in plain words — what each speeds up and how to turn it off.
---

# Planner rules

Most of VecRuntime's work is columnar operators. A handful of speedups instead need a different
*logical plan*, not a different operator — so they are optimizer rules the plugin injects. Each one is
general SQL (TPC-DS queries are the evidence, not the pattern), applies only where the rewrite is
exact, returns Spark's result, and has its own `spark.vecruntime.optimizer.*` switch (all default
**on**, and all off when the plugin is off). None of them changes a Spark AQE setting.

The detailed page — the exact shapes, why each is correct, the decline conditions and the measured
numbers — is the
[detailed planner-rules page](https://github.com/vecruntime/vecruntime/blob/main/docs/aqe-dpp-rules.md).

---

## Doing less work per row

- **`SelfJoinToAggregate`** — an existence-only self-join (`t1.k = t2.k AND t1.v <> t2.v`) becomes one
  `GROUP BY k HAVING min(v) <> max(v)`. Two rows with different values exist exactly when the min and
  max differ. (q95)
- **`MergeFilteredAggregates`** — several global aggregates over the same input that differ only in
  their `WHERE` clauses are computed in one pass, each keeping its own rows with a `FILTER`. (q9, q28,
  q88, q90)
- **`SharedAggregateInputs`** — when the same aggregate is used twice and Spark adds an `IS NOT NULL`
  to only one copy, the rule lines the two inputs back up so Spark can reuse the exchange instead of
  reading the input twice. (q1, q65)
- **`NarrowBelowJoin`** — a `substr`/`length` of a join side's string is computed *below* the join, so
  the join carries the short result instead of the whole string for every joined row. (q23a, q23b)

## Aggregating before the join

- **`AggregateBelowJoin`** — a fact summed per its join keys (customer, year, …) is pre-aggregated
  right above its scan, before the dimension joins, so far fewer rows are joined and shuffled. Planned
  as one local aggregate per task with no extra exchange, and it gives up on a key that does not
  repeat. (q4, q11, q74)
- **`CountBelowJoin`** — a join input that only supplies grouping columns is collapsed to its distinct
  rows plus a count, and the aggregate above weighs each row by that count. This kills join-output
  explosions no input-skew handling can see. (q6, q24a/b and ~24 others)
- **`DistinctBelowSemiJoin`** — the right side of a left semi / anti join (from `INTERSECT`, `EXCEPT`,
  `EXISTS`, `IN`/`NOT IN`) is reduced to its distinct join columns, since duplicates there change no
  answer. (q14a/b, q35, q94, q95 and others)

## Pruning partitions and rows at run time

- **`DppThroughAggregate`** — dynamic partition pruning carried through an aggregate, where Spark's own
  DPP stops because the join key is a grouping column rather than a partition column. (q2, q59)
- **`TransitiveDpp`** — dynamic partition pruning carried through a *second* join key, where the
  filtering dimension becomes selective only via an equality from the other side of the join. (q72)
- **`FactBloomFilter`** — a runtime bloom filter built from a smaller fact onto a much larger one, so
  rows with no match are dropped before the join. It fires only with evidence of selectivity (distinct
  counts, or a filtered creation side) and at run time, and uses a cache-friendly split-block filter
  with a vectorised probe. (q16, q50, q93, q94)
- **`RemoveRedundantGroupKeys`** — an AQE (runtime) rule: when a broadcast join's build key is unique,
  grouping keys that the key determines are dropped from the aggregate, shrinking its hash table and
  shuffle. (q23a, q23b, q39a, q39b)

Each rule's switch is `spark.vecruntime.optimizer.<name>.enabled`. The full list, with the exact
rewrites and measurements, is on the
[detailed planner-rules page](https://github.com/vecruntime/vecruntime/blob/main/docs/aqe-dpp-rules.md).
