---
layout: default
title: Supported expressions
description: Which Spark expressions VecRuntime compiles to a kernel, grouped by family, and which fall back to Spark.
---

# Supported expressions

`ExpressionCompiler.compile` is one match over Catalyst expressions: each one either becomes a kernel
or returns a reason, which the operator carrying it records as a fallback. An expression that compiles
runs on the lane types below; anything over a type without a lane, or an expression with no kernel yet,
falls back to Spark with the same result.

Spark 4 defaults to **ANSI mode**. Where ANSI changes the semantics (integer, decimal and division
overflow, invalid casts) the kernels raise the same error Spark does, for active rows only; the legacy
mode nulls instead.

The detailed page — every expression, its accepted types and its exact fallback string — is the
[detailed expressions page](https://github.com/vecruntime/vecruntime/blob/main/docs/expressions.md).

---

## Lane types

| Lane | Spark types |
|---|---|
| BOOL | `boolean` |
| INT32 | `int`, `date` |
| INT64 | `bigint`, `timestamp`, `decimal(p ≤ 18)` |
| FLOAT64 | `double` |
| UTF8 | `string` (compared in `UTF8_BINARY` order); collated strings are **carried** and used as keys, but character functions over them stay Spark's |
| DECIMAL128 | `decimal(p > 18)` as two 64-bit limbs |

`float`, `short`, `byte`, `binary`, `array`, `map`, `struct` and intervals have no lane.

## What compiles

| Family | Covered | Falls back |
|---|---|---|
| Comparisons | `= < <= > >= <> <=>`, `BETWEEN`, `IN`, `InSet`, `isnan` — over every lane incl. wide decimals and booleans | Operands of different Spark types until a cast compiles |
| Boolean / null | `AND` `OR` `NOT`, `IS [NOT] NULL` | A boolean *literal* operand |
| Arithmetic | `+ - * /`, unary minus, `% pmod`, `div`, `abs`/`positive`/`negative` over int/long/double and decimals (incl. 128-bit); `try_add`/`try_subtract`/`try_multiply`/`try_divide`/`try_mod` | Decimal `try_*`; wide-decimal `%`/`pmod`/`round` |
| Math | `sqrt` `cbrt` `exp` trig/hyperbolic, `ln` `log*`, `pow` `atan2` `hypot`, `greatest` `least` `nanvl`, `ceil` `floor` `round` `bround` `rint`, `sign` — bit-identical to Spark | — |
| Bitwise | `& \| ^ ~`, `shiftleft`/`shiftright`/`shiftrightunsigned`, `bit_count` over int/long | `bit_get` (returns tinyint, no lane); bytes/shorts |
| Strings | `startswith`/`endswith`/`contains`, `LIKE` (prefix/suffix/contains and multi-`%`), `instr` `locate` `replace` `translate` `substring_index` `split_part` `find_in_set`, `upper`/`lower`/`initcap`, `trim`/`ltrim`/`rtrim`, `concat`/`concat_ws`/`elt`, `length`/`octet_length`/`bit_length`, `ascii` `chr`, `substring`/`left`/`right`, `lpad`/`rpad` `repeat` `space` `overlay`, `md5`/`sha*`/`crc32` | `LIKE` with `_`/escape, `rlike`; non-literal patterns; character functions over a **collated** or **binary** column |
| Dates & times | `year` `month` `day` … `extract`, `trunc`/`date_trunc`, `date_add`/`date_sub`/`datediff`, `last_day`/`add_months`/`next_day`/`weekofyear`, `months_between`, `make_date`, the `unix_*`/`timestamp_*` family, `date_format`/`from_unixtime`, `hour`/`minute`/`second` | Zone-dependent datetime under a session zone **with DST rules** (only `date_format` over a timestamp takes any zone); parsing a string in `unix_timestamp` |
| Casts | int/bigint/double/boolean/decimal among themselves; narrowing (legacy wraps / ANSI raises); string ↔ number/boolean/date/timestamp; date ↔ timestamp (fixed-offset zone); to/from 128-bit decimals; `try_cast` | `→ float`/`binary`; `string → decimal`; a DST session zone for date↔timestamp; decimal `try_cast` |
| Conditionals | `CASE WHEN`, `IF`, `COALESCE`/`NVL`/`NVL2`/`NULLIF`/`IFNULL` | A wide-decimal or nested result |
| Nested reads | `col.field` (struct field chains), `size`/`cardinality`, `IS [NOT] NULL` over a nested column | `arr[i]`, `map[key]`, `arr.field` over an array of structs |
| Hashing / misc | `hash` (Murmur3), `xxhash64`, `BloomFilterMightContain` (runtime join filter), `monotonically_increasing_id` `spark_partition_id`, `ScalarSubquery`, `NormalizeNaNAndZero` | — |

## Aggregate functions

`count`, `sum`, `min`, `max`, `avg`, `first`, `last`, `max_by`/`min_by`, `bit_and`/`bit_or`/`bit_xor`,
`count_if`, the full moment family (`stddev*`, `var*`, `skewness`, `kurtosis`, `covar*`, `corr`,
`regr_*`), `collect_list`/`collect_set` and `bloom_filter_agg` all compile, with `FILTER` and
`DISTINCT`. Decimal `sum`/`avg` past 18 digits accumulate in 128 bits and emit Spark's exact buffer.

**Falls back:** probabilistic sketches (`approx_count_distinct`, `approx_percentile`,
`count_min_sketch`, `hll_*`), `percentile*`, `collect_top_k`, `listagg`, and `try_sum`/`try_avg` over a
decimal.

## Not planned

By decision, not omission: geospatial functions; building a struct/array/map from scalars
(`named_struct`, `array`, `map`, …); Avro/Protobuf codecs; JVM reflection (`reflect`, `java_method`);
niche string validators/encoders (`soundex`, `levenshtein`, `luhn_check`, `*_utf8`); and non-Arrow
Python UDFs / Scala UDFs as kernels (a row-based escape hatch is the planned path).

The full per-expression table and the `Planned` list are on the
[detailed expressions page](https://github.com/vecruntime/vecruntime/blob/main/docs/expressions.md).
