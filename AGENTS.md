# AGENTS.md

Working notes for agents and contributors changing vecruntime: the design decisions the code
embodies, the rules every change is held to, and what "done" means. `README.md` is the user-facing
description, `docs/results.md` holds the measurements, `docs/operators.md` / `docs/expressions.md`
the support matrices, `docs/comet.md`, `docs/iceberg.md` and `docs/flight-shuffle.md` the
integrations. This file is the contract behind them. Issue numbers (`#N`) are references to the
GitHub issue or PR where a decision was measured; the number that justifies a decision is kept here,
everything else lives in `docs/results.md`.

## 1. What this project is

A Spark SQL plugin that runs `Filter`, `Project`, `HashAggregate` (all four modes, spilling), `Sort`
(spilling runs), `Window`, `Expand`, `Generate`, the limit family, `Sample`, `Union` / `Coalesce`,
Iceberg's `MergeRows`, the hash, broadcast-nested-loop and sort-merge joins, and its own columnar
shuffle over Arrow-layout columnar batches with the Java Vector API (`jdk.incubator.vector`), in the
style of Apache DataFusion Comet but entirely on the JVM. It reads batches from Spark's vectorized
Parquet reader, Iceberg's vectorized reader or Comet's native scan, and emits unshaded Arrow vectors
that Spark's own `ColumnarToRowExec` consumes. With Comet configured it can also feed Comet's native
shuffle.

Non-goals, deliberately: no native code, no JNI, no bundled Parquet reader, no shading of Arrow, no
dependence on Comet or Iceberg at compile time.

Fixed versions (the parent `pom.xml` is the source of truth): Spark 4.1.x, Scala 2.13, JDK 25, Arrow
18.3.0 (the version Spark bundles), Comet 1.0.0 (built from source, for the Comet-backed tests and
benchmarks), Iceberg 1.11. Maven group `io.github.vecruntime`, artifacts `vecruntime-*`. Packages
`io.vecruntime.*`; what must live inside Spark's package-private APIs is
`org.apache.spark.sql.vecruntime.*`. Configuration keys are `spark.vecruntime.*`, JVM properties
`vecruntime.*` (the 0.0.2 rename; no aliases, the old `spark.vector.*` / `sparkvector.*` names are
unknown).

## 2. Layout and commands

| Module | Language | Contents |
|---|---|---|
| `kernels/` | Java 25 | `VectorBuffers` (Arrow-layout `MemorySegment`s), `VecType`, `Species`, `Platform`, the SIMD kernels (compare, bitmap, compact, arith, decimal and wide decimal, cast, agg incl. overflow-checked sums, hash, partition ids, grouped accumulators, `GroupKeyTable`), the sort (`SortKernels`, `RunMerge`), gather and `ColumnBuilder` kernels, and `reference/` (`ScalarReference`, `SortReference`), the scalar oracles the tests compare against |
| `spark/` | Scala 2.13 + Java | `VectorPlugin`, `VectorSparkSessionExtensions`, `VectorColumnarRule`, the expression compiler (`expr/`), the aggregate functions (`agg/`), the operators (`Vector*Exec`), `ArrowOutput`, the input adapters (Spark on-heap / off-heap, Arrow, Comet, Iceberg), the Comet bridge (`comet/`), the Vector Acceleration UI tab (`ui/`) |
| `shuffle/` | Scala 2.13 | the columnar shuffle (#288): `VectorShuffleExchangeExec`, `VectorShuffleManager` with its writer and reader, `PartitionedIpcWriter` / `PartitionedIpcFile`, `ShuffleCompression`, `StructFlattening`, the Flight data plane (`flight/FlightShuffle`) and the `VectorShuffleBackend` seam; `flight-core` lives here so the plugin jar never carries gRPC |
| `benchmarks/` | Java + Scala | JMH microbenchmarks (`AggBenchmark`, `CompactBenchmark`, `SortBenchmark`, `GroupKeyTableBenchmark`, `CrossingBenchmark`, `FlightShuffleBenchmark`, `ScatterBenchmark`, ...), the TPC-H / TPC-DS runners (`TpchRunner`, `TpcdsRunner`, `ClusterRunner`), the Iceberg CDC MERGE runner (`CdcMergeRunner`), the scripts under `benchmarks/scripts/` and the cluster manifests under `benchmarks/k8s/` |
| `spark-sql-tests/` | Scala 2.13 | Spark's `SQLQueryTestSuite` with the extension injected (`VectorSQLQueryTestSuite`); profile `spark-sql-tests` only, run by `benchmarks/scripts/run-spark-sql-tests.sh` |

Commands that are known to work. Always unset `JAVA_TOOL_OPTIONS` first (an IDE-set one breaks
Spark's JVM options) and point `JAVA_HOME` at a JDK 25:

```bash
unset JAVA_TOOL_OPTIONS; export JAVA_HOME=/path/to/jdk-25
mvn -B -q clean install                    # kernels + Spark suites, Comet suites skipped (~3 min)
mvn -B -q -Pcomet clean install            # also the Comet-backed suites (needs the Comet jar in ~/.m2)
mvn -B -q -Pcomet,iceberg clean install    # plus the Iceberg suites (Iceberg 1.11 runtime from Maven Central)
mvn -B -Pcomet,iceberg -pl kernels,spark,shuffle,benchmarks install   # THE GATE every change runs before a PR
mvn -pl kernels test -Dvector.jvm.args="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -Dvecruntime.vectorBits=512"
mvn -pl spark install -Dsuites=io.vecruntime.spark.VectorAggregateSuite   # one suite
benchmarks/scripts/run-spark-sql-tests.sh [regex]          # the SQL golden suite (section 7.2)
benchmarks/scripts/gen-tpch.sh 1           # DuckDB-generated TPC-H tables, decimals as doubles; 10 for SF10 (gitignored)
benchmarks/scripts/gen-tpch.sh 1 benchmarks/data --decimals   # the same with real DECIMAL(15,2), into sf1-decimal
benchmarks/scripts/gen-tpcds.sh 1          # 24 TPC-DS tables with real DECIMAL(7,2)/DATE columns, into benchmarks/data/tpcds-sf1
benchmarks/scripts/run-tpch.sh benchmarks/data/sf10 spark,vector,vector-shuffle --iterations 7 --warmup 5
benchmarks/scripts/run-tpcds.sh benchmarks/data/tpcds-sf1 spark,vector --queries q10,q35,q45
benchmarks/scripts/run-tpch.sh --report    # rewrite benchmarks/results/results.{md,html}; run-tpcds.sh --report likewise
benchmarks/scripts/gen-iceberg-mor.sh benchmarks/data/sf1   # Iceberg merge-on-read variants of lineitem (#260)
benchmarks/scripts/run-tpch.sh benchmarks/data/sf1 spark,vector --iceberg benchmarks/data/iceberg --variant sf1.pos_10 --queries q1,q6,probe-count,probe-sum,probe-group
benchmarks/scripts/run-cdc-merge.sh benchmarks/data/iceberg sf25.pos_20 spark,vector   # the Iceberg CDC MERGE benchmark
benchmarks/scripts/profile-query.sh benchmarks/data/sf10 vector q6   # one query under JFR + jfr-summary.sh (section 9)
benchmarks/scripts/submit-cluster.sh vector s3a://bucket/tpcds/sf1000/parquet sf1000-parquet s3a://bucket/results/sf1000-parquet   # cluster run (DRY_RUN=1 prints the spark-submit line)
benchmarks/scripts/run-tpcds.sh --cluster-report s3a://bucket/results/sf1000-parquet
```

Maven output is large: redirect to a log file and grep it. Full builds take about three minutes;
run them in the background and poll. When several worktrees build on one machine, build each in an
isolated Maven repository (`-Dmaven.repo.local=...`), or a parallel `install` overwrites the jar
under test.

## 3. Rules for every change (read first)

- **Arrow data structures, Comet-compatible.** Columns are Arrow layout end to end (validity bitmap,
  data, offsets, dictionary; `VectorBuffers`, section 5.4), so a batch crosses to Spark, Arrow and
  Comet without conversion: Comet's off-heap buffers are wrapped zero-copy and our batches go back to
  it through the Arrow C Data Interface (`ArrowCData`, section 5.11). A new structure that is not
  Arrow layout needs a measured reason, and it must not break the Comet hand-over in either direction
  (the `CometTest` suites under `-Pcomet` are the check). Arrow stays unshaded and Comet stays a
  runtime-only dependency.
- **Keep the garbage collector out of hot paths.** Per-row or per-batch objects in a kernel or an
  operator's inner loop are a defect: prefer native `MemorySegment`s from the batch's `Arena`,
  primitive arrays reused across batches, and struct-of-arrays over boxed or per-row objects. Take a
  GC-free structure when it is measured to be faster (JMH for a kernel, TPC-H/TPC-DS plus a JFR
  profile for an operator); if it is not faster, keep the simpler code. A JFR profile with GC pauses
  or allocation sites in our frames near the top is a finding to fix, not noise (for scale: the
  target-scan stage of the v3 CDC MERGE spends ~38 % of its task time in GC, an open finding, #20).
- **JFR before any explanation.** A number worse than expected, or better in a way you cannot
  account for, is profiled with Java Flight Recorder before a word is written about its cause
  (section 9). Do not write a hypothesis into the docs or the code; record the run and read the
  profile first.
- **Testing conventions.** Correctness first, by comparison with an oracle: the scalar reference for
  kernels, Spark itself for SQL (`checkVectorized` / `checkFallback`), byte-for-byte results for
  shuffle and Iceberg writes. Every change runs the gate above, exit 0; planner or expression changes
  also run the SQL golden suite with no arguments and must hold its coverage floor (section 7.2). A
  performance claim needs a measurement (JMH, or a benchmark run with matching checksums); a
  benchmark whose checksums differ is a correctness bug. Report exit codes and numbers, not
  impressions; never claim a run that did not happen.
- **No silent fallback.** An operator or expression that cannot be converted stays on Spark with a
  recorded reason; tests assert on the reason text, the UI shows it, `docs/operators.md` and
  `docs/expressions.md` list it. Every operator or expression change updates its row in the same
  commit.
- **Small, scoped diffs; docs in step.** Match the language split (Scala for planner, operators and
  expression compilation; Java for kernels and anything touching `MemorySegment` in a hot loop),
  document every new key in `README.md` and `docs/configuration.md`, note a changed default in
  `CHANGELOG.md`, and never lower a test count without saying why in the commit.

## 4. `kernels/` (Java, the Vector API)

- **The Java Vector API is the implementation.** Kernels are written against `jdk.incubator.vector`
  over `MemorySegment`s. No JNI, no native code.
- **One species.** `Species` is the single source of vector shape: the platform's preferred width by
  default (128 bits on NEON, 256 on AVX2, 512 on AVX-512), forced with
  `-Dvecruntime.vectorBits=128|256|512`. Kernels never pick their own species. The preferred width
  stays the default (TPC-H Q1 at SF10 was 23 % faster at 512 than 256 bits, #283).
- **Constant operators.** `lanewise(op, ...)` / `compare(op, ...)` are only intrinsified when `op` is
  a compile-time constant. Never pass a `VectorOperators` value through a variable or a parameter;
  switch on the operation and call the constant form.
- **`compress` only where it is native.** `Vector.compress` is one instruction on AVX-512 and SVE and
  emulated on NEON and AVX2. Compaction uses it wherever `Platform.NATIVE_COMPRESS` (13-28 % over the
  table on dense selections, #283) and the 256-entry shuffle table for 8-lane species elsewhere; a
  `compress` is never emulated. Two 64-bit lanes are not worth a shuffle: partial selection words take
  a scalar walk over set bits when the species has two lanes (`rearrange` lost to it on NEON); full
  words are bulk copied.
- **Platform dispatch** (`Platform.java`, #283): the SIMD target is probed once at class init from
  HotSpot's own `UseAVX` / `UseSVE` / `MaxVectorSize` (`-Dvecruntime.platform=neon|sve|avx2|avx512`
  overrides it, to emulate a foreign path or compare two on one machine) and folded into `static
  final` booleans: `MASK_REGISTERS` (AVX-512 only; `-Dvecruntime.maskRegisters=true|false` overrides)
  and `NATIVE_COMPRESS` (AVX-512, SVE). Kernels switch on those at the top of a loop, never per call,
  and never on CPU flags read at runtime, so a platform-specific decision leaves the other paths
  untouched by construction. Aggregate lane masks come from `VectorMask.fromLong` (one `kmov`) under
  `MASK_REGISTERS` (14-25 % faster on the null paths at 512 bits); broadcast-AND-compare stays for
  NEON, AVX2 and SVE -- on Graviton4 (JDK 25, 128-bit SVE) `fromLong` is not intrinsified and measured
  0.50-0.79x (#253, #484). Re-measure `-Dvecruntime.maskRegisters=true` on SVE after each JDK update.
- **Grouped aggregation thresholds** are the measured crossovers: `vecruntime.agg.maskPathMaxGroups`
  (one masked reduction per group up to this many groups, else a scatter into accumulators; default
  4 where the platform has mask registers and the double species has 4+ lanes, 1 elsewhere) and
  `vecruntime.agg.interleave` (accumulator copies the scatter rotates over; default 1 on AVX-512,
  4 elsewhere). Several copies sum doubles in a different order than Spark; `interleave=1` forces
  Spark's order in every double sum (the golden suite's JVM sets it).
- **Every kernel has a scalar twin** in `reference/ScalarReference` (or `SortReference`); the kernel
  suite (`kernels/src/test`) compares them on random data with nulls, selections and awkward lengths
  (tails shorter than a vector, batches not a multiple of 64), at 128, 256 and 512 bits. The wider
  widths are emulated on a NEON machine but are the only coverage the AVX2/AVX-512 paths have.
- **JMH for every kernel change.** A kernel is not faster until a JMH benchmark in `benchmarks/`
  says so, with the convention `-wi 2 -i 3 -w 1 -r 1 -f 1`; add one when a kernel has none. Several
  intuitive kernel changes were slower (the reversed assumptions in `docs/results.md`), so the
  benchmark decides, not the reasoning. Run the kernel benchmarks from `benchmarks/target/benchmarks.jar`;
  the ones needing Spark or Comet classes (`CrossingBenchmark`, `FlightShuffleBenchmark`) need the
  classpath the run scripts build (`benchmarks/scripts/run-flight-bench.sh` does it for the latter).
- **Allocation-free inner loops.** Kernels read and write `MemorySegment`s and caller-owned buffers;
  they do not allocate per row or per vector. Popcount and bitmap bookkeeping are `Long.bitCount`
  over 64-bit words and never show in profiles.
- **Do not reintroduce:** `MemorySegment.mismatch` on tiny ranges in a loop (deoptimised on q67,
  #493); ordered string compare walks bytes instead. A per-element `MemorySegment.get` in a join's
  pair loop compiles to a virtual call when the receiver profile mixes native and heap segments;
  `HeapMirror` (fixed-width columns copied once into Java arrays) exists for that (#332).
- **Decimals:** `Decimal128` is two little-endian `long` limbs per value (Arrow's `Decimal128`
  layout) with an exact `BigInteger` path for a row whose intermediate leaves 128 bits
  (`WideDecimalKernels`, `WideDecimalCastKernels`); an emulated lane-pair SIMD variant was rejected
  (#28). `DecimalKernels` rescales, range-checks and divides INT64 decimals Spark-exactly.
- **Sort:** `SortKernels.sortIndices` is an LSD radix sort over order-preserving unsigned key passes
  (int32/bool one pass, int64/double one, strings <= 8 bytes a length pass and a prefix pass, longer
  strings a rank from a stable merge sort, decimal128 four); doubles in Spark's total order, strings
  in `UTF8String`'s. Key normalisation is lane-parallel, the histograms, scatters and gathers are not
  (`SortBenchmark`). `RunMerge` k-way merges sorted runs with a loser tree and a widened leaf (a
  block of a winner's rows leaves together after a binary search).

## 5. `spark/` (planner, expressions, operators)

### 5.1 Planning: a Comet-style columnar rule with explicit fallbacks

- Operators are replaced by `VectorExecRule` (`VectorColumnarRule.preColumnarTransitions`,
  registered through `injectColumnar`), bottom-up. An operator is converted only if its child
  produces columnar batches of supported types (vectorized Parquet or Iceberg scan, Comet scan, or
  another vecruntime operator) and every expression compiles. Operators whose input is a shuffle
  (the merging aggregate modes, the shuffled hash join, the window) accept exchanges on types alone;
  Spark inserts `RowToColumnarExec` under them for its row shuffle.
- Everything not converted gets a reason attached as a `VectorFallback.Tag` on the original
  operator. Tests assert on the text (`checkFallback(..., reasonContains = ...)`), the UI shows it,
  `spark.vecruntime.explainFallback.enabled` logs it. Every operator has an
  `spark.vecruntime.exec.<kind>.enabled` switch (`docs/configuration.md`).
- Supported types are exactly `VecType`: BOOL, INT32 (also Date), INT64 (also Timestamp and
  `Decimal(p <= 18)` as the unscaled value), FLOAT64, UTF8, DECIMAL128 for `Decimal(p > 18)`.
  `TypeMapping.isSupported` (every kernel computes on it) is false for wide decimals while
  `TypeMapping.hasLane` is true: operators that merely move a column ask `hasLane`; what computes
  asks `isSupported`, and the compiler routes a wide operand to the wide kernels explicitly
  (`isWideDecimal`). Adding a type means `VecType` + `TypeMapping` + every kernel switch + the
  adapters + `ArrowOutput` + tests at all three vector widths.
- Every replacement operator reports `outputPartitioning` exactly as the Spark operator it replaces,
  *through its output aliases* (`VectorHashAggregateExec` and `VectorProjectExec` mix in Spark's
  `PartitioningPreservingUnaryExecNode`): `EnsureRequirements` ran before the replacement and planned
  on the strength of that claim; getting it wrong returned every row twice on TPC-DS q66 (#162). Run
  `run-tpcds.sh` at SF1 after any change to an operator's output contract.
- Columns without a lane pass through (#19): filter and project require only a columnar child;
  `VectorProjectExec.isPassThrough` columns are never compiled, dense batches borrow Spark's vector
  (`BorrowedColumnVector`), a selection is applied through `RemappedColumnVector`. Struct field
  access (`StructFieldExpr`, `NestedColumnRef` / `NestedFieldColumnVector`, `SizeExpr`,
  `NestedValidityExpr` in `expr/NestedExprs.scala`) resolves `GetStructField` chains to a leaf and
  folds the ancestors' nulls into the validity (#50); `arr[i]`, `map[key]`, constructors and the
  lambda families are refused with reasons naming #50.

### 5.2 Decimals and ANSI

- Narrow decimals ride on INT64 with the scale in the Spark type (`expr/DecimalExprs.scala`:
  `DecimalArithExpr`, `DecimalCastExpr` -- also serving `CheckOverflow` -- `UnscaledValueExpr`,
  `MakeDecimalExpr`). Output decimals are `VectorDecimalColumnVector` over a `BigIntVector`. The
  optimizer's `DecimalAggregates` turns `sum(decimal <= 8 digits)` into an ANSI bigint sum and
  `avg(decimal <= 11 digits)` into a double average; wider ones go through `WideDecimalSumAgg` /
  `WideDecimalAvgAgg` and their merge counterparts (Spark's `(sum: Decimal(p+10), isEmpty)` buffer,
  `GroupedAccumulators.WideLongSum`, the result emitted ready-made and forwarded by
  `compileFinalResults`).
- Speculative narrow decimals (#26): `ExpressionCompiler.speculativeDecimalArithmetic` compiles
  `* + -` over decimal(<=18) operands whose declared result is wide into `SpeculativeDecimalMulExpr` /
  `SpeculativeDecimalAddExpr`, INT64 products with a `Math.multiplyHigh` check per row and exact
  `BigInteger` escalation of the overflowing rows into the wide sum / avg (`Escalation`). Only
  `VectorAggregates`' sum and avg ask for it; `eval` on a speculative expression throws. Spark 4.1's
  `Multiply` carries a `NumericEvalContext` (read `m.evalContext.evalMode`), `Average` carries
  `evalMode` directly.
- Spark 4 defaults to ANSI mode. Double arithmetic is bit-identical in both modes; integer `+ - *`
  and negation are computed wrapping and checked with an overflow lane mask (`OverflowKernels`),
  raising Spark's `ARITHMETIC_OVERFLOW` with `Math.*Exact`'s message; `try_*` forms null those rows,
  `try_sum` poisons the group. ANSI errors are raised only for rows that are active (survive earlier
  conjuncts / the selection), matching Spark's short-circuit semantics (`VectorErrors`).
- Floating-point rounding is a configuration choice, like Comet's:
  `spark.vecruntime.exec.strictFloatingPoint` (default `true`) makes every double `sum` / `avg`
  bit-identical to Spark's (one accumulator per group, `AggKernels.sumDoubleSequential`); `false`
  restores lane-parallel and interleaved sums that differ in the last bits (TPC-H Q15 then returns
  no rows: a double sum compared for equality against Spark's subquery -- the benchmark checksum
  mismatch there is expected). The benchmark configurations run with `false` (`TpchRunner.VectorFast`).
  Tests compare other doubles at `1e-9` relative; assert bit equality on a double sum only under
  strict mode.

### 5.3 Expressions

Expressions compile to a small `VectorExpr` tree (`expr/VectorExpr.scala`; one file per family:
`CastExprs`, `PredicateExprs`, `MathExprs`, `RoundExprs`, `BitExprs`, `TranscendentalExprs`,
`DateExprs`, `DateArithExprs`, `DateFormatExprs`, `StringCaseExprs`, `StringConcatExprs`,
`StringLengthExprs`, `StringSearchExprs`, `StringSliceExprs`, `HashExprs`, `InjectedExprs`,
`NestedExprs`, `DecimalExprs`, `CaseWhenExpr`, `MonotonicIdExpr`). Rules that hold across them:

- **Every expression has a kernel, a scalar reference and a Spark comparison test.** Anything else
  is a `Left(reason)`; the reason lands in `docs/expressions.md`.
- **Results are Spark's, bit for bit where Spark's are deterministic.** Transcendentals make one
  scalar `Math` / `StrictMath` call per lane exactly as Spark does (the Vector API's 1-2 ulp
  operators are deliberately not used); string functions reuse `UTF8String`'s algorithms byte for
  byte; casts, formats and parsers that Spark implements with formatter objects run Spark's own per
  row (`SparkCasts`, `SparkFormatters`); `round` on doubles takes Spark's BigDecimal-of-toString path.
- **Later conjuncts see only active rows** (`EvalContext.active`, 64-row blocks with an undecided
  row); `CaseWhenExpr` carries per-branch win masks forward as `active` and blends with
  `SelectKernels.select`; literal branches are constant columns (`ArrowOutput.constant`, also for
  `SELECT 1 FROM ...`).
- **Dictionary strings are computed once per dictionary entry** where the function allows
  (`StringLengthKernels.perEntry`); kernels that produce new UTF8 data (`StringSliceKernels`) run two
  passes -- lengths, then one data buffer sized from their prefix sum -- with a 1 GiB per-batch cap.
- **Injected values** (`InjectedExprs.scala`): a scalar subquery is read as a literal at execution
  (`SubqueryLiteralExpr`); a merging aggregate whose function instance was rewritten between stages
  (`ReusedSubquery`) carries fresh buffer exprIds, so `VectorAggregates` binds buffers by position
  when the ids do not match. Time zones: `TimestampToDateExpr` / `TimeFieldExpr` only under a
  fixed-offset session zone; zones with rules fall back.

### 5.4 Memory model and batch lifecycle

- `VectorBuffers` is the only view kernels have of a column: validity bitmap, data, offsets (UTF8),
  optional dictionary, all Arrow layout. Batches live in native memory from a per-batch `Arena`.
- Spark's `OnHeapColumnVector` batches are copied once into native memory per operator chain
  (reading a heap segment in place measured at half the speed). Comet's and Arrow's off-heap buffers
  and the fixed-width columns of Spark's `OffHeapColumnVector` (`spark.sql.columnVector.offheap.enabled=true`)
  are wrapped zero-copy (`SparkColumnVectorBuffers.wrappedOffHeapColumns()` counts them); strings and
  dictionaries still take the copy, in bulk (#398). The benchmark configurations set the off-heap
  flag (#403).
- `VectorPrefetchScanExec` (`spark.vecruntime.scan.prefetch=1|2`, default 0) converts the reader's
  batches on a helper thread (`PrefetchingBatchConverter`) between a Spark or Iceberg vectorized file
  scan and our first operator; off by default because the reader, not the conversion, is the cost.
- Dictionary-encoded strings stay dictionary encoded end to end: the adapters forward Parquet
  dictionary indices, the filter compacts int32 indices, `VectorDictionaryColumnVector` lets Spark
  read them, the aggregate hashes the dictionary rather than every row, `ArrowOutput.gather` gathers
  through the codes into the dictionary. Decoding strings row by row was the first big profiling
  finding; do not reintroduce it.
- Batch lifecycle follows Spark's columnar contract: the producer may reuse or release a batch when
  the next one is requested. Operators finish with a batch before pulling the next and copy only
  what they keep (`ArrowOutput.copy` / `compact`).
- Bounded memory: operators that hold data stay within a budget and spill to local disk. The grouped
  aggregate acquires its footprint from Spark's `TaskMemoryManager` and past
  `spark.vecruntime.agg.spillThreshold` (1g) or a refusal, a buffer-emitting mode emits its table and
  starts over (`EmitAndReset`, with a pass-through once a full table shows the input does not reduce,
  `spark.vecruntime.agg.passThroughRatio`), a merging mode spills into hash buckets (`GraceHash`,
  `AggregateSpill`; #363, #367, #376). The sort spills sealed runs past `spark.vecruntime.sort.spillBytes`
  (1 GiB, #416). The shuffled hash join splits both sides into buckets on disk past
  `spark.vecruntime.join.spillBytes` (`GraceHashJoin`, #416). The broadcast joins hold the relation
  per task and are gated by `spark.vecruntime.join.maxBuildSize`. The window does not spill.
- A cached table is a columnar input only when Spark's `DefaultCachedBatchSerializer` says so, on
  the cached relation's *whole* schema (primitive types only); an Arrow `CachedBatchSerializer` (#55)
  would lift that.

### 5.5 Selection vectors between our operators

- A filter feeding another vecruntime operator forwards a `SelectedColumnarBatch` (input batch plus
  a selection bitmap) instead of compacting when at least `vecruntime.selection.minFraction` (0.5) of
  the rows survive; below that it compacts (Q6 at 2 % forwarded made project and aggregate walk all
  rows). `spark.vecruntime.exec.selection.enabled` turns forwarding off. `VectorSampleExec` is a
  selection producer too. The rule only forwards selections into our own operators
  (`markSelectionProducers`); `VectorPassThrough` operators (union, coalesce, expand) do not count
  as consumers, so a filter below them is not marked.
- Foreign batch shapes are normalised where batches enter our operators (`InputBatches.normalize`
  from `VectorBatchIterator.hasNext` and `EvalContexts.withBatch`): Iceberg's merge-on-read row-id
  mapping becomes a `SelectedColumnarBatch` over the physical rows. Any consumer that sizes scratch by
  the input must take the physical count from `ctx.numRows`, not `batch.numRows()` (the aggregate
  silently dropped rows once).
- Columns a project forwards unchanged are borrowed (`BorrowedColumnVector`), not copied.

### 5.6 Aggregation (`VectorHashAggregateExec`, `agg/`)

- All four modes are ours: update vs merge per aggregate expression, buffers vs results per operator
  (`VectorAggregatePlanner.emitsResults`; a mix is refused). Merge functions reuse the accumulators;
  result expressions are compiled with `evaluateExpression` substituted, keyed on the operator's
  `aggregateAttributes`. `FILTER` clauses apply in the update modes (`FilteredAggregates.scala`);
  `DISTINCT` is a marker only. A string buffer makes Spark plan `SortAggregateExec`; the rule builds
  the same hash operator and drops the sort below when it is exactly the required one.
  `spark.vecruntime.exec.aggregate.final.enabled` turns the merging modes off.
- Kernel-backed `count` / `sum` / `min` / `max` / `avg`; scalar per-row accumulators in
  `ExtraAggregates.scala` (`OrderedMinMaxAgg` for booleans and strings, so `bool_and` / `every` /
  `any`; `FirstAgg` / `LastAgg`; `BitAgg`; `MaxMinByAgg`; `CountAllAgg`); the statistical family in
  `StatAggregates.scala` (`MomentsAgg`: Spark's Welford step in partition order for update, Spark's
  `mergeExpressions` for merge, compared at a relative tolerance).
- `GroupKeyTable` memoises group ids per combination of dictionary indices when every key is
  dictionary encoded and the product of dictionary sizes is small (bounded by the batch, #416).
  UTF8 keys are kept by id in a per-column dictionary (`spark.vecruntime.agg.dictionaryKeys`, #377:
  the keys are emitted dictionary-encoded, which pays when the consumer is the shuffle writer) or as
  contiguous byte records for a plain consumer (a final aggregate feeding a sort or the result: q67's
  300k-entry key at 1 TB was +36 % by ids). `GroupKeyTableBenchmark` puts the memo crossover at a few
  hundred distinct values.
- A partial aggregate over our `Expand` (`ROLLUP` / `CUBE` / `GROUPING SETS`) aggregates the finest
  grouping once and rolls the partials up (`RollupRewrite`, `VectorRollupExec`,
  `spark.vecruntime.exec.aggregate.rollupRewrite.enabled`, #383).

### 5.7 Sort, limits, pass-through operators

- `VectorSortExec` replaces `SortExec` only over a columnar child (our shuffle, Comet's, or one of
  our operators for a local sort); over Spark's row shuffle the reason is "child ... is not
  columnar" -- converting rows to sort them gains nothing. The partition is sorted in runs of
  `spark.vecruntime.sort.runRows` (1M) rows, each sealed into its columns, keys and permutation
  (`VectorSortIterator`); several runs are k-way merged by `RunMerge` and gathered through
  `ArrowOutput.gatherRuns`; runs past `spark.vecruntime.sort.spillBytes` go to local Arrow IPC files
  in sorted order and take part in the merge as refillable runs (#285, #416). Under a limit each run
  keeps only its first `n` rows. Oracle: `SortReference`; `SortKernelsTest` compares permutations,
  `VectorSortSuite` compares against `SortExec` positionally on the key columns only (ties in
  non-key columns may differ).
- `VectorTakeOrderedAndProjectExec` (`ORDER BY ... LIMIT n`): the sort iterator with a `limit` per
  partition, then Spark's own single-partition shuffle, `LazilyGeneratedOrdering` and
  `UnsafeProjection` through `VectorRowStages` (UnsafeRow copies -> shuffle -> one on-heap batch).
  `VectorLocalLimitExec` / `VectorGlobalLimitExec` / `VectorCollectLimitExec` share
  `VectorLimitIterator` (whole batches pass, the boundary batch is compacted, the child is not pulled
  again). `OFFSET` falls back in both.
- `VectorUnionExec` / `VectorCoalesceExec` / `VectorExpandExec` are `VectorPassThrough`: they forward
  children's batches unchanged. The union keeps Spark's partitioning contract (`outputPartitioning`
  as `UnionExec` would report; co-partitioned children read together) and is planned whatever the
  column types, because Spark 4.1's own `UnionExec` has a concatenating columnar path over columnar
  children that would otherwise run (#128). The expand emits one batch per projection per input
  batch: column slots borrowed, `NULL` slots `ArrowOutput.nulls`, other literals constant columns;
  the input is held until its last projection is released.
- `VectorSampleExec` seeds Spark's `BernoulliCellSampler` with `seed + partitionIndex` and draws once
  per live row, so rows match Spark's for a seed. `VectorGenerateExec` (#59) replaces `GenerateExec`
  for `explode` / `posexplode` over a nested column path with a lane element type; Spark's
  `InferFiltersFromGenerate` and `NestedColumnAliasing` rewrites compile so the chain stays columnar.
  `VectorLocalTableScanExec` (off by default, `spark.vecruntime.exec.localTableScan.enabled`) writes
  a local relation into on-heap batches; tests over `VALUES` must exclude `ConvertToLocalRelation`.
- `VectorMergeRowsExec` (#21, #273) is Iceberg's `MergeRows` operator; its join is ours with a
  lane-less payload column (`_partition` beside `_file` / `_pos` / `_spec_id`) passed through on the
  streamed side as a `RemappedColumnVector`; a lane-less column on the build side is still refused.
  The write stays Spark's. `VectorMergeRowsSuite` compares against Spark's operator.

### 5.8 Window (`VectorWindowExec`, #58)

- Requires exactly `WindowExec`'s distribution and ordering; accepts any child on types because the
  sort below is Spark's without a columnar shuffle. `VectorWindowPlanner` maps: ranking functions
  (`rankKind`: `row_number` / `rank` / `dense_rank` streaming in `VectorWindowIterator`;
  `percent_rank` / `cume_dist` / `ntile` on the held path), whole-partition frames (Complete-mode
  aggregates reusing `VectorAggregates.compile` with the partition ordinal as group id), running
  frames (`frameKind`: RunningRows / RunningRange, prefix combiners per slot; functions without a
  slot-wise prefix are refused), sliding `ROWS` frames with literal bounds (`slidingAggregate`, the
  O(n x frame) re-aggregation Spark also does, so double sums are bit-identical), and the offset
  family `lag` / `lead` / `first_value` / `last_value` / `nth_value` (`offsetWindows`, in
  `VectorWindowOffsetIterator`).
- The held path borrows output columns from held copies: a released batch stays until no later
  batch can address its partitions, then until the consumer has moved past its output, and only then
  is closed -- do not shortcut this, `lag` reads earlier batches after they were emitted.
- Refused with a reason: `RANGE ... n PRECEDING` (value comparisons), `IGNORE NULLS`, decimal
  aggregates in Complete mode (gate on #28), double keys, two frame kinds or an offset function
  beside an aggregate in one operator.
- `VectorWindowGroupLimitExec` replaces Spark's `WindowGroupLimitExec` (Partial and Final) keeping a
  row while its ranking value is at most k -- tighter than Spark's pre-filter, exact because the
  window above computes the real ranks. Spark plans no group limit past
  `spark.sql.optimizer.windowGroupLimitThreshold`, and `row_number() ... <= k` without a partition
  becomes `TakeOrderedAndProject`; use `rank()` to get the group-limit shape in a test.

### 5.9 Joins (`VectorHashJoinExec.scala`, `VectorSortMergeJoinExec.scala`, `GraceHashJoin.scala`)

- `VectorBroadcastHashJoinExec` replaces `BroadcastHashJoinExec` when the streamed side is columnar
  or an exchange; the build side stays Spark's `BroadcastExchangeExec` / `HashedRelation`, read once
  per task into a `GroupKeyTable` (`HashedRelationAccess` in `org.apache.spark.sql.execution.vector`,
  the relation being `private[execution]`). `VectorBroadcastNestedLoopJoinExec` runs the same
  `VectorHashJoinIterator` with "every build row" as the candidate step, in bounded pair chunks.
  `VectorShuffledHashJoinExec` replaces `ShuffledHashJoinExec`, `ClusteredDistribution` on both
  sides, spilling into buckets past `spark.vecruntime.join.spillBytes`.
- Supported: inner, left / right / full outer (full outer on the shuffled join only), left semi,
  left anti, existence, each with an optional non-equi condition compiled against `left ++ right` and
  evaluated over a gather of only the columns it reads (`conditionRefs`; the rest are
  `PlaceholderColumn`s). Inner joins evaluate the condition before the output gather (`survivors`);
  a `lane OP lane` condition over mirrored INT32 / INT64 lanes is tested per pair on the `HeapMirror`s
  inside `emitMatches` (`PairPredicate`) so a failing pair is never appended (TPC-DS q72 26 s -> 7.4 s,
  #332). Null-aware anti joins (`NOT IN` over nullables) are the broadcast path plus the two
  singleton cases. Refused: skew joins on the hash paths, double keys (Spark normalises NaN / -0.0,
  the table compares bits).
- `SortMergeJoinExec` under `spark.vecruntime.exec.sortMergeJoin.mode` = `off` | `hash` | `merge` |
  `auto` (default `auto` since #311; the boolean `.enabled` key reads `false` as `off`, `true` as
  `auto`). `hash` re-expresses it as our shuffled hash join with the required sorts stripped
  (`VectorJoinPlanner.sortMergeBuildSide`, `sortMergeInputs`); `merge` plans `VectorSortMergeJoinExec`
  over Spark's sorted inputs (any SMJ type, skew joins included; streams the first input, buffers the
  second run by run with `RunKernels.boundaries`; a run at a batch edge is copied into its own
  *confined* arena -- a shared arena's close is a handshake with every thread and made q17 a hundred
  times slower). `auto` (`markSortMergeJoins`) takes the merge join where the row order is visible
  (below a limit, a sort or a range-partitioned exchange) or relied on by the parent, and otherwise
  the hash rewrite when a side's statistics fit `spark.vecruntime.join.hashMaxBuildSize`
  (`sortMergeEligibility`, memoised bottom-up; `estimatedBuildSize` prefers a materialised AQE
  stage's `computeStats()` and caps a logical estimate by a stage reachable through unary nodes, #329),
  else the merge join. The choice and its reason are the `SortMergeChoice` / `SortMergeWhy` tags
  both operators print. A hash join below a merge join gets a `VectorSortExec` back (`resortedMerge`);
  a wrong eligibility guess costs a sort, never a row. The #311 size gate that left large merge joins
  to Spark was removed in #416 once the sort spills (at 1 TB it had sent 70 of 101 TPC-DS queries to
  Spark's operator). `VectorSortMergeJoinSuite` compares row order, the hash suites row sets.
- Both joins are `VectorBinaryExec`; `VectorPlan` is the base the rule, selection marking, Comet
  bridging and the UI classify on. A columnar broadcast exchange of our own is a listed gap (#325).

### 5.10 The Arrow compatibility layer

Everything that crosses a boundary goes through one of three seams; new sources or sinks plug into
these rather than adding special cases to operators.

- Input: `ColumnVectorAdapters.adapt(ColumnVector, numRows, arena)` turns any Spark `ColumnVector`
  into `VectorBuffers`. Zero-copy adapters are tried first (`VectorArrowColumnVector` /
  `BorrowedColumnVector`, then anything registered through `ColumnVectorAdapters.register`, which is
  how the Comet and Iceberg adapters join without a compile-time dependency); anything else is copied
  by `SparkColumnVectorBuffers`. `adaptedColumns()` / `copiedColumns()` count the two outcomes;
  `VectorUnknownSourceSuite` pins the copy fallback with a test-only DSv2 source
  (`test/UnknownColumnarSource.scala`).
- Output: `ArrowOutput` writes result columns as unshaded Arrow 18.3.0 vectors (`ArrowSegments`,
  `VectorAllocators`) wrapped in Spark's `ArrowColumnVector`, so `ColumnarToRowExec`, Spark's Arrow
  paths and any Arrow consumer work unchanged. The Arrow version stays the one Spark bundles.
- Between our operators: `SelectedColumnarBatch` (section 5.5); a foreign consumer never sees one.

### 5.11 The Comet compatibility layer (`comet/`, `VectorToCometExec.scala`, `PreferComet.scala`)

- No compile-time dependency on Comet. `CometVectorAdapter`, `CometBatchBridge`, `CometMixedBridge`
  and `VectorToCometExec` / `CometShuffle` resolve Comet classes reflectively and register only if
  Comet is on the classpath. `-Pcomet` adds the jar for tests; the scripts add it when `COMET_JAR`
  is set.
- Comet's shaded Arrow and Spark's unshaded Arrow are the same version and cannot share a class.
  Rejected, do not retry: `arrow-c-data` on our classpath (Comet keeps `org.apache.arrow.c.*`
  unshaded with shaded signatures), maven-shade relocation (Comet's JNI looks classes up by name), a
  bulk-copy bridge. What works is sharing memory: `ArrowCData` writes Arrow C Data Interface structs
  with the FFM API and `Linker.upcallStub` release callbacks, Comet's `ArrowImporter` imports them
  over our buffers; `CometShuffleSuite` asserts every export is released. A DECIMAL128 lane is
  exported as its 16-byte layout, only INT64 decimal lanes are widened (word-by-word widening of a
  wide lane doubled every value, #281).
- The rule rewrites a `ShuffleExchangeExec` above a `VectorPlan` into Comet's native shuffle over
  `VectorToCometExec` for hash, single, round-robin and range partitioning (range needs Comet's
  `spark.comet.shuffle.native.partitioning.range.enabled` and our
  `spark.vecruntime.comet.shuffle.range.enabled`; its sampling pass never closes the imported
  vectors, so the bridge `releaseOutstanding()`s them on task completion). Requires
  `CometShuffleManager` and `spark.comet.exec.shuffle.enabled`; `spark.vecruntime.comet.shuffle.enabled`
  turns it off. Our own shuffle takes precedence only where Comet's is not configured.
- Mixed chains (#280, `spark.vecruntime.comet.mixed.enabled`, default off): Comet above ours through
  a `CometSinkPlaceHolder` built reflectively in `CometMixedBridge`, then Comet's `CometExecRule` on
  the parent. Rejected, do not retry: `sparkToColumnar` (leaf-only, copies) and a placeholder
  directly over our export node (`None.get` in Comet's `buildNativeContext`). The allowlist
  `spark.vecruntime.comet.preferComet` (#281) tags a listed operator `VectorFallback.Delegated`;
  if Comet declines it, our conversion runs, so a requested swap never lands on Spark's operator.
  Adding an entry needs the three-part rule of `docs/comet.md` (Comet faster by 2x the crossing cost
  where the operator dominates; no query regressed; a profiled reason). Settled by the #281 study:
  never delegate a blocking Comet consumer above our chain (it pins every exported batch); a
  projection or filter between two of our operators never pays for its two crossings; a delegated
  child must count as columnar for its parent (`columnarChild`); Comet's joins are the one swap that
  wins. The crossing cost is a known number (`CrossingBenchmark`: ~2.7-3 µs per column into Comet,
  0.1-0.25 µs back, a dictionary string decoded on the way in at 20-25 ns per row).
- Not combined with Comet: Comet's Final aggregate over our partials and native blocks. Comet's
  `LargeVarCharVector` falls back to the copying adapter.

### 5.12 Iceberg (`iceberg/IcebergVectorAdapter.java`, `-Piceberg`)

- The read side adapts Iceberg's vectorized batches in place: nulls from Iceberg's byte-per-row
  holder into a validity buffer in the scratch arena, strings as Parquet dictionary indices,
  int-backed and dictionary-encoded small decimals (#476, #491); merge-on-read deletes (v2 positional
  and equality, v3 deletion vectors) arrive as `ColumnVectorWithFilter` wrappers and are normalised
  into a selection (section 5.5). `docs/iceberg.md` has the variant table. The delete cost is a fixed
  per-batch price inside Iceberg's reader (`buildRowIdMapping`), paid by both engines; a direct
  bitmap from a deletion vector would need the reader to hand out the index (upstream, #261).
- The write stays Spark's; rebalance exchanges before an Iceberg write are sized by rows and by our
  shuffle's bytes per row (`spark.vecruntime.shuffle.rebalance.*`, #485, #495, #506).
- In progress, not on `main`: an Iceberg v3 deletion-vector writer in an optional `iceberg-bridge`
  module (PR #509). Until it merges, nothing in this repository writes deletion vectors.

### 5.13 The Vector Acceleration UI tab (`ui/`)

- Attached from the driver plugin; lives under `org.apache.spark.sql.vecruntime.ui` because
  `SparkUITab`, `WebUIPage` and `UIUtils` are `private[spark]`. It never influences execution: every
  listener callback and the attachment itself are wrapped so a UI failure cannot fail a query.
- Engine classification is by operator identity (`PlanAcceleration`: a `VectorPlan`, a class in
  Comet's packages, a columnar leaf, a transition), not by tags. "Fully accelerated" means no operator
  runs on plain Spark; scans, `ColumnarToRow` / `RowToColumnar` and AQE's shuffle readers are plumbing
  and count for neither side (`AQEShuffleRead` must not be coloured as a conversion).
- Spark 4 ships Bootstrap 4 and jQuery 3.5: use Spark's own `collapseTable` from `webui.js` and its
  CSS classes, not Bootstrap 5 `data-bs-*` attributes. The DAG uses the d3 / dagre-d3 bundles Spark
  already serves.

### 5.14 Tests per change in `spark/`

- Every operator or expression change adds or extends a `VectorQuerySuite` comparison
  (`test/VectorQuerySuite.scala`): `checkVectorized` runs the same SQL with `spark.vecruntime.enabled`
  toggled on one session, compares rows (`1e-9` for doubles) and asserts the expected operators in
  the final post-AQE plan; `checkFallback` asserts the Spark operator stayed and the recorded reason
  contains the expected text. Suites: `VectorFilterSuite`, `VectorProjectSuite`,
  `VectorAggregateSuite`, `AggregateSpillSuite`, `VectorSortSuite`, `VectorLimitSuite`,
  `VectorUnionSuite`, `VectorExpandSuite`, `VectorWindowSuite`, `VectorGenerateSuite`,
  `VectorSampleSuite`, `VectorDecimalSuite` (exact, no tolerance), `VectorWideDecimalSuite`,
  `VectorNarrowIntSuite`, `VectorJoinSuite`, `VectorSortMergeJoinSuite`, `VectorMergeJoinGateSuite`,
  `GraceHashJoinSuite`, `VectorMergeRowsSuite`, `VectorScanSuite`, `VectorPrefetchScanSuite`,
  `VectorCacheSuite`, `VectorUnknownSourceSuite`, the adapter / Arrow suites, `VectorPluginSuite`
  and `SparkOnJdkSmokeSuite`.
- The ported DataFusion Comet matrices -- `VectorPortedCometExprSuite`, `VectorPortedCometCastSuite`,
  `VectorPortedCometAggregateSuite`, `VectorPortedCometJoinSuite`, `VectorPortedCometWindowSuite`
  (#497-#510) -- must keep passing; a gap they reveal is fixed or recorded in `docs/compatibility.md`.
- Planner or expression changes run the SQL golden suite and hold its coverage floor (section 7.2).
- Comet: `CometScanSuite`, `CometShuffleSuite`, `CometMixedChainSuite`, `CometMixedShuffleSuite`,
  `CometPreferCometSuite` (tag `CometTest`, `-Pcomet`). Iceberg: `IcebergScanSuite` (tag `IcebergTest`,
  `-Piceberg`) and `CometIcebergSuite` (both), sharing `IcebergMorSuiteBase`: positional deletes,
  deletion vectors, equality deletes, a merge-on-read lineitem for Q1/Q6, and a `MERGE INTO` over a
  mutated table with the plugin on and off whose results must match row for row, plus adapter
  counters proving columns were adapted in place.
- UI: `PlanAccelerationSuite` (stand-ins in `org.apache.spark.sql.comet`, `FakeCometExec.scala`),
  `VectorAccelerationUiSuite` (a real Spark UI fetched over HTTP).
- Operator changes are profiled with JFR on TPC-H / TPC-DS (`profile-query.sh`) and GC and
  allocation in our frames must not have grown.

## 6. `shuffle/` (the columnar shuffle, #288)

`docs/flight-shuffle.md` is the full design; these are the rules and the decisions the code depends
on.

- **Design.** `VectorShuffleExchangeExec` (a `ShuffleExchangeLike`, so AQE's coalescing, skew
  splitting and local reads apply unchanged) replaces `ShuffleExchangeExec` above a vecruntime
  operator when `spark.vecruntime.shuffle.enabled` is on, the `vecruntime-shuffle` jar is present and
  `spark.shuffle.manager` is `org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager`; hash,
  round-robin, single and range partitionings. Partition ids come from `PartitionKernels` (kernels
  module), Spark's `Murmur3_x86_32` verbatim so `hashPartitionIds` equals `Pmod(Murmur3Hash(keys), n)`
  for every type (`PartitionKernelsSuite` is a property test against Spark's expression). Struct
  columns and struct keys travel flattened (`StructFlattening`, #480).
- **Arrow IPC on disk and over the wire; no private wire format.** The map output
  (`PartitionedIpcWriter`) is one Arrow IPC stream per record batch, several per reduce partition,
  concatenated into the resolver's data file with a per-partition index (#340), committed through
  Spark's `IndexShuffleBlockResolver` so `MapStatus`, the index file and Spark's block transfer all
  work on it. Record batches are sized by the writer (`spark.vecruntime.shuffle.batchRows` 8192 /
  `batchBytes`), not the input: per-(input batch, partition) slices made shuffled joins 2x slower than
  the row shuffle (#349, #351). Strings travel as ids over one task-wide `StringDictionary` per column
  whenever the dictionary pays (`spark.vecruntime.shuffle.writer.dictionaryMaxRatio`, #416; frozen to
  plain UTF8 past the cap), written once per map file; under `backend=block` the writer keeps per-block
  dictionaries because Spark's transfer delivers a block's bytes alone. Small decimals travel as int64
  with the Spark type in field metadata.
- **Compression:** bodies are zstd through zstd-jni (`spark.vecruntime.shuffle.compression`; raw IPC
  wrote 1.8x Spark's lz4 bytes and Arrow's pure-Java lz4 was an order of magnitude too slow). Codecs
  come from `ShuffleCompression.Factory`, never Arrow's `CommonsCompressionFactory`: arrow-java
  18.3.0's zstd codec may write 8 bytes past the compressed buffer into a neighbouring column
  (apache/arrow-java GH-1116; `PartitionedIpcSuite` reproduces it) and `SafeZstdCodec` passes the
  right capacity.
- **Memory:** flushes are decided by the writer allocator's *real* allocation, bounded by
  `spark.vecruntime.shuffle.writer.memoryLimit` (1g) and `flushBytes` / `bufferBytes`; Arrow's
  `OutOfMemoryException` is rethrown as a serializable `SparkException` (Spark's
  `SerializationDebugger` dies on JDK 25 otherwise, SPARK-55679, fixed in 4.2 only; see `upstream/`).
  The reader owns batch memory and closes a batch when the next one is produced; local segments are
  read positionally into Arrow memory with no per-batch arrays.
- **AQE sees Spark-scale sizes.** The exchange's `dataSize` metric is the *uncompressed* Arrow bytes
  (a zero there turned every shuffled join into a broadcast join), and map output sizes are reported
  scaled to Spark's estimated bytes per row (`spark.vecruntime.shuffle.aqe.mapSizeScaling`,
  `sparkCompressionRatio`, #511, #514: our shuffle is 1.6-4.3x smaller and AQE packed twice Spark's
  rows into a task; q67 92 s with a 115 GB spill -> 39 s with none). Rebalance exchanges are sized by
  rows (`RebalanceAdvisory`, `RowProportionalSizes`, #485, #495, #506). A change that affects AQE's
  view of map outputs is measured on TPC-DS and the CDC MERGE.
- **Flight data plane** (`flight/FlightShuffle`): one `FlightServer` per executor started by the
  executor plugin and registered with the driver plugin, one `DoGet` per executor and reducer carrying
  all of that executor's blocks for the reducer (#347), IPC bytes streamed straight from the files as
  4 MB chunks and decoded by the same `PartitionedIpcFile.StreamReader` a local block goes through.
  **Not** re-framed as Flight record batches (#338: Flight writes dictionaries once per stream, our
  blocks carry replacement dictionaries -- one machine never shows the resulting wrong strings, a
  cluster did). Streams are opened at the first `hasNext`, not in the constructor (#416).
  `spark.authenticate`'s secret is the bearer token; TLS refuses to start rather than serve in the
  clear. gRPC runs on the Netty 4.2 Spark bundles.
- **Stay Spark-compatible.** `VectorShuffleManager` handles only `VectorShuffleDependency` and
  delegates everything else to Spark's sort shuffle; a failed remote fetch is Spark's
  `FetchFailedException` with the block's map index so the lost map outputs are recomputed (#364);
  data and index files are deleted on `unregisterShuffle` by tracked map task id (#358); the reader
  merges task-level read metrics in its task-completion listener (Spark's does it in a completion
  iterator) or the stage shows zero bytes read; `PluginContext.hostname()` throws on an executor and
  the executor plugin initialises before the block manager, so the resolver is looked up at the first
  request. `spark.vecruntime.shuffle.backend=flight|block|<class>` is the `VectorShuffleBackend` seam
  where a push-based service (Celeborn-style) would plug in -- future work.
- **Low garbage on both paths:** roots and the serialised schema message reused across batches, a
  batch adapted once for ids and streams, no per-batch arrays in the reader (`vecruntime.shuffle.reader.coalesceRows`
  / `passEncodedRows` are the reader's JVM knobs), the scatter-based staged flush off by default
  (`spark.vecruntime.shuffle.writer.scatterFlush`, #487-#490). No fsync of map outputs, as Spark (#496).
- **Tests per change:** `VectorShuffleSuite`, `PartitionedIpcSuite`, `FlightSmokeSuite`,
  `FlightBlockStreamSuite`, `FlightShuffleClusterSuite`, `ShuffleManagerNameSuite`,
  `RebalanceAdvisorySuite`, `RowProportionalSizesSuite`, and `PartitionKernelsSuite` in `spark/`.
  Results must be byte-identical to the row shuffle's (the TPC-DS checksums under `vector-shuffle`).
- **JMH for writer or reader hot paths** (`FlightShuffleBenchmark` through
  `benchmarks/scripts/run-flight-bench.sh`, an A/B of two checkouts' JSON; `ScatterBenchmark`), then a
  cluster run for the end-to-end effect.

## 7. `benchmarks/` and `spark-sql-tests/`

### 7.1 Benchmarks

- `TpchRunner` (22 queries, `TpchQueries`) and `TpcdsRunner` (the 103 queries from Spark's
  `tpcds/q*.sql` resources, sharing the engine through `TpchRunner.Suite`) compute a checksum of every
  configuration's result rows to 10 significant digits and report whether all configurations agree.
  A run where checksums differ is a correctness bug, not a performance result (documented exceptions:
  TPC-DS q65's ties, TPC-H Q15 under `strictFloatingPoint=false`). The per-query acceleration column
  (`k/n` operators ours) and `operatorTimes` (per plan node: class, engine, rows, ms; Comet's from
  DataFusion's `elapsed_compute`) are in every results row; `--report` prints the operator matrix.
- Configurations live in `TpchRunner.Configs` (`VectorFast` is the base of every `vector*` one) and
  in the `ENGINE` arrays of `benchmarks/scripts/submit-cluster.sh`, kept in step by hand; the runner
  warns when the session disagrees with its label. Change a setting in both and in README's "The
  benchmark configurations" in the same PR. `vector` today: the plugin, `strictFloatingPoint=false`,
  `sortMergeJoin.mode=auto`, the vectorized Parquet reader made explicit, off-heap column vectors;
  `vector-shuffle` adds `VectorShuffleManager`; the `comet-*` and `hybrid` configurations need `COMET_JAR`.
- `ClusterRunner` / `submit-cluster.sh` submit one (configuration, dataset) pair as a Spark
  application; `benchmarks/k8s/` holds the image, the `SparkApplication` manifest, `run-matrix.sh`
  and `render-run.sh` for the EKS campaign; `run-tpcds.sh --cluster-report` reads a prefix of results.
- `CdcMergeRunner` / `run-cdc-merge.sh` is the Iceberg merge-on-read CDC benchmark (reads plus a
  timed `MERGE INTO`, the table rolled back to the pinned snapshot between runs); `run-iceberg-mor.sh`
  runs it on the cluster; `gen-iceberg-mor.sh` builds the lineitem variants and writes a
  `README-<namespace>.md` with live rows and snapshot ids -- the oracle.
- Results are appended to `benchmarks/results/<config>.jsonl` (committed); the report takes the
  latest measurement per (dataset, config, query). Numbers and their interpretation go to
  `docs/results.md`; a discarded run is noted there.

### 7.2 The SQL golden suite (`spark-sql-tests/`)

`VectorSQLQueryTestSuite` runs Spark's own `sql-tests/inputs/*.sql` with the extension injected
(`benchmarks/scripts/run-spark-sql-tests.sh [regex]`, profile `spark-sql-tests`, about 15 minutes;
the script builds with `-Piceberg` because the spark module's test sources only compile with it).
Last full run: 642 test cases pass, 111 ignored (the Python
UDF variants, no pyspark). The suite records, per case, how many query executions ran at least one
vecruntime operator and a full run fails when a case runs fewer than the checked-in floor
`spark-sql-tests/src/test/resources/vector-sql-coverage.tsv` (#17; ~4,231 accelerated executions);
`SQL_TESTS_UPDATE_BASELINE=true` rewrites the floor, `SQL_TESTS_JVM_ARGS` runs under a non-default
configuration, `SQL_TESTS_EXCLUDE='^$'` includes the default exclusions (`explain*.sql`, the
DataSketches files whose library rejects JDK > 21, `udtf/udtf.sql`). Its JVM sets
`vecruntime.agg.interleave=1`, `-Dspark.testing=true` and `-XX:-OmitStackTraceInFastThrow`. **Run it
after every planner or expression change**: it has caught eager `nanvl`, `count` over literal
arguments, collated strings as lanes, a build side pruned to zero columns and a bare literal
projection that the hand-written suites missed. `.github/workflows/spark-sql-tests.yml` runs it on
demand.

## 8. Validation: what "done" means

A change is not done until all of the following that apply have run green, locally, on JDK 25, and
the exit codes are reported.

1. The gate: `mvn -B -Pcomet,iceberg -pl kernels,spark,shuffle,benchmarks install`, exit 0. Last full
   gate: kernels 198 tests, spark 413 (Comet and Iceberg profiles on), shuffle 54. If a change lowers
   a number, explain why in the commit.
2. Kernel changes: the kernel suite at 128, 256 and 512 bits (`-Dvecruntime.vectorBits=...`).
3. Planner or expression changes: the SQL golden suite with no arguments, every case passing and the
   coverage floor held (section 7.2). At minimum run the files touching the change (`group-by`,
   `join`, `decimal`, `order-by`, `window`) before calling it done, and the whole suite before a release.
4. Operator output-contract changes: `run-tpcds.sh` at SF1 (~5 minutes), every checksum equal to
   Spark's.
5. Shuffle or AQE-facing changes: TPC-DS under `vector-shuffle` and the CDC MERGE, checksums equal.
6. Performance claims: a JMH number for a kernel change (`-wi 2 -i 3 -w 1 -r 1 -f 1`) or a TPC-H /
   TPC-DS median plus a JFR profile for an operator change. "It should be faster" is not evidence.
7. Docs in the same commit: `docs/operators.md` / `docs/expressions.md` rows, `README.md` and
   `docs/configuration.md` for keys, `CHANGELOG.md` for changed defaults, `docs/results.md` for
   numbers, this file for a changed decision.

## 9. JFR profiling

Unexpected results are profiled with Java Flight Recorder before they are explained. Two scripts make
the procedure one command each (#251):

```bash
benchmarks/scripts/profile-query.sh benchmarks/data/sf10 vector q6 --iterations 3 --warmup 2   # run + record + summarise
benchmarks/scripts/profile-query.sh benchmarks/data/tpcds-sf1 vector q72 --tpcds --conf spark.vecruntime.exec.sortMergeJoin.mode=merge
benchmarks/scripts/jfr-summary.sh /tmp/profiling/sf10/q6-vector.jfr --top 20    # any recording, as text for an issue
benchmarks/scripts/profile-query.sh - vector q6 --flags-only   # the recording flags for a cluster run's executor/driver options
```

- `profile-query.sh` runs one query in one configuration under a `settings=profile` recording, rows
  into a scratch `RESULTS_DIR` (never the report), prints the `[tpch]` / `[tpcds]` per-operator kernel
  times, and writes `<query>-<config>.jfr` plus `.summary.txt`. `jfr-summary.sh` prints, in reading
  order: `jfr view hot-methods`; the callers of the JDK-internal `MemorySegment` and
  `Buffer.checkIndex` frames (`MemorySessionImpl.checkValidStateRaw`, `checkBounds`,
  `isAlignedForElement` near the top mean a segment access the JIT did not hoist: a megamorphic call
  site, a segment from a different session per call, or `mismatch` on tiny ranges); our own frames by
  self time; allocation sites; GC pauses; waits; native methods (Comet's JVM side). Needs `JAVA_HOME`.
- By hand: `JVM_EXTRA="-XX:StartFlightRecording=filename=/tmp/x.jfr,settings=profile,dumponexit=true"
  RESULTS_DIR=/tmp/profiling benchmarks/scripts/run-tpch.sh ...`; `JVM_EXTRA` applies to the benchmark
  JVMs only. Compare two configurations by recording both.
- On the cluster: `EXEC_JAVA_OPTS="-XX:StartFlightRecording=delay=<s>,duration=<s>,filename=/tmp/exec.jfr,settings=profile"`
  (or `--flags-only`'s output) and copy the files out of the executor pods with `kubectl cp` while the
  application still runs (`KEEP_EXECUTORS=1`), or dump with `jcmd JFR.dump`. The window must cover the
  stage under study (the first q67 profiles missed the sort stage; a recording longer than the
  application is never written). Few samples per executor (~1.5 k in 50 s where a computing executor
  gives 30-40 k) means the executors are waiting: read `jdk.ThreadPark` by thread and first non-JDK
  frame before blaming a kernel.
- Only when the profile is understood do the fix, the doc entry and the rerun follow, in that order.

## 10. Benchmarking protocol

- Locally: one JVM per configuration, `local[8]`, 8 GB heap, `spark.sql.shuffle.partitions=8`; SF1
  with 10 warm-up and 10 measured runs, SF10 with 5 and 7. Run on a quiet machine: check `uptime` and
  the top CPU consumers first and make sure no build or test job runs during the whole run (one that
  started mid-run moved `spark` Q1 at SF10 from 1106 to 1451 ms); a `spark` Q1 median far from the
  documented one means the environment, not the code.
- On the cluster (`benchmarks/k8s/run-matrix.sh`): the 1 TB protocol is `EXECUTORS=8 EXEC_CORES=13
  EXEC_MEM=30g EXEC_OVERHEAD=20g DIRECT_MEM=30g DRIVER_CORES=2 DRIVER_MEM=4g KEEP_EXECUTORS=1`, 200
  shuffle partitions, one iteration per query, a warm-up query first, and a `spark` baseline in the
  same results prefix whenever a comparison is drawn. One results prefix per run; a fix is measured by
  exactly this loop -- gate, merge, image, targeted run of the affected queries, numbers on the issue
  -- before the next change. Single iterations at 1 TB have a wide band on the small queries; do not
  read a 5 % move on them as a result. Read medians from the driver log before the next run replaces
  the pod (the container log rotates at 10 MB); the `.jsonl` in the prefix and `--cluster-report` are
  the durable record. The results bucket is KMS-protected: read it in-cluster.
- Speedups are always relative to plain Spark on the same dataset and session. Update
  `docs/results.md` from the report; keep earlier phase tables for history there, not here.
- Iceberg merge-on-read: run every configuration over a variant with `--iceberg <warehouse>
  --variant <namespace>.<variant>` and the probes `probe-count,probe-sum,probe-group` beside `q1,q6`;
  checksums must agree for every variant, and `[tpch] scan=... merge-on-read live/physical=...` says
  which reader ran and how many rows the deletes removed.

## 11. Conventions

- Scala for planner rules, operators and expression compilation; Java for kernels and anything
  touching `MemorySegment` in a hot loop. No native code.
- Spark-facing configuration is `spark.vecruntime.*` (read from `SQLConf`, or from `SparkConf` for
  the UI and shuffle keys needed before a session exists; `VectorConf.scala`, `AggregateSpill.scala`
  and the shuffle module name where each is read); kernel tuning knobs are JVM system properties
  `vecruntime.*` (`vectorBits`, `platform`, `maskRegisters`, `agg.interleave`,
  `agg.maskPathMaxGroups`, `selection.minFraction`, `shuffle.reader.*`) because kernels have no Spark
  dependency. Document every new key in `README.md` and `docs/configuration.md`.
- Commit messages follow Conventional Commits with an explanatory body. Build before committing;
  never push to `main`; branches are `crew/...` or `kiro/...`; a human merges.
- Inclusive terminology throughout (allowlist / denylist, primary / replica).
- Local patches to upstream projects live under `upstream/` with the issue they track.

## 12. Known gaps

- Shuffle (#288): no TLS on the Flight server (Spark's material is JKS, Flight wants PEM; under
  `spark.ssl.rpc.enabled` use `spark.vecruntime.shuffle.backend=block`); one `DoGet` per executor and
  reducer; both backends assume executors that stay up for the job -- a push-based service is future
  work through the backend seam.
- The window operator holds its whole partition in memory (the aggregate, the sort and the shuffled
  hash join spill). The build side of a broadcast join is Spark's `HashedRelation`, read into columns
  once per task; a columnar broadcast exchange would read it once per job (#325).
- Not converted (falls back with the reason recorded): `ObjectHashAggregateExec` functions
  (`collect_*`, `percentile_*`, #57); cached tables over non-primitive schemas (#55); nested-type
  accessors and constructors, the lambda families (#50); Python UDFs (#65); the Parquet write path
  (#64); `RANGE` frames with value offsets, decimal window aggregates (#28), `IGNORE NULLS`; regular
  expressions, collated strings, binary and float columns as computed values or keys (they pass
  through). Wide products or sums as values need a 128-bit output column on escalated batches (#28).
- Without our shuffle or Comet's, the shuffle is Spark's row shuffle with a `ColumnarToRowExec` above
  the partial aggregate and a `RowToColumnarExec` below the Final, and the global sort stays Spark's.
- Selective predicates with scattered survivors (TPC-H Q6) lose to Spark's codegen over Spark's scan
  (0.86x at SF10): the scan is Spark's reader and its dictionary decode dominates; the lever is to
  evaluate comparisons over the dictionary table and gather bits by id. Do not reach for
  `spark.comet.parquet.rowFilterPushdown.enabled` (2x slower for Comet and for us on spread survivors).
- Platforms: measured on x86-64 (AVX-512 in the 1 TB campaign, AVX2) and Apple silicon (NEON);
  Graviton4 (SVE) measured for the mask-register decision (#484), the thresholds were set on x86. Ice
  Lake and Genoa are unmeasured (#282, #284); every switch reads `Platform`, so they are a measurement
  away.
- Iceberg merge-on-read reads (#261): the delete cost is paid inside Iceberg's reader by both engines,
  so our margin shrinks on deleted tables; equality deletes are the one shape where `vector` loses the
  pure-merge probe. The v3 deletion-vector writer is in progress (PR #509).
- The AOT class-data cache (`benchmarks/k8s/aot/`) is off by default: it speeds start-up and costs the
  heavy queries 20 % at 1 TB.
