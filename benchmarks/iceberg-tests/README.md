# Iceberg upstream test harness

Runs **Apache Iceberg 1.12.0's own Spark 4.1 test suites** with the vecruntime (spark-vector)
plugin on its path, as a reproducible local harness and a CI job. It is the Iceberg analogue of
`benchmarks/scripts/run-spark-sql-tests.sh`, which runs Spark's own golden suite under the plugin.

## What it exercises

The plugin is injected into Iceberg's Spark test bases (`TestBase`, `ExtensionsTestBase`) via
`spark.plugins=io.vecruntime.spark.VectorPlugin`. The plugin's driver side idempotently appends
`VectorSparkSessionExtensions` to `spark.sql.extensions`, so in the extensions suites the chain is
`IcebergSparkSessionExtensions,VectorSparkSessionExtensions`. `vecruntime/iceberg-bridge` is on the
test runtime classpath.

The harness fixes the vecruntime iceberg paths **on explicitly** (`VectorTestConf`) so the suites
exercise the full read AND write path regardless of any future default flip. As of current `main`
every one of these flags defaults **on** (verified in
`spark/src/main/scala/io/vecruntime/spark/VectorConf.scala`: each helper reads its key with
`default = true`), and the harness matches that:

| flag | default in `main` | harness sets | path |
|------|-------------------|--------------|------|
| `spark.vecruntime.scan.nativeIceberg.enabled` | on | **on** | read |
| `spark.vecruntime.iceberg.readSpecs.enabled` | on | **on** | read |
| `spark.vecruntime.iceberg.nativeEqualityDeletes.enabled` | on | **on** | read |
| `spark.vecruntime.iceberg.copyUnchanged.enabled` | on | *(unset → main default, on)* | COW copy |
| `spark.vecruntime.iceberg.writer.enabled` (columnar writer) | on | **on** | write |
| `spark.vecruntime.iceberg.dvWriter.enabled` (merge-on-read DV writer) | on | **on** | write |
| `spark.vecruntime.iceberg.positionDeletes.enabled` | on | **on** | write |

**Shading.** iceberg-bridge compiles against the SHADED `iceberg-spark-runtime` jar (parquet
relocated under `org.apache.iceberg.shaded.*`), while Iceberg's own Gradle test modules are
UNSHADED. The two places the bridge crosses that boundary — the native equality-delete reader
(`EqualityDeleteSet`) and the write-side schema/footer interop (`IcebergParquetInterop`) — detect at
runtime which parquet relocation Iceberg's classes bind to and convert to that flavour reflectively,
so the SAME jars work on the shaded runtime jar (production) AND on this unshaded test classpath.
Both fixes live on `crew/draco/eqdelete-shading-agnostic`. Pass `VECRUNTIME_WRITERS=off`
(→ `-Dvecruntime.writers=off`) to run the read-only scan path alone (e.g. to isolate a write
regression).

## JDK requirement (important)

Iceberg 1.12.0's Gradle **hard-refuses** any launcher JVM but **JDK 17 or 21**
(`build.gradle` ~line 78: `This build must be run with JDK 17 or 21`). The vecruntime jars need
**JDK 25** at *test runtime* (the Vector API incubator module). So:

- **Gradle is launched on JDK 21** (`GRADLE_JDK`).
- **The test JVM runs on JDK 25** via a Gradle toolchain (`TEST_JDK`), wired by the patch's
  `javaLauncher = javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(25) }`.

Both JDKs must be present. CI installs them with `setup-java`; on the crew dev host JDK 25 is the
system default and a JDK 21 launcher lives under `draco-keep/jdk21`. The driver fails fast with a
precise message if either `GRADLE_JDK` (17/21) or `TEST_JDK` (25) is missing.

## Files

- `iceberg-1.12.0.patch` — the smallest patch to Iceberg's test tree: injects the plugin + jars +
  JVM args + JDK-25 test toolchain into the two `SparkSession.builder()` sites and the two
  `spark/v4.1` test tasks, and adds the `VectorTestConf` helper. Applies cleanly against a pristine
  `apache-iceberg-1.12.0` checkout (`git apply --check` verified).
- `run-iceberg-tests.sh` — the driver: builds the jars (JDK 25, isolated maven repo, never
  `install`), applies the patch, runs the suites with the exclusion list. `VECRUNTIME_ENABLED=false`
  runs the vanilla-Iceberg baseline with the same harness.
- `exclusions.txt` — triaged exclusions, one reason per entry (categories b/c per the rubric below).
- `../../.github/workflows/iceberg-upstream-tests.yml` — nightly + `workflow_dispatch` CI job.

## Usage

```
# local (needs JDK 21 AND JDK 25 installed):
GRADLE_JDK=/path/to/jdk21 TEST_JDK=/path/to/jdk25 \
  benchmarks/iceberg-tests/run-iceberg-tests.sh

# vanilla-Iceberg baseline (no plugin), same suite set, to prove a failure is ours:
GRADLE_JDK=/path/to/jdk21 TEST_JDK=/path/to/jdk25 VECRUNTIME_ENABLED=false \
  benchmarks/iceberg-tests/run-iceberg-tests.sh

# a subset:
SUITES='org.apache.iceberg.spark.extensions.TestDelete' GRADLE_JDK=... TEST_JDK=... \
  benchmarks/iceberg-tests/run-iceberg-tests.sh
```

## Failure triage rubric

Every failure is one of:

- **(a) a vecruntime bug** — minimise, write the repro, fix if small and clearly ours, else report
  with the repro.
- **(b) a test asserting Iceberg's own physical plan nodes / exec-node shape** that the plugin
  replaces — add to `exclusions.txt` with a one-line reason.
- **(c) a known upstream Iceberg bug, or out of scope** (ORC/Avro/geo/encryption/shredded variant)
  — add to `exclusions.txt` with a one-line reason.

Run each suite once with the plugin **off** (`VECRUNTIME_ENABLED=false`) to confirm a failure is
ours and not the environment.
## Last full run (2026-10-24)

The run used a JDK-21 Gradle launcher and JDK-25 test JVMs, with every vecruntime Iceberg path on (the main defaults): native scan, columnar writer, DV / position-delete writer and copy-unchanged. vecruntime jars came from `main` plus #992 (`a269bdd9`); Iceberg is `apache-iceberg-1.12.0` (`212aa894`).

| module | tests | failed | skipped |
|--------|-------|--------|---------|
| `iceberg-spark-extensions-4.1_2.13` | 1334 | 0 | 106 |
| `iceberg-spark-4.1_2.13` | 579 | 0 (after the exclusions below) | 22 |

With the plugin **off** (`VECRUNTIME_ENABLED=false`) the same suites pass, so every exclusion is about the plugin, not the environment.

### Exclusions (`exclusions.txt`)

| test | reason |
|------|--------|
| `*WithVectorizedReads` x 6 (copy-on-write and merge-on-read DELETE / UPDATE / MERGE) | Plan shape: `assertAllBatchScansVectorized` looks for Iceberg's `BatchScanExec`, which vecruntime replaces with `VectorIcebergScanExec`. |
| `TestMergeOnReadDelete#testAggregatePushDownInMergeOnReadDelete` | Plan text: the test searches EXPLAIN for `max(data)`, which vecruntime's aggregate node prints. |
| `TestRewriteDataFilesAction#testBinPackCombineMixedFiles` (fails on v3/v4; v2 passes) | The test derives its max file size from the byte sizes of Iceberg's own writer, with a hand-fitted v3 margin. The columnar writer's v2 pages are ~1.7x smaller, so the large input stays under that max and is correctly not split. Rows and `rewrittenBytes` match. |
| `TestRewriteDataFilesAction#testBinPackCombineMediumFiles` (fails on v3/v4; v2 passes) | Same mechanism: the roll target is derived from input sizes ~1.9x smaller than Iceberg's, and the v3 row-lineage columns push one output past it, so it rolls a ~22 KB remainder. Rows match. |

The two bin-pack cases are excluded per method, so their v2 runs are skipped as well. Both were checked numerically (file sizes and thresholds, on vs off). v2 passes because of #997.

### Fixed in vecruntime because of this harness

| PR | Fix | Iceberg tests it fixed |
|----|-----|------------------------|
| #988 | Native scan keeps Iceberg's task grouping and per-scan `read.split.*` options | `testSkew*` and the scan side of the bin-pack tests |
| #989 | iceberg-bridge works with relocated and unshaded Iceberg parquet (read and write) | `testEqualityDeletePreservation`, `testRemoveDangledEqualityDeletesPartitionEvolution` |
| #991 | Parallel-decode leak on early task end | none (found by vecruntime CI while landing these fixes) |
| #992 | Copy-on-write chunk copy writes row groups in schema order | 6 x `TestCopyOnWriteUpdate` subquery / distribution cases ("column ... is not in the store") |
| #993 | Write coalescing honoured, `sort_order_id` on copy-unchanged files, bin-pack copy split size | `testCoalesce*`, `testCopyOnWriteDeleteSetsSortOrderIdOnRewrittenDataFiles` |
| #997 | Adaptive row-group boundary check in the columnar writer | `testBinPackCombineMediumFiles` v2 |
