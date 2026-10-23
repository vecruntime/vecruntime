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

The harness turns the vecruntime paths **on explicitly** (`VectorTestConf`), because in vecruntime
`main` they default **off** while they land slice by slice:

| flag | default in main | harness |
|------|-----------------|---------|
| `spark.vecruntime.scan.nativeIceberg.enabled` | off | **on** |
| `spark.vecruntime.iceberg.readSpecs.enabled` | on | on |
| `spark.vecruntime.iceberg.nativeEqualityDeletes.enabled` | on | on |
| `spark.vecruntime.iceberg.writer.enabled` (columnar writer) | off | **on** |
| `spark.vecruntime.iceberg.dvWriter.enabled` (merge-on-read DV writer) | off | **on** |
| `spark.vecruntime.iceberg.positionDeletes.enabled` | on | on |

(The task brief described these three as already default-on; they are not in current main. The
harness forces them so the suites actually test them. If they later default on, the explicit sets
are harmless.)

## JDK requirement (important)

Iceberg 1.12.0's Gradle **hard-refuses** any launcher JVM but **JDK 17 or 21**
(`build.gradle` ~line 78: `This build must be run with JDK 17 or 21`). The vecruntime jars need
**JDK 25** at *test runtime* (the Vector API incubator module). So:

- **Gradle is launched on JDK 21** (`GRADLE_JDK`).
- **The test JVM runs on JDK 25** via a Gradle toolchain (`TEST_JDK`), wired by the patch's
  `javaLauncher = javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(25) }`.

The crew dev host has **only JDK 25** installed, so the suites **cannot be executed there** — the
driver fails fast with a message saying so. CI installs both JDKs with `setup-java`, which is the
intended execution environment.

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
