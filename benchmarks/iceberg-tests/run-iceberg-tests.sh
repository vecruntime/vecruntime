#!/usr/bin/env bash
#
# Reproducible harness: run Apache Iceberg 1.12.0's OWN Spark 4.1 test suites with the vecruntime
# (spark-vector) plugin on its path. Mirrors benchmarks/scripts/run-spark-sql-tests.sh (which runs
# Spark's own golden suite under the plugin) but targets Iceberg's Gradle suites instead.
#
#   benchmarks/iceberg-tests/run-iceberg-tests.sh                 # default suite set, plugin ON
#   VECRUNTIME_ENABLED=false benchmarks/iceberg-tests/run-iceberg-tests.sh   # baseline, plugin OFF
#   ICEBERG_SRC=/path/to/iceberg-1.12.0 ...                       # reuse an existing clone
#   SUITES='org.apache.iceberg.spark.extensions.TestDelete' ...   # a subset (space-separated)
#
# JDK REQUIREMENT. Iceberg 1.12.0's Gradle HARD-REFUSES any launcher JVM other than JDK 17 or 21
# (build.gradle line ~78: "This build must be run with JDK 17 or 21"). vecruntime's jars and the
# plugin require JDK 25 at TEST RUNTIME (the Vector API incubator module). So Gradle is LAUNCHED on
# JDK 21 and the test tasks are told to use a JDK 25 TOOLCHAIN via -Porg.gradle.java.installations.
# Set:
#   GRADLE_JDK   = a JDK 17 or 21 home   (launches Gradle)        [required]
#   TEST_JDK     = a JDK 25 home         (runs the test JVMs)     [required]
# The script fails fast with a precise message if either is missing.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"                     # the vecruntime repo root
KEEP="${KEEP:-$ROOT/../draco-keep}"                            # logs + isolated m2 live here
M2="${M2:-$KEEP/m2b}"
ICEBERG_SRC="${ICEBERG_SRC:-$ROOT/../iceberg-1.12.0}"
ICEBERG_TAG="apache-iceberg-1.12.0"
PATCH="$ROOT/benchmarks/iceberg-tests/iceberg-1.12.0.patch"
EXCLUSIONS="$ROOT/benchmarks/iceberg-tests/exclusions.txt"
VECRUNTIME_ENABLED="${VECRUNTIME_ENABLED:-true}"
MEM_CAP="${MEM_CAP:-8G}"
TS="$(date +%Y%m%d-%H%M%S)"
LOGDIR="$KEEP/iceberg-tests-$TS"
mkdir -p "$LOGDIR"

# The default suite set the task asks for (spark-extensions + spark).
DEFAULT_SUITES="
org.apache.iceberg.spark.extensions.TestDelete
org.apache.iceberg.spark.extensions.TestUpdate
org.apache.iceberg.spark.extensions.TestMerge
org.apache.iceberg.spark.extensions.TestCopyOnWriteDelete
org.apache.iceberg.spark.extensions.TestCopyOnWriteUpdate
org.apache.iceberg.spark.extensions.TestCopyOnWriteMerge
org.apache.iceberg.spark.extensions.TestMergeOnReadDelete
org.apache.iceberg.spark.extensions.TestMergeOnReadUpdate
org.apache.iceberg.spark.extensions.TestMergeOnReadMerge
org.apache.iceberg.spark.extensions.TestRowLevelOperationsWithLineage
org.apache.iceberg.spark.extensions.TestRewriteDataFilesProcedure
org.apache.iceberg.spark.actions.TestRewriteDataFilesAction
org.apache.iceberg.spark.sql.TestPartitionedWrites
org.apache.iceberg.spark.sql.TestPartitionedWritesToWapBranch
org.apache.iceberg.spark.sql.TestUnpartitionedWrites
org.apache.iceberg.spark.source.TestSparkDataWrite
org.apache.iceberg.spark.source.TestDataFrameWrites
org.apache.iceberg.spark.source.TestPositionDeletesTable
org.apache.iceberg.spark.source.TestMetadataTables
org.apache.iceberg.spark.source.TestSparkReaderDeletes
"
SUITES="${SUITES:-$DEFAULT_SUITES}"

die() { echo "FATAL: $*" >&2; exit 1; }

# --- JDK selection -----------------------------------------------------------
[ -n "${GRADLE_JDK:-}" ] || die "set GRADLE_JDK to a JDK 17 or 21 home (Iceberg's Gradle refuses JDK 25). None is installed on this dev host; CI installs one with setup-java."
[ -n "${TEST_JDK:-}" ]   || die "set TEST_JDK to a JDK 25 home (vecruntime's Vector API module needs 25 at test runtime)."
[ -x "$GRADLE_JDK/bin/java" ] || die "GRADLE_JDK=$GRADLE_JDK has no bin/java"
[ -x "$TEST_JDK/bin/java" ]   || die "TEST_JDK=$TEST_JDK has no bin/java"
gv="$("$GRADLE_JDK/bin/java" -version 2>&1 | head -1)"
case "$gv" in *'"17'*|*'"21'*) : ;; *) die "GRADLE_JDK must be JDK 17 or 21, got: $gv" ;; esac

# --- 1. Iceberg sources ------------------------------------------------------
if [ ! -d "$ICEBERG_SRC/.git" ]; then
  echo "Cloning Iceberg $ICEBERG_TAG into $ICEBERG_SRC"
  git clone --depth 1 --branch "$ICEBERG_TAG" https://github.com/apache/iceberg "$ICEBERG_SRC"
fi
git -C "$ICEBERG_SRC" rev-parse HEAD

# --- 2. Build the vecruntime jars (JDK 25, isolated m2, never `install`) -----
echo "Building vecruntime jars (JDK 25) into $M2"
JAVA_HOME="$TEST_JDK" mvn -o -Dmaven.repo.local="$M2" -f "$ROOT/pom.xml" -Piceberg -DskipTests -T1 package \
  > "$LOGDIR/jar-build.log" 2>&1 || { tail -40 "$LOGDIR/jar-build.log"; die "jar build failed"; }

jars=""
for m in kernels parquet spark shuffle iceberg-bridge; do
  j="$(find "$ROOT/$m/target" -maxdepth 1 -name 'vecruntime-*.jar' ! -name '*-sources.jar' ! -name '*-tests.jar' ! -name '*-javadoc.jar' | head -1)"
  [ -n "$j" ] || die "missing jar for module $m"
  jars="${jars:+$jars:}$j"
done
echo "vecruntime.jars=$jars"

# --- 3. Apply the patch to the Iceberg tree ---------------------------------
git -C "$ICEBERG_SRC" checkout -- . 2>/dev/null || true
git -C "$ICEBERG_SRC" clean -fdq spark/v4.1 2>/dev/null || true
echo "Applying $PATCH"
git -C "$ICEBERG_SRC" apply "$PATCH"

# --- 4. Build the Gradle include filter and exclude property -----------------
testargs=()
for s in $SUITES; do testargs+=(--tests "$s"); done
# Join the exclusion patterns (class or Class#method) into one comma list; the patched build.gradle
# turns it into test-task excludeTestsMatching entries. '#' -> '.' is done inside the build.
exclude_list="$(grep -vE '^\s*#|^\s*$' "$EXCLUSIONS" | paste -sd, - || true)"
echo "${exclude_list:-<none>}" > "$LOGDIR/active-exclusions.txt"
TEST_JAVA_VERSION="${TEST_JAVA_VERSION:-25}"

export ICEBERG_SRC GRADLE_JDK TEST_JDK jars VECRUNTIME_ENABLED LOGDIR exclude_list TEST_JAVA_VERSION
export GRADLE_TESTARGS="${testargs[*]}"

run_module() {  # $1 = gradle project path, $2 = label
  local proj="$1" label="$2"
  local onoff; onoff="$([ "$VECRUNTIME_ENABLED" = true ] && echo on || echo off)"
  local log="$LOGDIR/$label.$onoff.log"
  echo "=== $label (plugin $VECRUNTIME_ENABLED) -> $log ==="
  # shellcheck disable=SC2086
  ( cd "$ICEBERG_SRC" && \
    JAVA_HOME="$GRADLE_JDK" ./gradlew --no-daemon --console=plain \
      -Porg.gradle.java.installations.paths="$TEST_JDK" \
      -Dvecruntime.jars="$jars" \
      -Dvecruntime.enabled="$VECRUNTIME_ENABLED" \
      -Dvecruntime.testJavaVersion="$TEST_JAVA_VERSION" \
      ${exclude_list:+-Dvecruntime.excludeTests="$exclude_list"} \
      "$proj:test" $GRADLE_TESTARGS \
      -PmaxParallelForks=1 ) > "$log" 2>&1 || true
  echo "  done: $log ; reports under $ICEBERG_SRC/spark/v4.1/*/build/reports/tests"
}
export -f run_module

run_capped() {  # wrap a module run in a memory-capped systemd unit when available
  if command -v systemd-run >/dev/null 2>&1; then
    local unit="iceberg-tests-$2-$$"
    systemd-run --user --unit="$unit" --collect --wait -p MemoryMax="$MEM_CAP" \
      -p StandardOutput=file:"$LOGDIR/$2.systemd.log" -p StandardError=file:"$LOGDIR/$2.systemd.log" \
      bash -c "run_module '$1' '$2'"
  else
    run_module "$1" "$2"
  fi
}

run_capped ":iceberg-spark:iceberg-spark-extensions-4.1_2.13" spark-extensions
run_capped ":iceberg-spark:iceberg-spark-4.1_2.13"            spark

echo
echo "Logs under $LOGDIR"
echo "Per-suite pass/fail: parse $ICEBERG_SRC/spark/v4.1/*/build/test-results/test/*.xml"
echo "Exclusions applied from $EXCLUSIONS (see active-exclusions.txt)"
