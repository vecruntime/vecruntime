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
# KEEP / ICEBERG_SRC must be NORMALISED absolute paths (no '/../'): the per-module runner below
# starts a systemd-run unit whose StandardOutput=file:<LOGDIR>/... is rejected with "Path ... is not
# normalized" when LOGDIR still contains '..'. Resolve them through `cd ... && pwd` (the dir may not
# exist yet for KEEP, so resolve its parent then append the leaf).
_keep_default="$ROOT/../draco-keep"
KEEP="${KEEP:-$_keep_default}"
mkdir -p "$KEEP"
KEEP="$(cd "$KEEP" && pwd)"
M2="${M2:-$KEEP/m2b}"
ICEBERG_SRC="${ICEBERG_SRC:-$ROOT/../iceberg-1.12.0}"
ICEBERG_TAG="apache-iceberg-1.12.0"
PATCH="$ROOT/benchmarks/iceberg-tests/iceberg-1.12.0.patch"
EXCLUSIONS="$ROOT/benchmarks/iceberg-tests/exclusions.txt"
VECRUNTIME_ENABLED="${VECRUNTIME_ENABLED:-true}"
VECRUNTIME_WRITERS="${VECRUNTIME_WRITERS:-off}"  # bridge WRITE path; off by default -- write-side shaded/unshaded parquet mismatch, see README
MEM_CAP="${MEM_CAP:-8G}"
TS="$(date +%Y%m%d-%H%M%S)"
LOGDIR="$KEEP/iceberg-tests-$TS"
mkdir -p "$LOGDIR"

# The default suite set the task asks for (spark-extensions + spark). TestDelete/TestUpdate/TestMerge
# and TestRowLevelOperationsWithLineage are ABSTRACT bases in 1.12.0 -- their concrete COW/MOR
# subclasses below are the runnable suites, so the abstract names are not listed (they match no
# tests).
DEFAULT_SUITES="
org.apache.iceberg.spark.extensions.TestCopyOnWriteDelete
org.apache.iceberg.spark.extensions.TestCopyOnWriteUpdate
org.apache.iceberg.spark.extensions.TestCopyOnWriteMerge
org.apache.iceberg.spark.extensions.TestMergeOnReadDelete
org.apache.iceberg.spark.extensions.TestMergeOnReadUpdate
org.apache.iceberg.spark.extensions.TestMergeOnReadMerge
org.apache.iceberg.spark.extensions.TestCopyOnWriteWithLineage
org.apache.iceberg.spark.extensions.TestMergeOnReadWithLineage
org.apache.iceberg.spark.extensions.TestRewriteDataFilesProcedure
org.apache.iceberg.spark.actions.TestRewriteDataFilesAction
org.apache.iceberg.spark.sql.TestPartitionedWrites
org.apache.iceberg.spark.extensions.TestPartitionedWritesToWapBranch
org.apache.iceberg.spark.sql.TestUnpartitionedWrites
org.apache.iceberg.spark.source.TestSparkDataWrite
org.apache.iceberg.spark.source.TestPositionDeletesTable
org.apache.iceberg.spark.extensions.TestMetadataTables
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
ICEBERG_SRC="$(cd "$ICEBERG_SRC" && pwd)"   # normalise (strip any '/../') for the inner runner paths
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
# iceberg-bridge's IcebergParquetInterop references Iceberg's SHADED parquet classes
# (org.apache.iceberg.shaded.org.apache.parquet.*), which live only in the iceberg-spark-runtime
# shaded jar -- not in Iceberg's UNSHADED test modules (iceberg-parquet/core). Putting the WHOLE
# runtime jar on the test classpath drags its shaded Jackson onto it and breaks Iceberg's REST
# catalog (NoSuchMethodError), so carve out ONLY the shaded-parquet subtree into a minimal jar
# (zero unshaded classes -> no collisions) and add that.
runtime_jar="$(find "$M2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0" -maxdepth 1 -name 'iceberg-spark-runtime-4.1_2.13-1.12.0.jar' | head -1)"
[ -n "$runtime_jar" ] || die "missing iceberg-spark-runtime-4.1_2.13-1.12.0.jar in $M2 (needed for shaded parquet)"
shaded_parquet_jar="$LOGDIR/iceberg-shaded-parquet-1.12.0.jar"
python3 "$ROOT/benchmarks/iceberg-tests/extract-shaded-parquet.py" "$runtime_jar" "$shaded_parquet_jar" \
  || die "failed to extract shaded parquet jar"
jars="$jars:$shaded_parquet_jar"
echo "vecruntime.jars=$jars"

# --- 3. Apply the patch to the Iceberg tree ---------------------------------
git -C "$ICEBERG_SRC" checkout -- . 2>/dev/null || true
git -C "$ICEBERG_SRC" clean -fdq spark/v4.1 2>/dev/null || true
echo "Applying $PATCH"
git -C "$ICEBERG_SRC" apply "$PATCH"

# --- 4. Build the Gradle include filter and exclude property -----------------
testargs_str=""
for s in $SUITES; do testargs_str="$testargs_str --tests $s"; done
# Join the exclusion patterns (class or Class#method) into one comma list; the patched build.gradle
# turns it into test-task excludeTestsMatching entries. '#' -> '.' is done inside the build.
exclude_list="$(grep -vE '^\s*#|^\s*$' "$EXCLUSIONS" | paste -sd, - || true)"
echo "${exclude_list:-<none>}" > "$LOGDIR/active-exclusions.txt"
TEST_JAVA_VERSION="${TEST_JAVA_VERSION:-25}"
onoff="$([ "$VECRUNTIME_ENABLED" = true ] && echo on || echo off)"

# Emit a standalone runner script per module (no exported bash functions -- systemd-run starts a
# clean environment that would drop them).
emit_runner() {  # $1 = gradle project path, $2 = label -> prints the script path
  local proj="$1" label="$2"
  local script="$LOGDIR/run-$label.$onoff.sh"
  local log="$LOGDIR/$label.$onoff.log"
  {
    echo '#!/usr/bin/env bash'
    echo 'set -uo pipefail'
    echo "cd '$ICEBERG_SRC'"
    echo "JAVA_HOME='$GRADLE_JDK' ./gradlew --no-daemon --console=plain \\"
    echo "  -DsparkVersions=4.1 \\"
    echo "  -Porg.gradle.java.installations.paths='$TEST_JDK' \\"
    echo "  -Dvecruntime.jars='$jars' \\"
    echo "  -Dvecruntime.enabled='$VECRUNTIME_ENABLED' \\"
    echo "  -Dvecruntime.testJavaVersion='$TEST_JAVA_VERSION' \\"
    echo "  -Dvecruntime.writers='$VECRUNTIME_WRITERS' \\"
    [ -n "$exclude_list" ] && echo "  -Dvecruntime.excludeTests='$exclude_list' \\"
    echo "  '$proj:test'$testargs_str \\"
    echo "  -PmaxParallelForks=1 > '$log' 2>&1"
    echo "echo \"gradle exit \$? -> $log\""
  } > "$script"
  chmod +x "$script"
  echo "$script"
}

run_capped() {  # $1 = gradle project path, $2 = label
  local proj="$1" label="$2"
  local script; script="$(emit_runner "$proj" "$label")"
  echo "=== $label (plugin $VECRUNTIME_ENABLED) ==="
  if command -v systemd-run >/dev/null 2>&1; then
    systemd-run --user --unit="iceberg-tests-$label-$onoff-$$" --collect --wait -p MemoryMax="$MEM_CAP" \
      -p StandardOutput=file:"$LOGDIR/$label.$onoff.systemd.log" \
      -p StandardError=file:"$LOGDIR/$label.$onoff.systemd.log" \
      bash "$script" || true
  else
    bash "$script" || true
  fi
  echo "  log: $LOGDIR/$label.$onoff.log ; reports: $ICEBERG_SRC/spark/v4.1/*/build/reports/tests"
}

run_capped ":iceberg-spark:iceberg-spark-extensions-4.1_2.13" spark-extensions
run_capped ":iceberg-spark:iceberg-spark-4.1_2.13"            spark

echo
echo "Logs under $LOGDIR"
echo "Per-suite pass/fail: parse $ICEBERG_SRC/spark/v4.1/*/build/test-results/test/*.xml"
echo "Exclusions applied from $EXCLUSIONS (see active-exclusions.txt)"
