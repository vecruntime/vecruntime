#!/usr/bin/env bash
# End-to-end Parquet scan benchmark (ParquetScanE2EBenchmark): OUR native chunk reader
# (NativeParquetColumnReader -> ColumnChunkDecoder -> Arrow) vs Spark's own
# VectorizedParquetRecordReader on the same real 4M-row file, same JVM. Spark is a 'provided'
# dependency of the benchmarks module, so this cannot run from benchmarks.jar alone: the classpath is
# built as run-flight-bench.sh builds it.
#
#   benchmarks/scripts/run-parquet-e2e.sh                 # default; JSON + text to benchmarks/results
#   benchmarks/scripts/run-parquet-e2e.sh -wi 3 -i 5 -f 1 # override the JMH knobs
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
JAVA="${JAVA_HOME:?set JAVA_HOME to a JDK 25}/bin/java"
OUT="${RESULTS_DIR:-$ROOT/benchmarks/results}"
mkdir -p "$OUT"

CP_FILE="$ROOT/benchmarks/target/classpath.txt"
if [ ! -f "$CP_FILE" ] || [ "$ROOT/benchmarks/pom.xml" -nt "$CP_FILE" ]; then
  (cd "$ROOT" && mvn -q -B -pl benchmarks dependency:build-classpath -Dmdep.outputFile="$CP_FILE" >/dev/null)
fi
CP="$ROOT/benchmarks/target/classes:$(cat "$CP_FILE")"

JVM_OPTS=(
  -Xmx"${JVM_MEM:-6g}" -XX:+UseG1GC
  --add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED
  --sun-misc-unsafe-memory-access=allow -XX:+IgnoreUnrecognizedVMOptions
  --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED
  -Dio.netty.tryReflectionSetAccessible=true -Dlog4j2.level=warn
)
FORK_OPTS="${JVM_OPTS[*]}"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DEFAULTS=(-wi 2 -i 3 -f 1)
if [ "$#" -gt 0 ]; then DEFAULTS=(); fi
exec "$JAVA" "${JVM_OPTS[@]}" -cp "$CP" org.openjdk.jmh.Main ParquetScanE2EBenchmark \
  "${DEFAULTS[@]}" -jvmArgs "$FORK_OPTS" -rf json -rff "$OUT/parquet-e2e-$STAMP.json" "$@"
