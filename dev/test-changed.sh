#!/usr/bin/env bash
# Copyright 2026-2027 Angel Conde and the vecruntime contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# The fast local loop: build and test only what a change can affect, instead of the whole gate.
#
#   dev/test-changed.sh [--since REF] [--area a,b] [--full] [--dry-run] [--no-format] [-- <extra mvn args>]
#
# What changed is `git diff --name-only REF...HEAD` (default origin/main) plus uncommitted and untracked files.
#  - Modules: every changed module of the gate set and every module after it in build order (kernels, then
#    parquet-writer when present, spark, shuffle, benchmarks) is built and tested. A change to the root pom,
#    `.mvn/` or a directory this script does not know runs the whole gate set.
#  - Inside spark (the slow module): only the suites tagged with the changed areas
#    (`io.vecruntime.spark.test.tags.*`, -DtagsToInclude). Areas come from the paths of the changed main
#    sources and from the tags of changed suites; anything unmapped -- the planner rule, the operator base,
#    adapters, shims, test bases, resources, the spark pom, a kernels change -- runs every spark suite.
#    --area replaces the computed areas; --full runs every suite of every affected module.
#  - Docs-only changes build nothing (the web site is built if web/ or docs/ changed and web/node_modules exists).
#
# This is the inner loop only. A PR is ready after THE GATE (AGENTS.md), run once on the final commit with the
# worktree untouched while it runs; only that exit code counts.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

SINCE=origin/main AREAS="" FULL=0 DRY=0 FORMAT=1 EXTRA=()
while [ $# -gt 0 ]; do
  case "$1" in
    --since) SINCE=$2; shift 2 ;;
    --area) AREAS=$2; shift 2 ;;
    --full) FULL=1; shift ;;
    --dry-run) DRY=1; shift ;;
    --no-format) FORMAT=0; shift ;;
    --) shift; EXTRA=("$@"); break ;;
    -h|--help) sed -n '16,33p' "$0"; exit 0 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done

TAGPKG=io.vecruntime.spark.test.tags
ALL_TAGS="Scan Expr Agg Join SortWindow Nested Write Iceberg Comet Shuffle Core"
MVN=(mvn -B -ntp -Pcomet,iceberg)
ORDER=(kernels)
[ -d parquet-writer ] && ORDER+=(parquet-writer)
ORDER+=(spark shuffle benchmarks)

# --- Every concrete spark suite carries an area tag, or a tagged run would skip it silently.
untagged=()
while IFS= read -r f; do
  grep -qE '^(final )?class [A-Za-z0-9_]+Suite\b' "$f" || continue
  grep -q "$TAGPKG\." "$f" || untagged+=("$f")
done < <(find spark/src/test -name '*Suite.scala' | sort)
if [ ${#untagged[@]} -gt 0 ]; then
  echo "These suites have no area tag ($TAGPKG.{${ALL_TAGS// /,}}):" >&2
  printf '  %s\n' "${untagged[@]}" >&2
  exit 3
fi

# --- What changed.
changed=$( { git diff --name-only "$SINCE"...HEAD; git diff --name-only HEAD; git ls-files --others --exclude-standard; } | sort -u)
if [ -z "$changed" ]; then echo "nothing changed against $SINCE"; exit 0; fi

declare -A mods=() tags=()
spark_full=0 all=0 web=0
add_tags() { for t in "$@"; do tags[$t]=1; done; }

# Areas of one spark main source, by its path; returns 1 when no area fits (run every suite).
areas_of() {
  local p=$1 hit=0
  case "$p" in */adapter/*|*/arrow/*|*/shims/*|*/resources/*|*VectorColumnarRule*|*VectorExec.*|*VectorBatchIterator*|*RegisterLate*|*PlaceholderColumn*|*VectorPlugin*|*VectorConf*|*TypeMapping*) return 1 ;; esac
  [[ $p =~ /parquet/|Parquet|Scan|DecodeAhead|Prefetch ]] && { add_tags Scan; hit=1; }
  [[ $p =~ /iceberg/|Iceberg|V2BroadcastBySize|V2PruningColumns|V2RuntimeFilterBySize|WriteDelta ]] && { add_tags Iceberg; hit=1; }
  [[ $p =~ Write ]] && { add_tags Write; hit=1; }
  [[ $p =~ /comet/|Comet ]] && { add_tags Comet; hit=1; }
  [[ $p =~ /agg/|Aggregate|Rollup|Expand|GroupKeys|ObjectAgg|KeyStats ]] && { add_tags Agg; hit=1; }
  [[ $p =~ Join|Bloom|Dpp|RuntimeFilter|Broadcast|Hash ]] && { add_tags Join; hit=1; }
  [[ $p =~ Sort|Window|Limit|TakeOrdered ]] && { add_tags SortWindow; hit=1; }
  [[ $p =~ Nested|Generate|Structural|Lambda ]] && { add_tags Nested; hit=1; }
  [[ $p =~ /expr/|Filter|Project|Cast|Formatters|Errors ]] && { add_tags Expr; hit=1; }
  [[ $p =~ Shuffle|Partition ]] && { add_tags Shuffle; hit=1; }
  [[ $p =~ /ui/ ]] && { add_tags Core; hit=1; }
  [ $hit = 1 ]
}

while IFS= read -r p; do
  top=${p%%/*}
  case "$p" in
    pom.xml|.mvn/*) all=1 ;;
    docs/*|web/*) web=1 ;;
    *.md|LICENSE|NOTICE|images/*|upstream/*|.github/*|dev/*) ;;
    spark/*)
      mods[spark]=1
      case "$p" in
        spark/pom.xml) spark_full=1 ;;
        spark/src/test/*Suite.scala)
          if [ -f "$p" ] && grep -q "$TAGPKG\." "$p"; then
            add_tags $(grep -oE "$TAGPKG\.[A-Za-z]+" "$p" | sed "s/$TAGPKG\.//" | sort -u)
          else spark_full=1; fi ;;
        spark/src/test/java/io/vecruntime/spark/test/tags/*) ;;
        spark/src/test/*) spark_full=1 ;;
        spark/src/main/*) areas_of "$p" || spark_full=1 ;;
        *) spark_full=1 ;;
      esac ;;
    *)
      if printf '%s\n' "${ORDER[@]}" | grep -qx "$top"; then
        mods[$top]=1
        [ "$top" = kernels ] && spark_full=1
        [ "$top" = parquet-writer ] && add_tags Write
      else
        all=1
      fi ;;
  esac
done <<< "$changed"

# --- Affected modules: the changed ones and everything after them in build order.
affected=() on=0
for m in "${ORDER[@]}"; do
  { [ $all = 1 ] || [ -n "${mods[$m]:-}" ]; } && on=1
  [ $on = 1 ] && affected+=("$m")
done
[ $all = 1 ] && spark_full=1
[ $FULL = 1 ] && spark_full=1
if [ -n "$AREAS" ]; then
  spark_full=0; tags=()
  for a in ${AREAS//,/ }; do
    [[ " $ALL_TAGS " == *" $a "* ]] || { echo "unknown area $a (one of: $ALL_TAGS)" >&2; exit 2; }
    tags[$a]=1
  done
fi
[ ${#tags[@]} = 0 ] && [ $spark_full = 0 ] && spark_full=1
include=""
if [ $spark_full = 0 ]; then
  for t in "${!tags[@]}"; do include+="${include:+,}$TAGPKG.$t"; done
fi

before=() after=() seen_spark=0 has_spark=0
for m in "${affected[@]}"; do
  if [ "$m" = spark ]; then has_spark=1; seen_spark=1
  elif [ $seen_spark = 0 ]; then before+=("$m")
  else after+=("$m"); fi
done

echo "changed against $SINCE: $(wc -l <<< "$changed") files"
echo "modules:      ${affected[*]:-none}"
if [ $has_spark = 1 ]; then
  if [ -n "$include" ]; then echo "spark suites: tagged ${!tags[*]}"; else echo "spark suites: all"; fi
fi
[ $web = 1 ] && echo "web:          docs or web changed"
[ $DRY = 1 ] && exit 0

join() { local IFS=,; echo "$*"; }
if [ ${#affected[@]} -gt 0 ] && [ $FORMAT = 1 ]; then
  "${MVN[@]}" -q -pl "$(join "${affected[@]}")" sortpom:sort
  "${MVN[@]}" -q validate spotless:apply
fi
[ ${#before[@]} -gt 0 ] && "${MVN[@]}" -pl "$(join "${before[@]}")" install "${EXTRA[@]}"
if [ $has_spark = 1 ]; then
  if [ -n "$include" ]; then "${MVN[@]}" -pl spark install "-DtagsToInclude=$include" "${EXTRA[@]}"
  else "${MVN[@]}" -pl spark install "${EXTRA[@]}"; fi
fi
[ ${#after[@]} -gt 0 ] && "${MVN[@]}" -pl "$(join "${after[@]}")" install "${EXTRA[@]}"
if [ $web = 1 ] && [ -d web/node_modules ]; then (cd web && npm run -s build); fi
echo "TEST_CHANGED_EXIT=0"
