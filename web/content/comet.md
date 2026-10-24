---
layout: default
title: Comet as the scan
description: Using Apache DataFusion Comet's native scan and shuffle under VecRuntime's operators, zero-copy.
---

# Comet as the scan

VecRuntime runs its operators above whatever columnar batches sit below them. Apache
DataFusion Comet is one optional source: its native Parquet-to-Arrow reader can replace
Spark's scan, and our JVM SIMD operators run above it. The Arrow buffers Comet's reader
produces are wrapped as `MemorySegment`s with **no copy**, and dictionary-encoded string
columns stay encoded, so a `GROUP BY` string key hashes the dictionary once per batch.

Comet is optional. Without it, VecRuntime reads Parquet through its own native reader (see
[Apache Parquet](parquet.html)) or Spark's vectorized reader — neither needs Comet.

## Scan-only configuration

Comet is used in *scan-only* mode: its native reader on, its native operators off, our
operators above the scan. List Comet's plugin **first** so our rule sees `CometScanExec`
already in place.

```
--conf spark.plugins=org.apache.spark.CometPlugin,io.vecruntime.spark.VectorPlugin
--conf spark.comet.enabled=true
--conf spark.comet.scan.enabled=true
--conf spark.comet.exec.enabled=true
--conf spark.comet.exec.shuffle.enabled=false
--conf spark.comet.exec.project.enabled=false     # ... and the other spark.comet.exec.<op>.enabled keys off
--conf spark.memory.offHeap.enabled=true
--conf spark.memory.offHeap.size=2g
--conf spark.driver.extraJavaOptions="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow"
--conf spark.executor.extraJavaOptions="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow"
--jars comet-spark-spark4.1_2.13-1.0.0.jar,vecruntime-spark_4.1_2.13-0.0.9.jar
```

Comet 1.0 ships only the fully native DataFusion scan, which needs
`spark.comet.exec.enabled=true` and off-heap memory, so "scan-only" means enabling exec and
switching every Comet operator off individually. There is no compile-time dependency on
Comet: the adapter binds to Comet's classes reflectively, so the same VecRuntime jar works
with or without Comet on the classpath. On a session where Comet's scan is active, the
native Parquet reader steps aside unless `spark.vecruntime.scan.nativeParquet.enabled` is set
explicitly.

## Comet's native shuffle

Add Comet's shuffle manager and enable its shuffle:

```
--conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager
--conf spark.comet.exec.shuffle.enabled=true
```

Comet's planner will not hand a Comet shuffle to a child it does not recognise, and its
row-based "columnar" shuffle would convert our batches to rows and back. VecRuntime's rule
therefore rewrites any exchange above one of its operators into Comet's **native** shuffle
over one bridge node (`VectorToCometExec`): each column is exported through the Arrow C Data
Interface (written with the FFM API, no JNI on our side) and imported by Comet without a
copy. Hash, single, round-robin and range partitioning are rewritten; the sort above the
shuffle then becomes ours because the shuffle output is columnar. This is what makes TPC-H
Q1 with Comet's scan and shuffle fully accelerated.

## macOS

The Comet jars on Maven Central bundle Linux native libraries only. On macOS, build Comet
from source once (Rust toolchain, `protoc`, JDK 17 for the build); the resulting jar runs on
JDK 25 with VecRuntime. The command and the Comet-backed test instructions are on the
[detailed Comet page](https://github.com/vecruntime/vecruntime/blob/main/docs/comet.md).

## What is not combined

- Comet's native operators other than the scan and the shuffle are **not** combined with
  ours by default: a Comet final aggregate would need Comet's own partial buffers, and our
  operators are not Comet native nodes. The experimental mixed-chain pass
  (`spark.vecruntime.comet.mixed.enabled`, off by default) can place some of Comet's
  operators above ours; the study behind it shipped an empty allowlist.
- Comet's `LargeVarCharVector` (64-bit offsets) is not adapted zero-copy and falls back to
  the copying adapter.
- Dictionary-encoded strings are decoded when crossing *into* Comet (its stream reader
  decodes them anyway); everything else crosses as-is.

The mixed-chain design, the hybrid-planning study and the per-operator attribution are on
the [detailed Comet page](https://github.com/vecruntime/vecruntime/blob/main/docs/comet.md).
