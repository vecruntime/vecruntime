---
title: Roadmap
description: Where VecRuntime is headed — the larger pieces of work on the horizon.
---

# Roadmap

This page sketches the larger pieces of work on VecRuntime's horizon. It is a
direction, not a commitment or a schedule: items land as they are designed,
measured and proven not to regress the benchmarks. Smaller work is tracked in
the [issue tracker](https://github.com/vecruntime/vecruntime/issues); this page
is for the themes big enough to shape the engine.

## Deeper integration with Project Panama

VecRuntime is already built on the JDK's Foreign Function & Memory API: our
columnar vectors are off-heap `MemorySegment`s, and the native boundary is
crossed through the Panama FFM machinery rather than legacy JNI. The roadmap is
to lean further into [OpenJDK Project Panama](https://openjdk.org/projects/panama/)
as it matures:

- Track the FFM API as it stabilizes across JDK releases and adopt the
  improvements that reduce the cost of the native boundary.
- Evaluate the **Vector API** (Panama's SIMD incubator) for the hot
  per-row/per-batch kernels where explicit vectorization beats what the JIT
  autovectorizes today.
- Keep allocation, arena lifecycle and the segment accessors aligned with where
  Panama is going, so the engine benefits from the platform's work rather than
  working around it.

## Arrow memory on the new foreign-memory model (FFM)

Apache Arrow's Java library historically manages off-heap memory through its own
allocator built on `Unsafe`/`DirectByteBuffer`. The direction here is to have
Arrow's buffers sit directly on the **FFM `MemorySegment`** model — the same
foreign-memory foundation VecRuntime already uses — so there is one memory model
end to end, with no copy or wrapper at the Arrow boundary and a single place that
accounts off-heap bytes.

This depends on upstream work in the Arrow project: we are **waiting on the
relevant pull request landing in Apache Arrow** before we can adopt it. Until
then, VecRuntime bridges between its FFM segments and Arrow's allocator at the
edges; once Arrow exposes a first-class FFM-backed allocator, we move to it and
retire the bridge.

## More input formats: JSON, CSV, ORC and Avro

The native scan today targets Parquet (reader and writer). The roadmap is to
widen the engine's reach to the other formats data lakes commonly hold, so more
of a query runs on the vectorized path instead of falling back:

- **JSON** and **CSV** — text formats where, as DuckDB's async-I/O work shows,
  the scan is dominated by I/O and parsing throughput; strong candidates for the
  vectorized, overlapped-I/O treatment the Parquet scan already gets.
- **ORC** and **Avro** — the other columnar/row binary formats common in
  existing lakes, read into the same `VectorBuffers` the rest of the engine
  consumes, so a table in any of these formats benefits from the native
  operators without a per-format rewrite.

Each format is added behind the same contract as the Parquet reader: results
must match Spark's row for row (or fall back cleanly), proven against a
conformance corpus, with no regression on the benchmark suite.

---

*Have a use case that should shape the roadmap? Please
[open an issue](https://github.com/vecruntime/vecruntime/issues).*
