---
layout: default
title: Roadmap
description: Where VecRuntime is headed — the larger pieces of work ahead, and what is out of scope.
---

# Roadmap

A direction, not a commitment or a schedule: items land as they are designed, measured and proven not
to regress the benchmarks. Smaller work is in the
[issue tracker](https://github.com/vecruntime/vecruntime/issues); this page is for the themes big
enough to shape the engine.

## Deeper integration with Project Panama

VecRuntime is already built on the JDK's Foreign Function & Memory API — our columnar vectors are
off-heap `MemorySegment`s and the hot kernels use the Vector API for explicit SIMD. The direction is
to lean further into [Project Panama](https://openjdk.org/projects/panama/) as it matures: track the
FFM API as it stabilizes and adopt what lowers the native-boundary cost, widen the set of kernels on
explicit SIMD and re-tune lane widths and masks against each release's codegen as the Vector API
moves toward a stable (non-incubator) form.

## Arrow memory on the FFM model

Apache Arrow's Java library manages off-heap memory through its own `Unsafe`-based allocator. The
direction is to have Arrow's buffers sit directly on the FFM `MemorySegment` model VecRuntime already
uses, for one memory model end to end with no copy at the Arrow boundary. This **depends on upstream
work landing in Apache Arrow**; until then VecRuntime bridges between its FFM segments and Arrow's
allocator at the edges, and retires the bridge once Arrow exposes a first-class FFM-backed allocator.

## More input formats: JSON, CSV, ORC and Avro

The native scan today targets Parquet (reader and writer). The roadmap widens it to the other formats
lakes commonly hold, so more of a query runs on the vectorized path instead of falling back —
**JSON** and **CSV** (I/O- and parsing-bound text), and **ORC** and **Avro** (the other binary lake
formats), each read into the same buffers the rest of the engine consumes. Every format is added
behind the Parquet reader's contract: match Spark row for row or fall back cleanly, proven against a
conformance corpus, with no benchmark regression.

## Out of scope

These are deliberately **not** on the roadmap. Where a table uses one, VecRuntime falls back cleanly
to Spark's or Iceberg's own code and returns the same result:

- **Geospatial types** — `geometry` and `geography`.
- **Encryption** — Parquet modular encryption and encrypted Iceberg tables, on both read and write.
- **Shredded variant on the write side** — read natively, but the columnar writer declines it.

---

*Have a use case that should shape the roadmap?
[Open an issue](https://github.com/vecruntime/vecruntime/issues).*
