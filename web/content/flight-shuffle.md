---
layout: default
title: Flight shuffle
description: The columnar shuffle that moves Arrow record batches between executors over Arrow Flight — what it is, how to turn it on, its settings and its limits.
---

# Flight shuffle

Spark's own shuffle is row-based: a columnar operator feeding it converts every batch to rows, the
writer serialises them, and the reducer deserialises rows the next operator turns back into columns —
two conversions and a serialiser round trip per stage. VecRuntime's shuffle moves **Arrow record
batches** instead. The map side partitions a batch and writes one data file per task; the reduce side
streams the bytes back and decodes them straight into the Arrow vectors the operators read in place.
Nothing is a row in between.

Like Comet's, it is a `ShuffleManager` that handles only its own columnar dependency and leaves every
other shuffle to Spark's sort shuffle untouched.

The detailed page — the file format, the reduce path, the Flight service, every tuning decision and
its measurement — is the
[Flight shuffle design page](https://github.com/vecruntime/vecruntime/blob/main/docs/flight-shuffle.md).

## What it is

- **The map file.** `PartitionedIpcWriter` writes one data file per map task: one IPC stream per
  partition, then one dictionary section for the whole file, then a trailer. Partition offsets and
  lengths go to Spark's ordinary shuffle index file, so `MapStatus`, the map output tracker,
  speculation and shuffle cleanup are all Spark's own.
- **Strings travel as dictionary ids** over one task-wide dictionary written once per map file, not
  one per record batch (#416). A column whose dictionary stops paying is frozen and travels plain
  UTF8 for the rest of the task.
- **The reduce side** opens one `FlightBlockStream` per remote executor up front and reads local map
  outputs straight from disk, feeding both through one decoder; small batches are coalesced so the
  operators are not set up per ten-row block.
- **The transport** is one Arrow Flight server per executor over gRPC on Spark's Netty. A `DoGet`
  names the shuffle, the reduce range and the executor's map ids; the server prepends the dictionary
  section and streams the bytes in chunks (default 4 MB). Executor locations travel through Spark's
  plugin mechanism and are cached per reducer.

## How to enable it

The shuffle needs its module's shuffle manager registered and the switch on:

```
spark.shuffle.manager=org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager
spark.vecruntime.shuffle.enabled=true
```

This is the `vector-shuffle` benchmark configuration. The VecRuntime operators (`spark.plugins`) must
already be in the session; the columnar exchange only replaces the shuffle between operators that are
already ours.

## Settings

Everything works at its default; these are the knobs worth knowing.

| Key | Default | What it moves |
|---|---|---|
| `spark.vecruntime.shuffle.enabled` | `true` | the columnar exchange (with the shuffle manager above) |
| `spark.vecruntime.shuffle.backend` | `flight` | `flight` (the Arrow Flight data plane), `block` (Spark's block transfer over the same files, per-block dictionaries), or a backend class name |
| `spark.vecruntime.shuffle.batchRows` / `batchBytes` | `8192` / `1m` | record-batch size on the map side |
| `spark.vecruntime.shuffle.bufferBytes` | `64m` | task-wide held bytes before a forced flush |
| `spark.vecruntime.shuffle.flushBytes` | `1m` | compression frame size (one zstd call per frame) |
| `spark.vecruntime.shuffle.compression` | `zstd` | `zstd`, `lz4`, or `none` |
| `spark.vecruntime.shuffle.writer.dictionaryMaxRatio` | `0.5` | distinct/rows above which a string column is frozen to plain UTF8 (`0` never encodes, `1` always) |
| `spark.vecruntime.shuffle.flight.threads` | `max(4, cores)` | the server's pool — concurrent `DoGet`s an executor serves |
| `spark.vecruntime.shuffle.flight.bindHost` | block-manager host | the server's bind address |
| `spark.vecruntime.shuffle.flight.chunkBytes` | `4m` | bytes per gRPC message |
| `spark.vecruntime.shuffle.flight.channelsPerPeer` | `1` | gRPC channels (TCP connections) per remote executor |
| `spark.vecruntime.shuffle.flight.clientWindow` | `0` (gRPC's auto-tuned) | a fixed HTTP/2 flow-control window for the client |
| `spark.vecruntime.shuffle.flight.backpressureBytes` | `0` (no wait) | queued outbound bytes per stream above which the server waits before the next message |

## Limits

- **TLS is not supported on the Flight server.** With `spark.ssl.rpc.enabled` on, the server refuses
  to start rather than run in the clear — use `spark.vecruntime.shuffle.backend=block` there. With
  `spark.authenticate` on, every call carries Spark's shuffle secret as a bearer token and
  unauthenticated requests are refused; without it the endpoint is as open as Spark's own block
  transfer.
- **Only the columnar shuffle is accelerated.** Any shuffle between non-columnar operators stays on
  Spark's sort shuffle.
- A **push-based shuffle service** (Celeborn-style) is not implemented; the backend seam is where one
  would plug in, and is future work.
- The **block-transfer backend** keeps the older per-block dictionary format rather than the one
  task-wide dictionary the Flight path uses.

---

The full file format, reduce path, Flight service and the measurements behind every choice are on the
[Flight shuffle design page](https://github.com/vecruntime/vecruntime/blob/main/docs/flight-shuffle.md).
