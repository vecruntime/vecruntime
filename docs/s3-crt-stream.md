---
title: S3 reads over the AWS CRT
---

# S3 reads over the AWS CRT

The benchmark image reads S3 through S3A. Hadoop 3.4.3 makes the
[Analytics Accelerator Library](https://github.com/awslabs/analytics-accelerator-s3) stream its default
(`fs.s3a.input.stream.type=analytics`), giving it Parquet footer caching, column prefetching and vectored
reads. S3A hands that library its own **synchronous** SDK client, so every range is a blocking GET over
the SDK's Java HTTP stack. The library recommends the AWS CRT S3 client for its connection pool and
download throughput.

The optional `s3-crt-stream` module (`io.vecruntime.s3.CrtAnalyticsStreamFactory`) is that same stream
(same `AnalyticsStream`, same `fs.s3a.analytics.accelerator.*` settings, same vectored-read context)
over an `S3CrtAsyncClient`. The engine never depends on it. The image ships the jar and `aws-crt`
(a JNI library, glibc builds for x86-64 and aarch64), and uses it only when a run asks:

```
spark.hadoop.fs.s3a.input.stream.type=custom
spark.hadoop.fs.s3a.input.stream.custom.factory=io.vecruntime.s3.CrtAnalyticsStreamFactory
spark.hadoop.fs.s3a.vecruntime.crt.max-concurrency=600
```

| Key | Default | Meaning |
|---|---|---|
| `fs.s3a.vecruntime.crt.max-concurrency` | 600 | The CRT client's concurrent connections, per JVM. S3A's own client keeps `fs.s3a.connection.maximum`. |
| `fs.s3a.endpoint.region` | SDK region chain | The CRT client's region. |

Credentials come from the SDK's default chain, which covers web identity under IRSA. S3A's
`fs.s3a.aws.credentials.provider` isn't consulted for the stream's GETs; listings and other S3A calls
still use it.

Build it with `mvn -Ps3-crt -pl s3-crt-stream package`. It isn't in the gate's module list.
