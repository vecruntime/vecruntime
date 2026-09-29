package io.vecruntime.s3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.VectoredIOContext;
import org.apache.hadoop.fs.s3a.impl.streams.AbstractObjectInputStreamFactory;
import org.apache.hadoop.fs.s3a.impl.streams.AnalyticsStream;
import org.apache.hadoop.fs.s3a.impl.streams.FactoryBindingParameters;
import org.apache.hadoop.fs.s3a.impl.streams.InputStreamType;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStream;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectReadParameters;
import org.apache.hadoop.fs.s3a.impl.streams.StreamFactoryRequirements;
import org.apache.hadoop.fs.s3a.impl.streams.StreamIntegration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;

/**
 * S3A's Analytics Accelerator stream, served by the AWS CRT S3 client.
 *
 * <p>Hadoop 3.4.3's own {@code AnalyticsStreamFactory} (the default {@code fs.s3a.input.stream.type})
 * hands the Analytics Accelerator Library S3A's synchronous SDK client, so every prefetch and vectored
 * range is a blocking GET over the SDK's Java HTTP stack. The library's README recommends the CRT
 * client instead, for its connection pool and download throughput. This factory is the same stream
 * (same {@link AnalyticsStream}, same {@code fs.s3a.analytics.accelerator.*} configuration, same
 * vectored-read context) with one difference: the object client is an {@link S3SdkObjectClient} over an
 * {@code S3CrtAsyncClient}.
 *
 * <p>Enable with
 * <pre>
 *   fs.s3a.input.stream.type=custom
 *   fs.s3a.input.stream.custom.factory=io.vecruntime.s3.CrtAnalyticsStreamFactory
 * </pre>
 * The CRT client needs {@code aws-crt} (a JNI library) on the class path. Credentials come from the SDK's
 * default chain (web identity under IRSA) and the region from {@code fs.s3a.endpoint.region}, else the
 * SDK's region chain. {@value #MAX_CONCURRENCY_KEY} caps the client's concurrent connections
 * (default {@value #DEFAULT_MAX_CONCURRENCY}), independently of {@code fs.s3a.connection.maximum},
 * which keeps sizing S3A's own client.
 */
public final class CrtAnalyticsStreamFactory extends AbstractObjectInputStreamFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CrtAnalyticsStreamFactory.class);

  /** Maximum concurrent connections of the CRT client, per JVM. */
  public static final String MAX_CONCURRENCY_KEY = "fs.s3a.vecruntime.crt.max-concurrency";

  public static final int DEFAULT_MAX_CONCURRENCY = 600;

  /** The prefix S3A's own Analytics Accelerator factory reads the library's settings from. */
  static final String AAL_PREFIX = "fs.s3a.analytics.accelerator";

  private static final String REGION_KEY = "fs.s3a.endpoint.region";

  private S3SeekableInputStreamConfiguration streamConfiguration;
  private int maxConcurrency;
  private String region;
  private volatile S3SeekableInputStreamFactory streamFactory;

  public CrtAnalyticsStreamFactory() {
    super("CrtAnalyticsStreamFactory");
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    streamConfiguration =
        S3SeekableInputStreamConfiguration.fromConfiguration(new ConnectorConfiguration(conf, AAL_PREFIX));
    maxConcurrency = conf.getInt(MAX_CONCURRENCY_KEY, DEFAULT_MAX_CONCURRENCY);
    if (maxConcurrency <= 0) {
      throw new IllegalArgumentException(MAX_CONCURRENCY_KEY + " must be positive, was " + maxConcurrency);
    }
    region = conf.getTrimmed(REGION_KEY, "");
  }

  @Override
  public void bind(FactoryBindingParameters parameters) throws IOException {
    super.bind(parameters);
  }

  @Override
  public ObjectInputStream readObject(ObjectReadParameters parameters) throws IOException {
    return new AnalyticsStream(parameters, factory());
  }

  /** One CRT client per S3A file system instance, built on first read. */
  private S3SeekableInputStreamFactory factory() {
    S3SeekableInputStreamFactory f = streamFactory;
    if (f == null) {
      synchronized (this) {
        f = streamFactory;
        if (f == null) {
          Region r = region.isEmpty() ? new DefaultAwsRegionProviderChain().getRegion() : Region.of(region);
          S3AsyncClient crt = S3AsyncClient.crtBuilder()
              .region(r)
              .credentialsProvider(DefaultCredentialsProvider.builder().build())
              .maxConcurrency(maxConcurrency)
              .build();
          LOG.info("CRT S3 client for the analytics stream: region {}, max concurrency {}", r, maxConcurrency);
          f = new S3SeekableInputStreamFactory(new S3SdkObjectClient(crt), streamConfiguration);
          streamFactory = f;
        }
      }
    }
    return f;
  }

  @Override
  public InputStreamType streamType() {
    return InputStreamType.Custom;
  }

  /** As S3A's analytics factory: no S3A thread pool, and vectored reads never coalesce across gaps. */
  @Override
  public StreamFactoryRequirements factoryRequirements() {
    VectoredIOContext vectored = StreamIntegration.populateVectoredIOContext(getConfig());
    vectored.setMinSeekForVectoredReads(0);
    return new StreamFactoryRequirements(0, 0, vectored);
  }

  @Override
  protected void serviceStop() throws Exception {
    S3SeekableInputStreamFactory f = streamFactory;
    streamFactory = null;
    if (f != null) {
      try {
        f.close();
      } catch (Exception e) {
        LOG.debug("Ignored exception while closing the CRT stream factory", e);
      }
    }
    super.serviceStop();
  }
}
