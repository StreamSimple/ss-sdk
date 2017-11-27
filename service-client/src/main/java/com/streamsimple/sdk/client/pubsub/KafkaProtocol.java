package com.streamsimple.sdk.client.pubsub;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.simplifi.it.javautil.err.ReturnError;
import com.simplifi.it.javautil.net.Endpoint;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class KafkaProtocol implements Protocol
{
  private static final String PROP_ACKS = "acks";
  private static final String PROP_BATCH_SIZE_BYTES = "batch.size";
  private static final String PROP_BUFFER_MEMORY_BYTES = "buffer.memory";
  private static final String PROP_SEND_TCP_BUFFER_BYTES = "send.buffer.bytes";
  private static final String PROP_ENABLE_AUTO_COMMIT = "enable.auto.commit";
  private static final String PROP_AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";

  private static final String PROP_NAME_BATCH_SIZE_BYTES = "batchSizeBytes";
  private static final String PROP_NAME_BUFFER_MEMORY_BYTES = "bufferMemoryBytes";
  private static final String PROP_NAME_SEND_TCP_BUFFER_BYTES = "sendTcpBufferBytes";
  private static final String PROP_NAME_AUTO_COMMIT_INTERVAL_MS = "autoCommitIntervalMS";
  private static final String PROP_NAME_MAX_BACKOFF_TIME = "maxBackoffTime";

  protected final String topic;
  protected final List<Endpoint> bootstrapEndpoints;
  protected final Properties properties;

  protected KafkaProtocol(final String topic, final List<Endpoint> bootstrapEndpoints, final Properties properties)
  {
    this.topic = Preconditions.checkNotNull(topic);
    this.bootstrapEndpoints = Preconditions.checkNotNull(bootstrapEndpoints);
    this.properties = Preconditions.checkNotNull(properties);
  }

  public Type getType()
  {
    return Type.KAFKA;
  }

  public String getTopic()
  {
    return topic;
  }

  public List<Endpoint> getBootstrapEndpoints()
  {
    return Lists.newArrayList(bootstrapEndpoints);
  }

  public String getBootstrapEndpointProp()
  {
    return buildBootstrapEndpointString(bootstrapEndpoints);
  }

  public Properties getProperties()
  {
    return new Properties(properties);
  }

  public static String buildBootstrapEndpointString(List<Endpoint> endpoints)
  {
    final Set<Endpoint> endpointSet = Sets.newHashSet(endpoints);

    Preconditions.checkArgument(endpointSet.size() == endpoints.size());

    final StringBuilder sb = new StringBuilder();
    String sep = "";

    for (Endpoint endpoint: endpoints) {
      sb.append(sep);
      sb.append(endpoint.toString());
      sep = ",";
    }

    return sb.toString();
  }

  protected static class Builder<T extends Builder>
  {
    protected Set<Endpoint> bootstrapEndpointsSet = Sets.newHashSet();
    protected List<Endpoint> bootstrapEndpoints = Lists.newArrayList();
    protected Properties properties = new Properties();
    protected String topic;

    public Builder()
    {
    }

    public T setTopic(final String topic)
    {
      this.topic = Preconditions.checkNotNull(topic);
      return (T)this;
    }

    public T addBootstrapEndpoint(final Endpoint endpoint)
    {
      Preconditions.checkNotNull(endpoint);
      Preconditions.checkArgument(bootstrapEndpointsSet.add(endpoint),
          String.format("The provided endpoint is a duplicate: %s", endpoint));
      bootstrapEndpoints.add(endpoint);
      return (T)this;
    }

    protected void validate()
    {
      Preconditions.checkState(topic != null, "The topic was not set");
      Preconditions.checkState(!bootstrapEndpointsSet.isEmpty(), "Atleast on bootstrap endpoint must be set.");
    }

    protected void postiveCheck(long value, String valueName) {
      if (value > 0) {
        return;
      }

      final String message = String.format("%s must be positive, but was (%d)", valueName, value);
      throw new IllegalArgumentException(message);
    }
  }

  public static class Publisher extends KafkaProtocol implements Protocol.Publisher
  {
    protected Publisher(String topic, List<Endpoint> bootstrapEndpoints, Properties properties)
    {
      super(topic, bootstrapEndpoints, properties);
    }

    public static class Builder extends KafkaProtocol.Builder
    {
      private ReturnError err;

      public Builder()
      {
      }

      public Builder setBatchSizeBytes(long numBytes)
      {
        postiveCheck(numBytes, PROP_NAME_BATCH_SIZE_BYTES);
        properties.put(PROP_BATCH_SIZE_BYTES, numBytes);
        return this;
      }

      public Builder setMemoryLimitBytes(long numBytes)
      {
        postiveCheck(numBytes, PROP_NAME_BUFFER_MEMORY_BYTES);
        properties.put(PROP_BUFFER_MEMORY_BYTES, numBytes);
        return this;
      }

      public Builder setTcpSendBufferBytes(long numBytes)
      {
        postiveCheck(numBytes, PROP_NAME_SEND_TCP_BUFFER_BYTES);
        properties.put(PROP_SEND_TCP_BUFFER_BYTES, numBytes);
        return this;
      }

      public KafkaProtocol.Publisher build()
      {
        validate();
        return new KafkaProtocol.Publisher(topic, bootstrapEndpoints, properties);
      }
    }
  }

  public static class Subscriber extends KafkaProtocol implements Protocol.Subscriber
  {
    private final long maxBackoffTime;

    protected Subscriber(final String topic, final List<Endpoint> bootstrapEndpoints,
                         final long maxBackoffTime, final Properties properties)
    {
      super(topic, bootstrapEndpoints, properties);

      this.maxBackoffTime = maxBackoffTime;
    }

    public long getMaxBackoffTime()
    {
      return maxBackoffTime;
    }

    public static class Builder extends KafkaProtocol.Builder
    {
      public static final long DEFAULT_MAX_BACKOFF_TIME = 30000L;

      private long maxBackoffTime = DEFAULT_MAX_BACKOFF_TIME;

      public Builder()
      {
      }

      public void setMaxBackoffTime(final long maxBackoffTime)
      {
        postiveCheck(maxBackoffTime, PROP_NAME_MAX_BACKOFF_TIME);
        this.maxBackoffTime = maxBackoffTime;
      }

      public KafkaProtocol.Subscriber build()
      {
        validate();
        return new KafkaProtocol.Subscriber(topic, bootstrapEndpoints, maxBackoffTime, properties);
      }
    }
  }
}
