package com.streamsimple.sdk.client.pubsub;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.streamsimple.javautil.net.Endpoint;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class KafkaProtocol implements Protocol
{
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
    this.properties = new Properties();
    this.properties.putAll(properties);
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

  public String getBootstrapEndpointsProp()
  {
    return buildBootstrapEndpointString(bootstrapEndpoints);
  }

  public Properties getProperties()
  {
    final Properties props = new Properties();
    props.putAll(properties);
    return props;
  }

  public static String buildBootstrapEndpointString(List<Endpoint> endpoints)
  {
    final Set<Endpoint> endpointSet = Sets.newHashSet(endpoints);

    Preconditions.checkArgument(endpointSet.size() == endpoints.size());

    return StringUtils.join(endpoints, ',');
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

    public T addBootstrapEndpoints(final List<Endpoint> endpoints)
    {
      for (Endpoint endpoint: endpoints) {
        addBootstrapEndpoint(endpoint);
      }

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

    public static class Builder extends KafkaProtocol.Builder<KafkaProtocol.Publisher.Builder>
    {
      public Builder()
      {
      }

      public Builder setBatchSizeBytes(long numBytes)
      {
        postiveCheck(numBytes, PROP_NAME_BATCH_SIZE_BYTES);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, numBytes);
        return this;
      }

      public Builder setMemoryLimitBytes(long numBytes)
      {
        postiveCheck(numBytes, PROP_NAME_BUFFER_MEMORY_BYTES);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, numBytes);
        return this;
      }

      public Builder setTcpSendBufferBytes(long numBytes)
      {
        postiveCheck(numBytes, PROP_NAME_SEND_TCP_BUFFER_BYTES);
        properties.put(CommonClientConfigs.SEND_BUFFER_CONFIG, numBytes);
        return this;
      }

      public KafkaProtocol.Publisher build()
      {
        validate();

        properties.setProperty("linger.ms", "0");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, buildBootstrapEndpointString(bootstrapEndpoints));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());

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

    public static class Builder extends KafkaProtocol.Builder<KafkaProtocol.Subscriber.Builder>
    {
      public static final long DEFAULT_MAX_BACKOFF_TIME = 30_000L;
      public static final long DEFAULT_AUTOCOMMIT_INTERVAL = 30_000L;

      private long maxBackoffTime = DEFAULT_MAX_BACKOFF_TIME;
      private long autocommitInterval = DEFAULT_AUTOCOMMIT_INTERVAL;

      public Builder()
      {
      }

      public KafkaProtocol.Subscriber.Builder setMaxBackoffTime(final long maxBackoffTime)
      {
        postiveCheck(maxBackoffTime, PROP_NAME_MAX_BACKOFF_TIME);
        this.maxBackoffTime = maxBackoffTime;
        return this;
      }

      public KafkaProtocol.Subscriber.Builder setAutocommitInterval(final long autocommitInterval)
      {
        postiveCheck(autocommitInterval, PROP_NAME_AUTO_COMMIT_INTERVAL_MS);
        this.autocommitInterval = autocommitInterval;
        return this;
      }

      public KafkaProtocol.Subscriber build(final String consumerGroup)
      {
        Preconditions.checkNotNull(consumerGroup);

        validate();

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            buildBootstrapEndpointString(bootstrapEndpoints));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getCanonicalName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getCanonicalName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, java.lang.Boolean.TRUE.toString());
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Long.toString(autocommitInterval));

        return new KafkaProtocol.Subscriber(topic, bootstrapEndpoints, maxBackoffTime, properties);
      }
    }
  }
}
