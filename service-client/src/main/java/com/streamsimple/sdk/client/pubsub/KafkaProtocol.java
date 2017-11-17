package com.streamsimple.sdk.client.pubsub;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.simplifi.it.javautil.err.Result;
import com.simplifi.it.javautil.err.ReturnError;
import com.simplifi.it.javautil.err.ReturnErrorImpl;
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

  private static final String PROP_NAME_BATCH_SIZE_BYTES = "batchSizeBytes";
  private static final String PROP_NAME_BUFFER_MEMORY_BYTES = "bufferMemoryBytes";
  private static final String PROP_NAME_SEND_TCP_BUFFER_BYTES = "sendTcpBufferBytes";

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

  public Properties getProperties()
  {
    return new Properties(properties);
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

      private void postiveCheck(long value, String valueName) {
        if (value > 0) {
          return;
        }

        this.err = ReturnErrorImpl.create("%s must be positive, but was (%d)", valueName, value);
      }

      public Result<KafkaProtocol> build()
      {
        if (err != null) {
          return Result.create(err);
        }

        return Result.create(new KafkaProtocol.Publisher(topic, bootstrapEndpoints, properties));
      }
    }
  }

  public static class Subscriber extends KafkaProtocol implements Protocol.Subscriber
  {
    protected Subscriber(String topic, List<Endpoint> bootstrapEndpoints, Properties properties)
    {
      super(topic, bootstrapEndpoints, properties);
    }

    public static class Builder extends KafkaProtocol.Builder
    {
      public Builder()
      {
      }
    }
  }
}
