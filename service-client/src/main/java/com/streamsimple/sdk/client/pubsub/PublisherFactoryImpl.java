package com.streamsimple.sdk.client.pubsub;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.simplifi.it.javautil.serde.Serializer;
import java.util.Map;

public class PublisherFactoryImpl<T> implements PublisherFactory<T>
{
  private OptimizationStrategy opStrategy = OptimizationStrategy.BOTH;

  enum OptimizationStrategy
  {
    LATENCY(new ImmutableMap.Builder<String, String>()
        .build()),
    THROUGHPUT(new ImmutableMap.Builder<String, String>()
        .build()),
    BOTH(new ImmutableMap.Builder<String, String>()
        .build());

    private final Map<String, String> props;

    OptimizationStrategy(Map<String, String> props)
    {
      this.props = Preconditions.checkNotNull(props);
    }

    public Map<String, String> getProps()
    {
      return props;
    }
  }

  public PublisherFactoryImpl()
  {
  }

  public PublisherFactoryImpl setOptimizationStrategy(OptimizationStrategy strategy)
  {
    this.opStrategy = Preconditions.checkNotNull(strategy);
    return this;
  }

  @Override
  public Publisher<T> create(String topic, Protocol.Publisher protocol, Serializer<T> serializer)
  {
    switch (protocol.getType()) {
      case KAFKA: {
        return new KafkaPublisher<T>(opStrategy, topic, (KafkaProtocol.Publisher)protocol, serializer);
      }
      default:
        throw new UnsupportedOperationException();
    }
  }
}
