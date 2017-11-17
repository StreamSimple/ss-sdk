package com.streamsimple.sdk.client.pubsub;

import com.google.common.base.Preconditions;

public class PublisherFactoryImpl<T> implements PublisherFactory<T>
{
  private HashingStrategy<T> hashingStrategy = new HashingStrategy.Default<T>();

  public PublisherFactoryImpl()
  {
  }

  @Override
  public PublisherFactory<T> setHashingStrategy(HashingStrategy<T> hashingStrategy)
  {
    this.hashingStrategy = Preconditions.checkNotNull(hashingStrategy);
    return this;
  }

  @Override
  public Publisher<T> create(Protocol.Publisher protocol)
  {
    switch (protocol.getType()) {
      case KAFKA: {
        return new KafkaPublisher<T>((KafkaProtocol.Publisher)protocol, hashingStrategy);
      }
      default:
        throw new UnsupportedOperationException();
    }
  }
}
