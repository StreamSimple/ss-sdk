package com.streamsimple.sdk.client.pubsub;

import com.simplifi.it.javautil.serde.Deserializer;

public class SyncSubscriberFactoryImpl<T> implements SyncSubscriberFactory<T>
{
  public SyncSubscriberFactoryImpl()
  {
  }

  @Override
  public SyncSubscriber<T> create(String consumerGroup, Protocol.Subscriber protocol, Deserializer<T> deserializer)
  {
    switch (protocol.getType()) {
      case KAFKA: {
        return new SyncKafkaSubscriber<T>(consumerGroup, (KafkaProtocol.Subscriber)protocol, deserializer);
      }
      default:
        throw new UnsupportedOperationException();
    }
  }
}
