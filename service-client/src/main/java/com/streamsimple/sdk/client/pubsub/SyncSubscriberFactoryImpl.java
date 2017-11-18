package com.streamsimple.sdk.client.pubsub;

public class SyncSubscriberFactoryImpl<T> implements SyncSubscriberFactory<T>
{
  public SyncSubscriberFactoryImpl()
  {
  }

  @Override
  public SyncSubscriber<T> create(Protocol.Subscriber protocol)
  {
    switch (protocol.getType()) {
      case KAFKA: {
        return new SyncKafkaSubscriber<T>((KafkaProtocol.Subscriber)protocol);
      }
      default:
        throw new UnsupportedOperationException();
    }
  }
}
