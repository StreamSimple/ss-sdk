package com.streamsimple.sdk.client.pubsub;

import com.simplifi.it.javautil.serde.Deserializer;

public interface SyncSubscriberFactory<T>
{
  SyncSubscriber<T> create(String consumerGroup, Protocol.Subscriber protocol, Deserializer<T> deserializer);
}
