package com.streamsimple.sdk.client.pubsub;

import com.streamsimple.javautil.serde.Deserializer;

public interface SyncSubscriberFactory<T>
{
  SyncSubscriber<T> create(Protocol.Subscriber protocol, Deserializer<T> deserializer);
}
