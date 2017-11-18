package com.streamsimple.sdk.client.pubsub;

public interface SyncSubscriberFactory<T>
{
  SyncSubscriber<T> create(Protocol.Subscriber protocol);
}
