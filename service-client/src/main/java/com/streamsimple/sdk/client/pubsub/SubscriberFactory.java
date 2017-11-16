package com.streamsimple.sdk.client.pubsub;

public interface SubscriberFactory
{
  Subscriber create(Protocol.Subscriber protocol);
}
