package com.streamsimple.sdk.client.pubsub;

public interface FaulTolerantPublisherFactory
{
  FaultTolerantPublisher create(Protocol.Publisher protocol);
}
