package com.streamsimple.sdk.client.pubsub;

public interface PublisherFactory
{
  Publisher create(Protocol.Publisher protocol);
}
