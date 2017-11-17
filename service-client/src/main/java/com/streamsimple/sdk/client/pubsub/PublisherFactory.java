package com.streamsimple.sdk.client.pubsub;

public interface PublisherFactory<T>
{
  PublisherFactory<T> setHashingStrategy(HashingStrategy<T> hashingStrategy);

  Publisher<T> create(Protocol.Publisher protocol);
}
