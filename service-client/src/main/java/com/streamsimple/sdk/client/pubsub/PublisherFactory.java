package com.streamsimple.sdk.client.pubsub;

import com.simplifi.it.javautil.serde.Serializer;

public interface PublisherFactory<T>
{
  PublisherFactory<T> setHashingStrategy(HashingStrategy<T> hashingStrategy);

  Publisher<T> create(Protocol.Publisher protocol, Serializer<T> serializer);
}
