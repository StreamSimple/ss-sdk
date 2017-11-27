package com.streamsimple.sdk.client.pubsub;

import com.simplifi.it.javautil.serde.Serializer;

public interface PublisherFactory<T>
{
  Publisher<T> create(String topic, Protocol.Publisher protocol, Serializer<T> serializer);
}
