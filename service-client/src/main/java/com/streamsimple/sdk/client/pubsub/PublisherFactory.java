package com.streamsimple.sdk.client.pubsub;

import com.streamsimple.javautil.serde.Serializer;

public interface PublisherFactory<T>
{
  Publisher<T> create(Protocol.Publisher protocol, Serializer<T> serializer);
}
