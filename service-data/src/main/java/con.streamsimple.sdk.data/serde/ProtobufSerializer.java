package com.streamsimple.sdk.client.serde;

import com.google.protobuf.MessageLite;
import com.simplifi.it.javautil.serde.Serializer;

public class ProtobufSerializer<T extends MessageLite> implements Serializer<T>
{
  @Override
  public byte[] serialize(T message)
  {
    return message.toByteArray();
  }
}
