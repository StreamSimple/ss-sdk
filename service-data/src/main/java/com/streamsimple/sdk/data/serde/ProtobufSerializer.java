package com.streamsimple.sdk.data.serde;

import com.google.protobuf.MessageLite;
import com.streamsimple.javautil.serde.Serializer;

public class ProtobufSerializer<T extends MessageLite> implements Serializer<T>
{
  @Override
  public byte[] serialize(T message)
  {
    return message.toByteArray();
  }
}
