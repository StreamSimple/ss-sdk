package com.streamsimple.sdk.client.pubsub;

public interface Protocol
{
  enum Type
  {
    KAFKA,
    WEB_SOCKET
  }

  Type getType();

  interface Publisher extends Protocol
  {
  }

  interface Subscriber extends Protocol
  {

  }
}
