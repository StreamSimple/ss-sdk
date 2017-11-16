package com.streamsimple.sdk.client.pubsub;

public class KafkaProtocol implements Protocol
{
  public Type getType()
  {
    return Type.KAFKA;
  }

  public static class Publisher extends KafkaProtocol implements Protocol.Publisher
  {

  }

  public static class Subscriber extends KafkaProtocol implements Protocol.Subscriber
  {

  }

  public static class BootstrapEndpoints
  {
  }
}
