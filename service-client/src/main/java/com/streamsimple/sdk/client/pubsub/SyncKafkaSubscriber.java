package com.streamsimple.sdk.client.pubsub;

import java.io.IOException;

public class SyncKafkaSubscriber<T> implements SyncSubscriber<T>
{
  protected SyncKafkaSubscriber(final KafkaProtocol.Subscriber protocol)
  {

  }

  @Override
  public T next() throws IOException
  {
    return null;
  }

  @Override
  public boolean isClosed()
  {
    return false;
  }

  @Override
  public void close() throws Exception
  {
  }
}
