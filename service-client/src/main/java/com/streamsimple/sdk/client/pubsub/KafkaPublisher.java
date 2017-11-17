package com.streamsimple.sdk.client.pubsub;

import java.io.IOException;

public class KafkaPublisher<T> implements Publisher<T>
{
  protected KafkaPublisher(final KafkaProtocol.Publisher protocol,
                           final HashingStrategy<T> hashingStrategy)
  {
  }

  @Override
  public void pub(T tuple) throws IOException
  {
  }

  @Override
  public boolean isConnected()
  {
    return false;
  }

  @Override
  public void close() throws Exception
  {
  }
}
