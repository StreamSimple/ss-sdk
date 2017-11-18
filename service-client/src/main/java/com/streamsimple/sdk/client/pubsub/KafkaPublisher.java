package com.streamsimple.sdk.client.pubsub;

import com.simplifi.it.javautil.serde.Serializer;
import java.io.IOException;

public class KafkaPublisher<T> implements Publisher<T>
{
  protected KafkaPublisher(final KafkaProtocol.Publisher protocol,
                           final HashingStrategy<T> hashingStrategy,
                           final Serializer<T> serializer)
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
