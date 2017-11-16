package com.streamsimple.sdk.client.pubsub;

import java.io.IOException;

/**
 * Exactly-once message delivery even in the case of client failure.
 * @param <T> The type of the data being sent.
 */
public interface FaultTolerantPublisher<T> extends AutoCloseable
{
  void pub(T tuple) throws IOException;
  boolean isConnected();
  Offset getOffset();
  void commit(Offset offset);

  interface CheckpointStrategy
  {
    interface Config
    {
    }
  }

  interface Offset
  {
  }
}
