package com.streamsimple.sdk.client.pubsub;

import java.io.IOException;

/**
 * No Fault-tolerance gaurantees in the event of client failure.
 */
public interface Publisher<T> extends AutoCloseable
{
  void pub(T tuple) throws IOException;
  boolean isConnected();
}
