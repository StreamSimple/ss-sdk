package com.streamsimple.sdk.client.pubsub;

import java.io.IOException;

/**
 * No Fault-tolerance gaurantees in the event of client failure.
 */
public interface SyncSubscriber<T> extends AutoCloseable
{
  T next() throws IOException;

  boolean isClosed();
}
