package com.streamsimple.sdk.client.pubsub;

import java.io.IOException;

public interface SyncSubscriber<T> extends AutoCloseable
{
  T next() throws IOException;

  boolean isClosed();
}
