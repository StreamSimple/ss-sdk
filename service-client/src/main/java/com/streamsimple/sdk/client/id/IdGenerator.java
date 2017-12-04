package com.streamsimple.sdk.client.id;

import com.google.common.annotations.VisibleForTesting;
import com.streamsimple.javautil.serde.SerdeUtils;

public class IdGenerator
{
  private long timestamp;
  private long counter;

  public IdGenerator()
  {
    timestamp = System.currentTimeMillis();
  }

  @VisibleForTesting
  protected IdGenerator(long timestamp)
  {
    this.timestamp = timestamp;
  }

  public Id nextId()
  {
    byte[] id = new byte[2 * SerdeUtils.NUM_BYTES_LONG];
    SerdeUtils.serializeLong(timestamp, 0, id);
    SerdeUtils.serializeLong(counter, SerdeUtils.NUM_BYTES_LONG, id);
    counter++;

    return new Id(id);
  }
}
