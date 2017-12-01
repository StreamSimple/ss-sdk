package com.streamsimple.sdk.client.id;

import java.util.Arrays;

public class Id
{
  private final byte[] id;

  public Id(byte[] id)
  {
    this.id = id;
  }

  public byte[] getBytes()
  {
    return id;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Id id1 = (Id) o;

    return Arrays.equals(id, id1.id);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(id);
  }

  @Override
  public String toString()
  {
    return "Id{" +
        "id=" + Arrays.toString(id) +
        '}';
  }
}
