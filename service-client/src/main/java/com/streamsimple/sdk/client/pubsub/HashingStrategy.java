package com.streamsimple.sdk.client.pubsub;

public interface HashingStrategy<T>
{
  int hashCode(T value);

  class Default<T> implements HashingStrategy<T>
  {
    public Default()
    {
    }

    @Override
    public int hashCode(T value)
    {
      return value.hashCode();
    }
  }
}
