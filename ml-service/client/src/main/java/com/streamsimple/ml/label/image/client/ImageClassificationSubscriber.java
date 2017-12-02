package com.streamsimple.ml.label.image.client;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.simplifi.it.javautil.serde.Deserializer;
import com.streamsimple.sdk.client.pubsub.Protocol;
import com.streamsimple.sdk.client.pubsub.SyncSubscriber;
import com.streamsimple.sdk.client.pubsub.SyncSubscriberFactoryImpl;
import com.streamsimple.sdk.ml.data.SingleLabelDeserializer;
import com.streamsimple.sdk.ml.data.SingleLabelOuterClass.SingleLabel;
import com.streamsimple.sdk.ml.data.TopNLabelDeserializer;
import com.streamsimple.sdk.ml.data.TopNLabelOuterClass.TopNLabel;
import java.io.IOException;

public class ImageClassificationSubscriber<T> implements SyncSubscriber<T>
{
  private final SyncSubscriber<T> subscriber;

  private ImageClassificationSubscriber(SyncSubscriber<T> subscriber)
  {
    this.subscriber = Preconditions.checkNotNull(subscriber);
  }

  @Override
  public T next() throws IOException
  {
    return subscriber.next();
  }

  @Override
  public boolean isClosed()
  {
    return subscriber.isClosed();
  }

  @Override
  public void close() throws Exception
  {
    subscriber.close();
  }

  public static class Builder
  {
    public Builder()
    {
    }

    public ImageClassificationSubscriber<SingleLabel> buildSingle(Protocol.Subscriber protocol) {
      final SyncSubscriber<SingleLabel> subscriber =
          new SyncSubscriberFactoryImpl<SingleLabel>()
          .create(protocol, new SingleLabelDeserializer());

      return new ImageClassificationSubscriber<>(subscriber);
    }

    public ImageClassificationSubscriber<TopNLabel> buildTopN(Protocol.Subscriber protocol) {
      final SyncSubscriber<TopNLabel> subscriber =
          new SyncSubscriberFactoryImpl<TopNLabel>()
          .create(protocol, new TopNLabelDeserializer());

      return new ImageClassificationSubscriber<>(subscriber);
    }
  }
}
