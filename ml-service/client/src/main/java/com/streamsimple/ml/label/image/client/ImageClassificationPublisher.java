package com.streamsimple.ml.label.image.client;

import com.google.common.base.Preconditions;
import com.streamsimple.sdk.client.pubsub.Protocol;
import com.streamsimple.sdk.client.pubsub.Publisher;
import com.streamsimple.sdk.client.pubsub.PublisherFactoryImpl;
import com.streamsimple.sdk.client.serde.ProtobufSerializer;
import com.streamsimple.sdk.ml.data.ImageClassificationRequestOuterClass.ImageClassificationRequest;
import java.io.IOException;

public class ImageClassificationPublisher implements Publisher<ImageClassificationRequest>
{
  private final Publisher<ImageClassificationRequest> publisher;

  public ImageClassificationPublisher(final Publisher<ImageClassificationRequest> publisher)
  {
    this.publisher = Preconditions.checkNotNull(publisher);
  }

  @Override
  public void pub(ImageClassificationRequest request) throws IOException
  {
    publisher.pub(request);
  }

  @Override
  public boolean isConnected()
  {
    return publisher.isConnected();
  }

  @Override
  public void close() throws Exception
  {
    publisher.close();
  }

  public static class Builder
  {
    public Builder()
    {
    }

    public ImageClassificationPublisher build(Protocol.Publisher protocol) {
      final Publisher<ImageClassificationRequest> publisher =
          new PublisherFactoryImpl<ImageClassificationRequest>()
          .create(protocol, new ProtobufSerializer<>());

      return new ImageClassificationPublisher(publisher);
    }
  }
}
