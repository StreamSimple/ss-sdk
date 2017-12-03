package com.streamsimple.ml.label.image.client;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.streamsimple.sdk.client.id.Id;
import com.streamsimple.sdk.client.id.IdGenerator;
import com.streamsimple.sdk.client.pubsub.Protocol;
import com.streamsimple.sdk.client.pubsub.Publisher;
import com.streamsimple.sdk.client.pubsub.PublisherFactoryImpl;
import com.streamsimple.sdk.data.serde.ProtobufSerializer;
import com.streamsimple.sdk.ml.data.ImageClassificationRequestOuterClass.ImageClassificationRequest;
import java.io.IOException;
import java.io.InputStream;

public class ImageClassificationPublisher
{
  private final IdGenerator idGenerator = new IdGenerator();
  private final Publisher<ImageClassificationRequest> publisher;

  public ImageClassificationPublisher(final Publisher<ImageClassificationRequest> publisher)
  {
    this.publisher = Preconditions.checkNotNull(publisher);
  }

  /**
   * <p>
   *   <b>Note: </b> This is more expensive because it involves a copy of the byte array. Use
   * </p>
   * @param image
   * @return
   * @throws IOException
   */
  public Id pub(byte[] image) throws IOException
  {
    return pub(ByteString.copyFrom(image));
  }

  public Id pub(InputStream imageStream) throws IOException
  {
    return pub(ByteString.readFrom(imageStream));
  }

  public Id pub(ByteString byteStringImage) throws IOException
  {
    final Id id = idGenerator.nextId();
    final ImageClassificationRequest request = ImageClassificationRequest.newBuilder()
        .setId(ByteString.copyFrom(id.getBytes()))
        .setImage(byteStringImage)
        .build();

    publisher.pub(request);
    return id;
  }

  public boolean isConnected()
  {
    return publisher.isConnected();
  }

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
