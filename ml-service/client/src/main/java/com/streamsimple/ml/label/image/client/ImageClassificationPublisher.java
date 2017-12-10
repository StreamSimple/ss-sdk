/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsimple.ml.label.image.client;

import java.io.IOException;
import java.io.InputStream;

import com.google.protobuf.ByteString;

import com.streamsimple.guava.common.base.Preconditions;
import com.streamsimple.sdk.client.id.Id;
import com.streamsimple.sdk.client.id.IdGenerator;
import com.streamsimple.sdk.client.pubsub.Protocol;
import com.streamsimple.sdk.client.pubsub.Publisher;
import com.streamsimple.sdk.client.pubsub.PublisherFactoryImpl;
import com.streamsimple.sdk.data.serde.ProtobufSerializer;
import com.streamsimple.sdk.ml.data.ImageClassificationRequestOuterClass.ImageClassificationRequest;

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

    public ImageClassificationPublisher build(Protocol.Publisher protocol)
    {
      final Publisher<ImageClassificationRequest> publisher =
          new PublisherFactoryImpl<ImageClassificationRequest>()
          .create(protocol, new ProtobufSerializer<>());

      return new ImageClassificationPublisher(publisher);
    }
  }
}
