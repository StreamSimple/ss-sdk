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

import com.streamsimple.guava.common.base.Preconditions;
import com.streamsimple.sdk.client.pubsub.Protocol;
import com.streamsimple.sdk.client.pubsub.SyncSubscriber;
import com.streamsimple.sdk.client.pubsub.SyncSubscriberFactoryImpl;
import com.streamsimple.sdk.ml.data.SingleLabelDeserializer;
import com.streamsimple.sdk.ml.data.SingleLabelOuterClass.SingleLabel;
import com.streamsimple.sdk.ml.data.TopNLabelDeserializer;
import com.streamsimple.sdk.ml.data.TopNLabelOuterClass.TopNLabel;

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

    public ImageClassificationSubscriber<SingleLabel> buildSingle(Protocol.Subscriber protocol)
    {
      final SyncSubscriber<SingleLabel> subscriber =
          new SyncSubscriberFactoryImpl<SingleLabel>()
          .create(protocol, new SingleLabelDeserializer());

      return new ImageClassificationSubscriber<>(subscriber);
    }

    public ImageClassificationSubscriber<TopNLabel> buildTopN(Protocol.Subscriber protocol)
    {
      final SyncSubscriber<TopNLabel> subscriber =
          new SyncSubscriberFactoryImpl<TopNLabel>()
          .create(protocol, new TopNLabelDeserializer());

      return new ImageClassificationSubscriber<>(subscriber);
    }
  }
}
