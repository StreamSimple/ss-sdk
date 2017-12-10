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
package com.streamsimple.sdk.client.pubsub;

import java.util.Map;

import com.streamsimple.guava.common.base.Preconditions;
import com.streamsimple.guava.common.collect.ImmutableMap;
import com.streamsimple.javautil.serde.Serializer;

public class PublisherFactoryImpl<T> implements PublisherFactory<T>
{
  private OptimizationStrategy opStrategy = OptimizationStrategy.BOTH;

  enum OptimizationStrategy
  {
    LATENCY(new ImmutableMap.Builder<String, String>()
        .build()),
    THROUGHPUT(new ImmutableMap.Builder<String, String>()
        .build()),
    BOTH(new ImmutableMap.Builder<String, String>()
        .build());

    private final Map<String, String> props;

    OptimizationStrategy(Map<String, String> props)
    {
      this.props = Preconditions.checkNotNull(props);
    }

    public Map<String, String> getProps()
    {
      return props;
    }
  }

  public PublisherFactoryImpl()
  {
  }

  public PublisherFactoryImpl setOptimizationStrategy(OptimizationStrategy strategy)
  {
    this.opStrategy = Preconditions.checkNotNull(strategy);
    return this;
  }

  @Override
  public Publisher<T> create(Protocol.Publisher protocol, Serializer<T> serializer)
  {
    switch (protocol.getType()) {
      case KAFKA: {
        return new KafkaPublisher<T>(opStrategy, (KafkaProtocol.Publisher)protocol, serializer);
      }
      default:
        throw new UnsupportedOperationException();
    }
  }
}
