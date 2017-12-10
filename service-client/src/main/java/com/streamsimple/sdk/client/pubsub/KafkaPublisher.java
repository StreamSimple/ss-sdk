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

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.streamsimple.guava.common.base.Preconditions;
import com.streamsimple.javautil.serde.Serializer;

public class KafkaPublisher<T> implements Publisher<T>
{
  private final KafkaProducer<byte[], byte[]> producer;
  private final String topic;
  private final Serializer<T> serializer;
  private boolean isConnected = true;

  protected KafkaPublisher(final PublisherFactoryImpl.OptimizationStrategy strategy,
      final KafkaProtocol.Publisher protocol,
      final Serializer<T> serializer)
  {
    final Properties props = protocol.getProperties();
    props.putAll(strategy.getProps());

    this.producer = new KafkaProducer<>(props);
    this.topic = protocol.getTopic();
    this.serializer = Preconditions.checkNotNull(serializer);
  }

  @Override
  public void pub(T tuple) throws IOException
  {
    byte[] tupleBytes = serializer.serialize(tuple);
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, tupleBytes);
    producer.send(record);
  }

  @Override
  public boolean isConnected()
  {
    return isConnected;
  }

  @Override
  public void close() throws Exception
  {
    isConnected = false;
  }
}
