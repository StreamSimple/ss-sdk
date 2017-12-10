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
import java.util.Queue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.streamsimple.guava.common.base.Preconditions;
import com.streamsimple.guava.common.collect.Lists;
import com.streamsimple.javautil.backoff.BackoffWaiter;
import com.streamsimple.javautil.backoff.FibonacciBackoffCalculator;
import com.streamsimple.javautil.serde.Deserializer;

public class SyncKafkaSubscriber<T> implements SyncSubscriber<T>
{
  private final KafkaConsumer<byte[], byte[]> consumer;
  private final BackoffWaiter backoffWaiter;
  private final Deserializer<T> deserializer;

  private boolean closed = false;
  private Queue<T> data = Lists.newLinkedList();

  protected SyncKafkaSubscriber(final KafkaProtocol.Subscriber protocol, final Deserializer<T> deserializer)
  {
    final FibonacciBackoffCalculator calculator =
        new FibonacciBackoffCalculator.Builder().build(protocol.getMaxBackoffTime());

    this.backoffWaiter = new BackoffWaiter(calculator);
    this.deserializer = Preconditions.checkNotNull(deserializer);

    this.consumer = new KafkaConsumer<>(protocol.getProperties());
    consumer.subscribe(Lists.newArrayList(protocol.getTopic()));

    // Force the consumer to fetch offsets, since this appears to be done lazily
    final ConsumerRecords<byte[], byte[]> records = consumer.poll(0L);
    addRecords(records);
  }

  @Override
  public T next() throws IOException
  {
    if (!data.isEmpty()) {
      return data.poll();
    }

    while (true) {
      final ConsumerRecords<byte[], byte[]> records = consumer.poll(0L);

      if (records.isEmpty()) {
        backoffWaiter.sleepUninterruptibly();
        continue;
      }

      addRecords(records);
      break;
    }

    return data.poll();
  }

  private void addRecords(ConsumerRecords<byte[], byte[]> records)
  {
    for (TopicPartition partition : records.partitions()) {
      for (ConsumerRecord<byte[], byte[]> record : records.records(partition)) {
        final T value = deserializer.deserialize(record.value());
        data.add(value);
      }
    }
  }

  @Override
  public boolean isClosed()
  {
    return closed;
  }

  @Override
  public void close()
  {
    consumer.close();
    closed = true;
  }
}
