package com.streamsimple.sdk.client.pubsub;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.simplifi.it.javautil.backoff.BackoffWaiter;
import com.simplifi.it.javautil.backoff.FibonacciBackoffCalculator;
import com.simplifi.it.javautil.serde.Deserializer;
import java.io.IOException;
import java.util.Queue;

public class SyncKafkaSubscriber<T> implements SyncSubscriber<T>
{
  private final KafkaConsumer<byte[], byte[]> consumer;
  private final BackoffWaiter backoffWaiter;
  private final Deserializer<T> deserializer;

  private boolean closed = false;
  private Queue<T> data = Lists.newLinkedList();

  protected SyncKafkaSubscriber(final KafkaProtocol.Subscriber protocol, final Deserializer<T> deserializer)
  {
    this.consumer = new KafkaConsumer<>(protocol.getProperties());

    final FibonacciBackoffCalculator calculator =
        new FibonacciBackoffCalculator.Builder().build(protocol.getMaxBackoffTime());

    this.backoffWaiter = new BackoffWaiter(calculator);
    this.deserializer = Preconditions.checkNotNull(deserializer);
  }

  @Override
  public T next() throws IOException
  {
    if (!data.isEmpty()) {
      return data.poll();
    }

    while (true) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(0L);

      if (records.isEmpty()) {
        backoffWaiter.sleepUninterruptibly();
        continue;
      }

      for (TopicPartition partition : records.partitions()) {
        for (ConsumerRecord<byte[], byte[]> record : records.records(partition)) {
          final T value = deserializer.deserialize(record.value());
          data.add(value);
        }
      }

      break;
    }

    return data.poll();
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
