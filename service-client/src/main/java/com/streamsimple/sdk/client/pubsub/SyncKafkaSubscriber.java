package com.streamsimple.sdk.client.pubsub;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import java.util.Properties;
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
