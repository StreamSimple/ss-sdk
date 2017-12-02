package com.streamsimple.sdk.client.pubsub;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import com.google.common.collect.Lists;
import com.simplifi.it.javautil.serde.StringDeserializer;
import com.simplifi.it.javautil.serde.StringSerializer;
import com.streamsimple.kafka.testutils.KafkaClusterTestWatcher;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Properties;

public class KafkaPublisherTest
{
  @Rule
  public final KafkaClusterTestWatcher kafkaTestWatcher = new KafkaClusterTestWatcher.Builder().build();

  @Test
  public void test() throws Exception
  {
    final String topic = "testTopic";

    final StringSerializer serializer = new StringSerializer.Builder()
        .setCharsetName(StandardCharsets.UTF_8.name())
        .build();

    final StringDeserializer deserializer = new StringDeserializer.Builder()
        .setCharsetName(StandardCharsets.UTF_8.name())
        .build();

    kafkaTestWatcher.createTopic(topic, 1);

    final com.streamsimple.sdk.client.pubsub.KafkaProtocol.Publisher publisherProtocol =
        new com.streamsimple.sdk.client.pubsub.KafkaProtocol.Publisher.Builder()
        .addBootstrapEndpoints(kafkaTestWatcher.getBootstrapEndpoints())
        .setTopic(topic)
        .build();

    final com.streamsimple.sdk.client.pubsub.Publisher<String> publisher = new PublisherFactoryImpl<String>()
        .create(publisherProtocol, serializer);

    final String expected = "My data";
    publisher.pub(expected);
    publisher.close();

    final Properties subProps = new Properties();
    subProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaTestWatcher.getBootstrapEndpointsProp());
    subProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
    subProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
    subProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
    subProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    subProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());

    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(subProps);
    consumer.subscribe(Lists.newArrayList(topic));

    final ConsumerRecords<byte[], byte[]> records;

    try {
      records = consumer.poll(30000L);
    } finally {
      consumer.close();
    }

    Assert.assertEquals(1, records.count());

    Iterator<ConsumerRecord<byte[], byte[]>> recordIterator = records.iterator();
    ConsumerRecord<byte[], byte[]> record = recordIterator.next();

    Assert.assertEquals(expected, deserializer.deserialize(record.value()));
  }
}
