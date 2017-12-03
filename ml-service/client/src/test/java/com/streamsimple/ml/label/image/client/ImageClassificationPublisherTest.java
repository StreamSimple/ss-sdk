package com.streamsimple.ml.label.image.client;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import com.google.common.collect.Lists;
import com.streamsimple.kafka.testutils.KafkaClusterTestWatcher;
import com.streamsimple.sdk.client.pubsub.KafkaProtocol;
import com.streamsimple.sdk.ml.data.ImageClassificationRequestOuterClass.ImageClassificationRequest;
import java.util.Iterator;
import java.util.Properties;

public class ImageClassificationPublisherTest
{
  @Rule
  public final KafkaClusterTestWatcher kafkaTestWatcher = new KafkaClusterTestWatcher.Builder().build();

  @Test
  public void simplePublisherTest() throws Exception
  {
    final String topic = "imageTopic1";

    kafkaTestWatcher.createTopic(topic, 1);

    final KafkaProtocol.Publisher publisherProtocol =
        new KafkaProtocol.Publisher.Builder()
            .addBootstrapEndpoints(kafkaTestWatcher.getBootstrapEndpoints())
            .setTopic(topic)
            .build();

    final ImageClassificationPublisher publisher = new ImageClassificationPublisher.Builder().build(publisherProtocol);

    final byte[] expectedImage = new byte[]{1, 2, 3};

    publisher.pub(expectedImage);
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

    ImageClassificationRequest request = ImageClassificationRequest.parseFrom(record.value());
    Assert.assertArrayEquals(expectedImage, request.getImage().toByteArray());
  }
}
