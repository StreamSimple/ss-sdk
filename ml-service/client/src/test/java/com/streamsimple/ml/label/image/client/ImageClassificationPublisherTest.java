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

import java.util.Iterator;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import com.streamsimple.guava.common.collect.Lists;
import com.streamsimple.kafka.testutils.KafkaClusterTestWatcher;
import com.streamsimple.sdk.client.pubsub.KafkaProtocol;

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

    Assert.assertArrayEquals(expectedImage, record.value());
  }
}
