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

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.streamsimple.javautil.serde.StringDeserializer;
import com.streamsimple.kafka.testutils.KafkaClusterTestWatcher;

public class SyncKafkaSubscriberTest
{
  @Rule
  public final KafkaClusterTestWatcher kafkaTestWatcher = new KafkaClusterTestWatcher.Builder().build();

  @Test
  public void simpleSubscriberTest() throws Exception
  {
    final String testValue = "testValue";
    final String topicName = "testTopic1";
    final String consumerGroup = "testGroup";

    kafkaTestWatcher.createTopic(topicName, 1);

    final StringDeserializer deserializer = new StringDeserializer.Builder()
        .setCharsetName(StandardCharsets.UTF_8.name())
        .build();

    final KafkaProtocol.Subscriber subscriberProtocol = new KafkaProtocol.Subscriber.Builder()
        .addBootstrapEndpoints(kafkaTestWatcher.getBootstrapEndpoints())
        .setTopic(topicName)
        .build(consumerGroup);

    final SyncSubscriber<String> subscriber = new SyncSubscriberFactoryImpl<String>()
        .create(subscriberProtocol, deserializer);

    final Properties prodProps = new Properties();
    prodProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
    prodProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
    prodProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaTestWatcher.getBootstrapEndpointsProp());

    final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prodProps);
    final Future<RecordMetadata> prodFuture;

    try {
      prodFuture = producer.send(new ProducerRecord<>(topicName, testValue));
      producer.flush();
      prodFuture.get();
    } finally {
      producer.close();
    }

    final String result = subscriber.next();

    try {
      Assert.assertEquals(testValue, result);
    } finally {
      subscriber.close();
    }
  }
}
