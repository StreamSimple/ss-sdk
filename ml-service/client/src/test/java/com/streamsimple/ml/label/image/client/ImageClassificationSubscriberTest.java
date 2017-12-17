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

import java.util.Properties;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.google.protobuf.ByteString;
import com.streamsimple.kafka.testutils.KafkaClusterTestWatcher;
import com.streamsimple.sdk.client.pubsub.KafkaProtocol;
import com.streamsimple.sdk.ml.data.ClassifierResponseOuterClass.ClassifierResponse;
import com.streamsimple.sdk.ml.data.SingleLabelOuterClass.SingleLabel;

public class ImageClassificationSubscriberTest
{
  @Rule
  public final KafkaClusterTestWatcher kafkaTestWatcher = new KafkaClusterTestWatcher.Builder().build();

  @Test
  public void simpleSubscriberTest() throws Exception
  {
    final String topic = "imageTopic2";
    final String group = "testImageTopic2";

    final SingleLabel label = SingleLabel.newBuilder()
        .setLabel("label")
        .setConfidence(.5f)
        .build();

    final ClassifierResponse response = ClassifierResponse.newBuilder()
        .setType(ClassifierResponse.Type.SINGLE_LABEL)
        .setId(ByteString.copyFrom(new byte[]{1}))
        .setSingleLabel(label)
        .build();

    kafkaTestWatcher.createTopic(topic, 1);

    final KafkaProtocol.Subscriber subscriberProtocol = new KafkaProtocol.Subscriber.Builder()
        .addBootstrapEndpoints(kafkaTestWatcher.getBootstrapEndpoints())
        .setTopic(topic)
        .build(group);

    final ImageClassificationSubscriber<SingleLabel> subscriber =
        new ImageClassificationSubscriber.Builder().buildSingle(subscriberProtocol);

    Properties prodProps = new Properties();
    prodProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    prodProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    prodProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaTestWatcher.getBootstrapEndpointsProp());

    final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(prodProps);
    final Future<RecordMetadata> prodFuture;

    try {
      prodFuture = producer.send(new ProducerRecord<>(topic, response.toByteArray()));
      producer.flush();
      prodFuture.get();
    } finally {
      producer.close();
    }

    final SingleLabel actual = subscriber.next();

    try {
      Assert.assertEquals(label, actual);
    } finally {
      subscriber.close();
    }
  }
}
