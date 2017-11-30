package com.streamsimple.sdk.client.pubsub;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import com.simplifi.it.javautil.serde.StringDeserializer;
import com.streamsimple.kafka.testutils.KafkaClusterTestWatcher;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SyncKafkaSubscriberTest
{
  @Rule
  public final KafkaClusterTestWatcher kafkaTestWatcher = new KafkaClusterTestWatcher.Builder().build();

  @Test
  public void simpleSubscriberTest() throws ExecutionException, InterruptedException, IOException
  {
    final String testValue = "testValue";
    final String topicName = "testTopic";
    final String consumerGroup = "testGroup";

    kafkaTestWatcher.createTopic(topicName, 1);

    final StringDeserializer deserializer = new StringDeserializer.Builder()
        .setCharsetName(StandardCharsets.UTF_8.name())
        .build();

    final KafkaProtocol.Subscriber subscriberProtocol = new KafkaProtocol.Subscriber.Builder()
        .addBootstrapEndpoints(kafkaTestWatcher.getBootstrapEndpoints())
        .setTopic(topicName)
        .build();

    final SyncSubscriber<String> subscriber = new SyncSubscriberFactoryImpl<String>()
        .create(consumerGroup, subscriberProtocol, deserializer);

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
    Assert.assertEquals(testValue, result);
  }
}
