package com.streamsimple.sdk.client.pubsub;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import com.google.common.base.Preconditions;
import com.simplifi.it.javautil.serde.Serializer;
import java.io.IOException;
import java.util.Properties;

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
    final Properties props = createProperties(strategy, protocol);
    this.producer = new KafkaProducer<>(props);
    this.topic = protocol.getTopic();
    this.serializer = Preconditions.checkNotNull(serializer);
  }

  @Override
  public void pub(T tuple) throws IOException
  {
    byte[] tupleBytes = serializer.serialize(tuple);
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, tupleBytes);
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

  public static Properties createProperties(final PublisherFactoryImpl.OptimizationStrategy strategy,
                                            final KafkaProtocol.Publisher protocol)
  {
    Properties props = new Properties();
    props.setProperty("linger.ms", "0");
    props.setProperty("bootstrap.servers", protocol.getBootstrapEndpointsProp());
    props.setProperty("key.serializer", ByteArraySerializer.class.getCanonicalName());
    props.setProperty("value.serializer", ByteArraySerializer.class.getCanonicalName());
    props.putAll(strategy.getProps());
    return props;
  }
}
