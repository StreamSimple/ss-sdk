package com.streamsimple.sdk.client.pubsub;

import java.util.Properties;

public class FaultTolerantKafkaPublisher
{
  public Properties createProperties()
  {
    Properties props = new Properties();

    // These properties are required to be set to enable exactly once delivery of a message even in the event of
    // broker failure or partition rebalancing https://kafka.apache.org/documentation/
    // TODO upgrade to 0.10.X or greater to enabble exactly once
    // props.setProperty("enable.idempotence", "true");
    props.setProperty("max.in.flight.requests.per.connection", "5");
    props.setProperty("retries", "5");
    props.setProperty("acks", "all");

    return props;
  }
}
