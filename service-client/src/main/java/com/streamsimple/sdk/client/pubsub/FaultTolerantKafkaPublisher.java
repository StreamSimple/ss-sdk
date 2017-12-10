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
