package com.streamsimple.sdk.ml.data;

import com.google.protobuf.InvalidProtocolBufferException;
import com.simplifi.it.javautil.serde.Deserializer;

public class TopNLabelDeserializer implements Deserializer<TopNLabelOuterClass.TopNLabel>
{
  @Override
  public TopNLabelOuterClass.TopNLabel deserialize(byte[] bytes)
  {
    try {
      return TopNLabelOuterClass.TopNLabel.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }
}
