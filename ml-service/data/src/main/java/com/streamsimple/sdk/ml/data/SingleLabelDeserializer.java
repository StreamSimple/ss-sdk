package com.streamsimple.sdk.ml.data;

import com.google.protobuf.InvalidProtocolBufferException;
import com.simplifi.it.javautil.serde.Deserializer;

public class SingleLabelDeserializer implements Deserializer<SingleLabelOuterClass.SingleLabel>
{
  @Override
  public SingleLabelOuterClass.SingleLabel deserialize(byte[] bytes)
  {
    try {
      return SingleLabelOuterClass.SingleLabel.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      // Something went wrong
      return null;
    }
  }
}
