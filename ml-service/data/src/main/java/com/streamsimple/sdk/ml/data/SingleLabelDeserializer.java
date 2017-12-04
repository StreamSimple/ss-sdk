package com.streamsimple.sdk.ml.data;

import com.google.protobuf.InvalidProtocolBufferException;
import com.streamsimple.javautil.serde.Deserializer;

public class SingleLabelDeserializer implements Deserializer<SingleLabelOuterClass.SingleLabel>
{
  @Override
  public SingleLabelOuterClass.SingleLabel deserialize(byte[] bytes)
  {
    try {
      return ClassifierResponseOuterClass.ClassifierResponse.parseFrom(bytes).getSingleLabel();
    } catch (InvalidProtocolBufferException e) {
      // Something went wrong
      return null;
    }
  }
}
