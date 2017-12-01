package com.streamsimple.sdk.client.id;

import org.junit.Assert;
import org.junit.Test;

public class IdGeneratorTest
{
  @Test
  public void simpleIdGenTest()
  {
    final IdGenerator generator = new IdGenerator(0x0102030405060708L);
    final Id id1 = generator.nextId();
    final Id id2 = generator.nextId();

    final byte[] expected1 = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    final byte[] expected2 = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};

    Assert.assertArrayEquals(expected1, id1.getBytes());
    Assert.assertArrayEquals(expected2, id2.getBytes());
  }
}
