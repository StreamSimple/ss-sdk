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
package com.streamsimple.sdk.client.id;

import java.util.Arrays;

public class Id
{
  private final byte[] id;

  public Id(byte[] id)
  {
    this.id = id;
  }

  public byte[] getBytes()
  {
    return id;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Id id1 = (Id)o;

    return Arrays.equals(id, id1.id);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(id);
  }

  @Override
  public String toString()
  {
    return "Id{" +
        "id=" + Arrays.toString(id) +
        '}';
  }
}
