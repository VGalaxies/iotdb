/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.stream;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class ColumnPartitionKey implements PartitionKey {

  private final Object[] columnValues;

  public ColumnPartitionKey(Object[] columnValues) {
    this.columnValues = columnValues;
  }

  public Object[] getColumnValues() {
    return columnValues;
  }

  @Override
  public int segmentNum() {
    return columnValues.length;
  }

  @Override
  public Object segmentValue(int segmentIndex) {
    return columnValues[segmentIndex];
  }

  @Override
  public int partitionHash() {
    return columnValues == null ? 0 : Arrays.hashCode(columnValues);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnPartitionKey that = (ColumnPartitionKey) o;
    return Arrays.equals(columnValues, that.columnValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnValues);
  }
}
