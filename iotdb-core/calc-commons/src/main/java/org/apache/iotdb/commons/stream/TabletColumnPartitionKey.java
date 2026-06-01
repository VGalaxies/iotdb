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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TabletColumnPartitionKey implements PartitionKey {

  private final List<Object> columnValues;

  public TabletColumnPartitionKey(final List<Object> columnValues) {
    this.columnValues =
        columnValues == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(columnValues));
  }

  public List<Object> getColumnValues() {
    return columnValues;
  }

  @Override
  public int partitionHash() {
    return columnValues.hashCode();
  }

  @Override
  public int segmentNum() {
    return columnValues.size();
  }

  @Override
  public Object segmentValue(final int segmentIndex) {
    return columnValues.get(segmentIndex);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionKey)) {
      return false;
    }
    return PartitionKey.super.equals((PartitionKey) o);
  }

  @Override
  public int hashCode() {
    return partitionHash();
  }

  @Override
  public String toString() {
    return "TabletColumnPartitionKey{" + "columnValues=" + columnValues + '}';
  }
}
