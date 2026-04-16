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

import org.apache.tsfile.write.record.Tablet;

import java.util.Objects;

/**
 * A {@link PartitionKey} that reads segment values lazily from a specific row of a {@link Tablet}.
 *
 * <p>Instead of copying column values up front, this key holds a reference to the tablet, the row
 * index, and the pre-resolved column indices ({@code colIndicesInTablet}). Each call to
 * {@link #segmentValue(int)} reads directly from {@code tablet.getValues()[colIndicesInTablet[i]][row]}.
 *
 * <p>The number of segments equals the length of {@code colIndicesInTablet}. A column index of
 * {@code -1} means the partition column is absent from this tablet; {@link #segmentValue(int)}
 * returns {@code null} for such segments.
 */
public class TabletPositionPartitionKey implements PartitionKey {

  private final Tablet tablet;
  private final int row;
  private final int[] colIndicesInTablet;

  public TabletPositionPartitionKey(
      final Tablet tablet, final int row, final int[] colIndicesInTablet) {
    this.tablet = tablet;
    this.row = row;
    this.colIndicesInTablet = colIndicesInTablet;
  }

  @Override
  public int segmentNum() {
    return colIndicesInTablet.length;
  }

  @Override
  public Object segmentValue(final int segmentIndex) {
    final int colIdx = colIndicesInTablet[segmentIndex];
    if (colIdx < 0) {
      return null;
    }
    return tablet.getValue(row, colIdx);
  }

  @Override
  public int partitionHash() {
    int result = 1;
    for (int i = 0; i < colIndicesInTablet.length; i++) {
      result = 31 * result + Objects.hashCode(segmentValue(i));
    }
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionKey)) {
      return false;
    }
    final PartitionKey other = (PartitionKey) o;
    return PartitionKey.super.equals(other);
  }

  @Override
  public int hashCode() {
    return partitionHash();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("TabletPositionPartitionKey{row=");
    sb.append(row).append(", values=[");
    for (int i = 0; i < colIndicesInTablet.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(segmentValue(i));
    }
    sb.append("]}");
    return sb.toString();
  }
}
