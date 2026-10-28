/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.streamnode.engine.sink;

import org.apache.tsfile.write.record.Tablet;

import java.util.List;

public class FlushBatch {
  private final Tablet tablet;
  private final List<SinkEntry> entries;
  private final long memoryToRelease;
  private final long batchLingerNanos;

  public FlushBatch(
      Tablet tablet, List<SinkEntry> entries, long memoryToRelease, long batchLingerNanos) {
    this.tablet = tablet;
    this.entries = entries;
    this.memoryToRelease = memoryToRelease;
    this.batchLingerNanos = batchLingerNanos;
  }

  public Tablet getTablet() {
    return tablet;
  }

  public List<SinkEntry> getEntries() {
    return entries;
  }

  public long getMemoryToRelease() {
    return memoryToRelease;
  }

  public long getBatchLingerNanos() {
    return batchLingerNanos;
  }
}
