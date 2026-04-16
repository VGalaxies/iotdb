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

package org.apache.iotdb.streamnode.engine.task;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.StreamWindow;
import org.apache.iotdb.streamnode.engine.window.WindowEngine;
import org.apache.iotdb.streamnode.engine.window.WindowEvent;

import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class StreamSubTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamSubTask.class);

  private final PartitionKey partitionKey;
  private final WindowEngine windowEngine;
  private final AtomicLong lastCommitId = new AtomicLong(-1);

  public StreamSubTask(PartitionKey partitionKey, StreamWindow window) {
    this.partitionKey = partitionKey;
    this.windowEngine = WindowEngine.create(window);
  }

  public List<WindowEvent> offer(Object data, int startRow, int endRow, long dataId) {
    List<WindowEvent> events = windowEngine.process(data, startRow, endRow);
    lastCommitId.set(dataId);
    return events;
  }

  public Future<Void> offer(DataSlice dataSlice) {
    // TODO: Implement asynchronous processing of data slice and return a Future that completes when processing is done
    return CompletableFuture.completedFuture(null);
  }

  public long getCommitId() {
    return lastCommitId.get();
  }

  public PartitionKey getPartitionKey() {
    return partitionKey;
  }

  public static class DataSlice {
    private Tablet tablet;
    private int startRow;
    private int endRow;
    private long tabletId;
  }
}
