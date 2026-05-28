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
package org.apache.iotdb.streamnode.engine.scheduler.task;

import org.apache.iotdb.calc.execution.schedule.queue.ID;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;
import org.apache.iotdb.streamnode.engine.window.IStreamWindow;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.apache.tsfile.read.common.block.TsBlock;

public class StreamDriver implements IStreamDriver {

  private DriverTaskId driverTaskId;

  IStreamWindow streamWindow;

  PartitionKey partitionKey;

  private Runnable wakeUpCallback;

  private final StreamSubTask streamSubTask;

  private boolean isBlocked = false;

  public StreamDriver(StreamSubTask streamSubTask) {
    this.streamSubTask = streamSubTask;
    this.partitionKey = streamSubTask.getPartitionKey();
    driverTaskId = new DriverTaskId(streamSubTask.getStreamName(), streamSubTask.getPartitionKey());
  }

  @Override
  public boolean isFinished() {
    return isBlocked;
  }

  /**
   * 任务执行类，循环
   *
   * @param duration
   * @return
   */
  @Override
  public ListenableFuture<?> processFor(Duration duration) {
    // TODO: add compute method
    try {
      Thread.sleep(20);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    SettableFuture<Void> future = SettableFuture.create();
    return future;
  }

  @Override
  public ListenableFuture<?> push(TsBlock tsBlock, long commitId) {
    ListenableFuture<?> future = addData(partitionKey, tsBlock);

    if (wakeUpCallback != null) {
      // notify scheduler：I hava data
      wakeUpCallback.run();
    }
    return future;
  }

  ListenableFuture<?> addData(PartitionKey key, TsBlock tsBlock) {
    return streamWindow.push(key, tsBlock);
  }

  @Override
  public void close() {}

  @Override
  public void failed(Throwable t) {}

  @Override
  public DriverTaskId getDriverTaskId() {
    return driverTaskId;
  }

  @Override
  public void setDriverTaskId(ID driverTaskId) {
    this.driverTaskId = (DriverTaskId) driverTaskId;
  }
}
