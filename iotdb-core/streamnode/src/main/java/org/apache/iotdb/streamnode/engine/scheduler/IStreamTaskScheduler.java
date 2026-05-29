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
package org.apache.iotdb.streamnode.engine.scheduler;

import org.apache.iotdb.streamnode.engine.scheduler.task.DriverTaskId;
import org.apache.iotdb.streamnode.engine.scheduler.task.IStreamDriver;

public interface IStreamTaskScheduler {
  /**
   * Submit stream tasks to the scheduler.
   *
   * @param driver stream driver to be submitted.
   * @param timeoutMs timeout for the submitted tasks.
   */
  void submitStreamDriver(IStreamDriver driver, long timeoutMs);

  /**
   * Cancel stream tasks.
   *
   * @param id task id
   */
  void cancelStreamTask(DriverTaskId id);

  void cancelStreamTask(String streamName);

  void start();

  void stop();

  int getTaskCount();

  int getRunningTaskCount();
}
