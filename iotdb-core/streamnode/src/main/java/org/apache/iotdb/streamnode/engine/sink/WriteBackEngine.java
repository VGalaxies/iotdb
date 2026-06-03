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

import org.apache.iotdb.commons.stream.IoTDBTarget;
import org.apache.iotdb.commons.stream.StreamTarget;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteBackEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteBackEngine.class);

  private final GlobalSinkMemoryController globalMemoryController =
      GlobalSinkMemoryController.getInstance();
  private final GlobalSinkWriterPool writerPool;
  private final StreamTask taskDefinition;
  private final SinkPipelineConfig defaultConfig = new SinkPipelineConfig();
  private SinkPipeline pipeline;
  private IoTDBTarget iotdbTarget;

  public WriteBackEngine(StreamTask taskDefinition) {
    this.taskDefinition = taskDefinition;
    this.writerPool = GlobalSinkWriterPool.getInstance(defaultConfig);
  }

  public void start() throws Exception {
    writerPool.start();
    StreamTarget target = taskDefinition.getTarget();
    if (target instanceof IoTDBTarget) {
      IoTDBTarget iotdbTarget;
      if (pipeline == null) {
        iotdbTarget = (IoTDBTarget) target;
        writerPool.acquireSessionPool(iotdbTarget);
        String pipelineId = taskDefinition.getTaskName() + "." + iotdbTarget.getDatabase();
        // TODO: SourceCommitCallback commitCallback for subscription
        pipeline =
            new SinkPipeline(
                pipelineId,
                iotdbTarget,
                defaultConfig,
                globalMemoryController,
                writerPool,
                commitId -> LOGGER.debug("Committed source commitId: {}", commitId));
      } else {
        iotdbTarget = pipeline.getIotdbTarget();
      }

      LOGGER.info(
          "StreamSinkPipeline created for target: {}.{}",
          iotdbTarget.getDatabase(),
          iotdbTarget.getTableName());
      return;
    }

    throw new UnsupportedOperationException("Unsupported stream sink type: " + target.getType());
  }

  public IStreamSinkTask createSinkSubTask(StreamSubTaskContext subTaskContext) {
    if (pipeline == null) {
      throw new IllegalStateException("WriteBackEngine is not started");
    }
    return pipeline.createSinkTask(subTaskContext);
  }

  public void stop() {
    if (pipeline != null) {
      writerPool.stop(pipeline);
      pipeline.stop();
    }

    LOGGER.info("StreamSinkEngine all sink tasks are stopped");
  }

  public GlobalSinkMemoryController getGlobalMemoryController() {
    return globalMemoryController;
  }

  public GlobalSinkWriterPool getWriterPool() {
    return writerPool;
  }

  public StreamTask getTaskDefinition() {
    return taskDefinition;
  }

  public SinkPipelineConfig getDefaultConfig() {
    return defaultConfig;
  }

  public SinkPipeline getPipeline() {
    return pipeline;
  }
}
