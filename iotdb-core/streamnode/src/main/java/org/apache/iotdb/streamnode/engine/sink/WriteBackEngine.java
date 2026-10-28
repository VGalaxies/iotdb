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
import org.apache.iotdb.streamnode.client.DataNodeLocationResolver;
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class WriteBackEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteBackEngine.class);

  private final GlobalSinkWriterPool writerPool;
  private final StreamTask taskDefinition;
  private final SinkPipelineConfig defaultConfig = new SinkPipelineConfig();
  private SinkPipeline pipeline;

  public WriteBackEngine(StreamTask taskDefinition) {
    this.taskDefinition = taskDefinition;
    this.writerPool = GlobalSinkWriterPool.getInstance();
  }

  public void start() throws Exception {
    StreamTarget target = taskDefinition.getTarget();
    if (target instanceof IoTDBTarget) {
      IoTDBTarget iotdbTarget;
      if (pipeline == null) {
        iotdbTarget = resolveDataNodeEndpoints((IoTDBTarget) target);
        writerPool.acquireSessionPool(iotdbTarget);
        String pipelineId = taskDefinition.getTaskName() + "." + iotdbTarget.getDatabase();
        pipeline = new SinkPipeline(pipelineId, iotdbTarget, defaultConfig, writerPool);
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

  /**
   * Resolves the writable DataNode endpoints from the cluster at sink-start time, overriding the
   * placeholder nodeUrls baked in at CREATE STREAM time (see {@code TableConfigTaskVisitor}).
   *
   * <p>Falls back to the target's configured nodeUrls when no ConfigNode is reachable (e.g. unit
   * tests without a cluster) or no readable DataNode is reported, so the sink can still start
   * against a statically configured endpoint.
   */
  private IoTDBTarget resolveDataNodeEndpoints(IoTDBTarget target) {
    List<String> urls = DataNodeLocationResolver.getReadableDataNodeUrls();
    if (urls.isEmpty()) {
      LOGGER.warn(
          "No readable DataNode resolved from cluster, falling back to target nodeUrls: {}",
          target.getNodeUrls());
      return target;
    }
    String resolved = String.join(",", urls);
    LOGGER.info(
        "Resolved {} writable DataNode endpoint(s) for target {}.{}: {}",
        urls.size(),
        target.getDatabase(),
        target.getTableName(),
        resolved);
    return target.withNodeUrls(resolved);
  }

  public IStreamSinkTask createSinkSubTask(StreamSubTaskContext subTaskContext) {
    if (pipeline == null) {
      throw new IllegalStateException("WriteBackEngine is not started");
    }
    return pipeline.createSinkTask(subTaskContext);
  }

  public void stop() {
    if (pipeline != null) {
      // Stop the pipeline first: doStop() takes each shard lock, so an in-flight flush settles
      // before the session pool it writes through is released.
      pipeline.stop();
      writerPool.stop(pipeline);
    }

    LOGGER.info("StreamSinkEngine all sink tasks are stopped");
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
