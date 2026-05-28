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

package org.apache.iotdb.streamnode.service;

import org.apache.iotdb.common.rpc.thrift.TLoadSample;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.SystemMetric;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.streamnode.client.ConfigNodeInfo;
import org.apache.iotdb.streamnode.manager.StreamTaskManager;
import org.apache.iotdb.streamnode.rpc.thrift.IStreamNodeRPCService;
import org.apache.iotdb.streamnode.rpc.thrift.TCreateTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TDropTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStartTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStopTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatResp;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StreamNodeRPCServiceProcessor implements IStreamNodeRPCService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamNodeRPCServiceProcessor.class);

  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  private static final String SYSTEM = "system";

  @Override
  public TStreamNodeHeartbeatResp getHeartbeat(TStreamNodeHeartbeatReq req) throws TException {
    TStreamNodeHeartbeatResp resp = new TStreamNodeHeartbeatResp();
    // Sampling load if necessary
    if (req.isNeedSamplingLoad()) {
      TLoadSample loadSample = new TLoadSample();
      // Sample cpu load
      double cpuLoad =
          MetricService.getInstance()
              .getAutoGauge(
                  SystemMetric.SYS_CPU_LOAD.toString(),
                  MetricLevel.CORE,
                  Tag.NAME.toString(),
                  SYSTEM)
              .getValue();
      if (cpuLoad != 0) {
        loadSample.setCpuUsageRate(cpuLoad);
      }

      // Sample memory load
      double usedMemory = getMemory("jvm.memory.used.bytes");
      double maxMemory = getMemory("jvm.memory.max.bytes");
      if (usedMemory != 0 && maxMemory != 0) {
        loadSample.setMemoryUsageRate(usedMemory * 100 / maxMemory);
      }

      // Sample disk load
      sampleDiskLoad(loadSample);

      resp.setLoadSample(loadSample);
    }

    resp.setHeartbeatTimestamp(req.getHeartbeatTimestamp());
    resp.setStatus(commonConfig.getNodeStatus().getStatus());
    if (commonConfig.getStatusReason() != null) {
      resp.setStatusReason(commonConfig.getStatusReason());
    }

    if (req.isSetConfigNodeEndPoints()) {
      if (ConfigNodeInfo.getInstance()
          .updateConfigNodeList(new ArrayList<>(req.getConfigNodeEndPoints()))) {
        resp.setConfirmedConfigNodeEndPoints(req.getConfigNodeEndPoints());
      }
    }
    return resp;
  }

  @Override
  public TSStatus createTask(TCreateTaskOnStreamNodeReq req) throws TException {
    LOGGER.info("Received createTask request, epoch={}", req.getEpoch());
    try {
      // TODO:  deserialize StreamTask from TStartTaskOnStreamNodeReq
      StreamTask task = StreamTask.deserialize(null);
      StreamTaskManager.getInstance().create(task);
    } catch (IOException e) {
      LOGGER.error("Failed to deserialize StreamTask", e);
      throw new TException(e);
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus startTask(TStartTaskOnStreamNodeReq req) throws TException {
    LOGGER.info("Received startTask request: {}, epoch={}", req.getTaskName(), req.getEpoch());
    try {
      // TODO:  deserialize StreamTask from TStartTaskOnStreamNodeReq
      StreamTask task = StreamTask.deserialize(null);
      StreamTaskManager.getInstance().start(task);
    } catch (IOException e) {
      LOGGER.error("Failed to deserialize StreamTask", e);
      throw new TException(e);
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus stopTask(TStopTaskOnStreamNodeReq req) throws TException {
    LOGGER.info("Received stopTask request: {}", req.getTaskName());
    StreamTaskManager.getInstance().stop(req.getTaskName());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus dropTask(TDropTaskOnStreamNodeReq req) throws TException {
    LOGGER.info("Received dropTask request: {}", req.getTaskName());
    StreamTaskManager.getInstance().drop(req.getTaskName());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus dropAllTasks() throws TException {
    LOGGER.info("Received dropAllTasks request");
    StreamTaskManager.getInstance().dropAll();
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private double getMemory(String gaugeName) {
    double result = 0d;
    try {
      //
      List<String> heapIds = Arrays.asList("PS Eden Space", "PS Old Eden", "Ps Survivor Space");
      List<String> noHeapIds = Arrays.asList("Code Cache", "Compressed Class Space", "Metaspace");

      for (String id : heapIds) {
        AutoGauge gauge =
            MetricService.getInstance()
                .getAutoGauge(gaugeName, MetricLevel.IMPORTANT, "id", id, "area", "heap");
        result += gauge.getValue();
      }
      for (String id : noHeapIds) {
        AutoGauge gauge =
            MetricService.getInstance()
                .getAutoGauge(gaugeName, MetricLevel.IMPORTANT, "id", id, "area", "noheap");
        result += gauge.getValue();
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to get memory from metric because: ", e);
      return 0d;
    }
    return result;
  }

  private void sampleDiskLoad(TLoadSample loadSample) {
    double availableDisk =
        MetricService.getInstance()
            .getAutoGauge(
                SystemMetric.SYS_DISK_AVAILABLE_SPACE.toString(),
                MetricLevel.CORE,
                Tag.NAME.toString(),
                SYSTEM)
            .getValue();
    double totalDisk =
        MetricService.getInstance()
            .getAutoGauge(
                SystemMetric.SYS_DISK_TOTAL_SPACE.toString(),
                MetricLevel.CORE,
                Tag.NAME.toString(),
                SYSTEM)
            .getValue();

    if (availableDisk != 0 && totalDisk != 0) {
      double freeDiskRatio = availableDisk / totalDisk;
      loadSample.setFreeDiskSpace(availableDisk);
      loadSample.setDiskUsageRate(1d - freeDiskRatio);
      // Reset NodeStatus if necessary
      if (freeDiskRatio < commonConfig.getDiskSpaceWarningThreshold()) {
        LOGGER.warn(
            "The available disk space is : {}, "
                + "the total disk space is : {}, "
                + "and the remaining disk usage ratio: {} is "
                + "less than disk_space_warning_threshold: {}, set system to readonly!",
            RamUsageEstimator.humanReadableUnits((long) availableDisk),
            RamUsageEstimator.humanReadableUnits((long) totalDisk),
            freeDiskRatio,
            commonConfig.getDiskSpaceWarningThreshold());
        commonConfig.setNodeStatus(NodeStatus.ReadOnly);
        commonConfig.setStatusReason(NodeStatus.DISK_FULL);
      } else if (NodeStatus.ReadOnly.equals(commonConfig.getNodeStatus())
          && NodeStatus.DISK_FULL.equals(commonConfig.getStatusReason())) {
        commonConfig.setNodeStatus(NodeStatus.Running);
        commonConfig.setStatusReason(null);
      }
    }
  }
}
