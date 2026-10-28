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

package org.apache.iotdb.streamnode.service.metrics;

import org.apache.iotdb.commons.client.ClientManagerMetrics;
import org.apache.iotdb.commons.concurrent.ThreadModule;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.ThreadPoolMetrics;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.service.metric.JvmGcMonitorMetrics;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.ProcessMetrics;
import org.apache.iotdb.commons.service.metric.cpu.CpuUsageMetrics;
import org.apache.iotdb.metrics.metricsets.UpTimeMetrics;
import org.apache.iotdb.metrics.metricsets.disk.DiskMetrics;
import org.apache.iotdb.metrics.metricsets.jvm.JvmMetrics;
import org.apache.iotdb.metrics.metricsets.logback.LogbackMetrics;
import org.apache.iotdb.metrics.metricsets.net.NetMetrics;
import org.apache.iotdb.metrics.metricsets.system.SystemMetrics;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.metrics.SinkGlobalMetrics;
import org.apache.iotdb.streamnode.metrics.StreamComputeMetrics;
import org.apache.iotdb.streamnode.metrics.StreamNodeMemoryMetrics;
import org.apache.iotdb.streamnode.metrics.StreamSchedulerMetrics;
import org.apache.iotdb.streamnode.metrics.StreamTaskManagerMetrics;
import org.apache.iotdb.streamnode.metrics.StreamWindowEngineMetrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StreamNodeMetricsHelper {
  /** Bind predefined metric sets into StreamNode. */
  public static void bind() {
    MetricService metricService = MetricService.getInstance();
    metricService.addMetricSet(new UpTimeMetrics());
    metricService.addMetricSet(new JvmMetrics());
    metricService.addMetricSet(new LogbackMetrics());
    metricService.addMetricSet(new ProcessMetrics());
    metricService.addMetricSet(new DiskMetrics(IoTDBConstant.SN_ROLE));
    metricService.addMetricSet(new NetMetrics(IoTDBConstant.SN_ROLE));
    metricService.addMetricSet(ClientManagerMetrics.getInstance());
    metricService.addMetricSet(ThreadPoolMetrics.getInstance());
    initCpuMetrics(metricService);
    initSystemMetrics(metricService);

    // bind stream related metrics
    metricService.addMetricSet(StreamTaskManagerMetrics.getInstance());
    metricService.addMetricSet(StreamSchedulerMetrics.getInstance());
    metricService.addMetricSet(StreamComputeMetrics.getInstance());
    metricService.addMetricSet(StreamNodeMemoryMetrics.getInstance());
    metricService.addMetricSet(SinkGlobalMetrics.getInstance());
    metricService.addMetricSet(StreamWindowEngineMetrics.getInstance());

    // bind gc metrics
    metricService.addMetricSet(JvmGcMonitorMetrics.getInstance());
  }

  private static void initSystemMetrics(MetricService metricService) {
    ArrayList<String> diskDirs = new ArrayList<>();
    StreamNodeConfig config = StreamNodeDescriptor.getInstance().getConfig();
    diskDirs.add(config.getSystemDir());
    diskDirs.add(config.getSortTmpDir());
    SystemMetrics.getInstance().setDiskDirs(diskDirs);
    metricService.addMetricSet(SystemMetrics.getInstance());
  }

  private static void initCpuMetrics(MetricService metricService) {
    List<String> threadModules = new ArrayList<>();
    Arrays.stream(ThreadModule.values()).forEach(x -> threadModules.add(x.toString()));
    List<String> pools = new ArrayList<>();
    Arrays.stream(ThreadName.values()).forEach(x -> pools.add(x.name()));
    metricService.addMetricSet(
        new CpuUsageMetrics(
            threadModules,
            pools,
            x -> ThreadName.getModuleTheThreadBelongs(x).toString(),
            x -> ThreadName.getThreadPoolTheThreadBelongs(x).name()));
  }
}
