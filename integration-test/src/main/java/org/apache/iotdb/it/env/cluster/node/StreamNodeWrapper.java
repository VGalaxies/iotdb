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

package org.apache.iotdb.it.env.cluster.node;

import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.env.cluster.config.MppBaseConfig;
import org.apache.iotdb.it.env.cluster.config.MppDataNodeConfig;
import org.apache.iotdb.it.env.cluster.config.MppJVMConfig;

import org.apache.tsfile.external.commons.io.file.PathUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class StreamNodeWrapper extends AbstractNodeWrapper {
  // StreamNode constants
  public static final String STREAMNODE_INIT_HEAP_SIZE = "StreamNodeInitHeapSize";
  public static final String STREAMNODE_MAX_HEAP_SIZE = "StreamNodeMaxHeapSize";
  public static final String STREAMNODE_MAX_DIRECT_MEMORY_SIZE = "StreamNodeMaxDirectMemorySize";
  public static final String SN_INTERNAL_ADDRESS = "sn_internal_address";
  public static final String SN_INTERNAL_PORT = "sn_internal_port";

  public StreamNodeWrapper(
      final String seedConfigNode,
      final String testClassName,
      final String testMethodName,
      final int[] portList,
      final int clusterIndex,
      final long startTime) {
    this(seedConfigNode, null, testClassName, testMethodName, portList, clusterIndex, startTime);
  }

  public StreamNodeWrapper(
      final String seedConfigNode,
      final String dataNodeEndpoints,
      final String testClassName,
      final String testMethodName,
      final int[] portList,
      final int clusterIndex,
      final long startTime) {
    super(testClassName, testMethodName, portList, clusterIndex, false, startTime);

    // Initialize mutable properties
    reloadMutableFields();

    // Initialize immutable properties
    immutableNodeProperties.setProperty("sn_seed_config_node", seedConfigNode);
    if (dataNodeEndpoints != null && !dataNodeEndpoints.isEmpty()) {
      immutableNodeProperties.setProperty("sn_cluster_ingress_node_urls", dataNodeEndpoints);
    }
    // Set system directory to be under the node working directory, not relative to project root
    immutableNodeProperties.setProperty(
        "sn_system_dir", getNodePath() + File.separator + "data" + File.separator + "streamnode");
  }

  @Override
  protected String getSystemConfigPath() {
    return workDirFilePath("conf", "iotdb-system.properties");
  }

  @Override
  protected String getDefaultNodeConfigPath() {
    return "";
  }

  @Override
  protected String getDefaultCommonConfigPath() {
    return "";
  }

  @Override
  public String getSystemPropertiesPath() {
    return workDirFilePath("conf", "iotdb-system.properties");
  }

  @Override
  protected MppJVMConfig initVMConfig() {
    return MppJVMConfig.builder()
        .setInitHeapSize(EnvUtils.getIntFromSysVar(STREAMNODE_INIT_HEAP_SIZE, 256, clusterIndex))
        .setMaxHeapSize(EnvUtils.getIntFromSysVar(STREAMNODE_MAX_HEAP_SIZE, 256, clusterIndex))
        .setMaxDirectMemorySize(
            EnvUtils.getIntFromSysVar(STREAMNODE_MAX_DIRECT_MEMORY_SIZE, 256, clusterIndex))
        .setTimezone("Asia/Shanghai")
        .build();
  }

  @Override
  public final String getId() {
    return "StreamNode" + getPort();
  }

  @Override
  protected void addStartCmdParams(final List<String> params) {
    final String workDir = getNodePath();
    final String confDir = workDir + File.separator + "conf";
    params.addAll(
        Arrays.asList(
            "-Dlogback.configurationFile=" + confDir + File.separator + "logback-streamnode.xml",
            "-DSTREAMNODE_HOME=" + workDir,
            "-DTSFILE_HOME=" + workDir,
            "-DSTREAMNODE_CONF=" + confDir,
            "-DTSFILE_CONF=" + confDir,
            "org.apache.iotdb.streamnode.StreamNode",
            "-s"));
  }

  @Override
  String getNodeType() {
    return "streamnode";
  }

  @Override
  protected void reloadMutableFields() {
    // Set basic properties
    mutableNodeProperties.setProperty(SN_INTERNAL_ADDRESS, super.getIp());
    mutableNodeProperties.setProperty(SN_INTERNAL_PORT, String.valueOf(super.getPort()));
  }

  @Override
  public void createNodeDir() {
    // First, delete any existing data directory that may contain old system.properties
    // The system.properties file contains config_node_list which may have old ConfigNode port
    // from previous test runs, causing connection failures
    final String dataDir = getNodePath() + File.separator + "data";
    try {
      PathUtils.deleteDirectory(Paths.get(dataDir));
    } catch (NoSuchFileException e) {
      // ignored - data directory may not exist
    } catch (IOException e) {
      // ignored - data directory may not exist or be deletable
    }
    // Then call parent method to copy template
    super.createNodeDir();
  }

  public void setWriteBackRetryForIT() {
    immutableNodeProperties.setProperty("sn_write_back_session_max_retry_count", "10");
    immutableNodeProperties.setProperty("sn_write_back_session_retry_interval_ms", "100");
    immutableNodeProperties.setProperty("sn_write_back_session_connection_timeout_ms", "1000");
    immutableNodeProperties.setProperty("sn_write_back_max_retry_attempts", "5");
    immutableNodeProperties.setProperty("sn_write_back_retry_backoff_base_ms", "200");
  }

  public void changeConfig() {
    try {
      reloadMutableFields();

      MppBaseConfig streamNodeConfig = new MppDataNodeConfig();
      streamNodeConfig.updateProperties(mutableNodeProperties);
      streamNodeConfig.updateProperties(immutableNodeProperties);

      streamNodeConfig.persistent(getSystemConfigPath());
    } catch (IOException ex) {
      throw new AssertionError("Change the config of StreamNode failed. " + ex);
    }
    this.jvmConfig.override(jvmConfig);
  }

  @Override
  public int getMetricPort() {
    // no metric currently
    return -1;
  }

  /* Abstract methods, which must be implemented in ConfigNode and DataNode. */
  public void renameFile() {}
}
