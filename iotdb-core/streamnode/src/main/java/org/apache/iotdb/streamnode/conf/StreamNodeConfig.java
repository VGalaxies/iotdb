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

package org.apache.iotdb.streamnode.conf;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.conf.IoTDBConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class StreamNodeConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamNodeConfig.class);

  private static final String SN_ROLE = "streamnode";
  private static final String SN_DEFAULT_DATA_DIR = "data" + File.separator + SN_ROLE;

  /** Cluster name this StreamNode belongs to */
  private String clusterName = "defaultCluster";

  /** Internal RPC address that StreamNode binds to */
  private String snInternalAddress = "127.0.0.1";

  /** Internal RPC port for CN->SN communication */
  private int snInternalPort = 10820;

  /** Seed ConfigNode endpoint (address:port) for registration */
  private TEndPoint snSeedConfigNode = new TEndPoint("127.0.0.1", 10710);

  /** DataNode node urls for fetching IoTDB data */
  private List<String> snClusterIngressNodeUrls = Collections.singletonList("127.0.0.1:6667");

  /** Username used to fetch IoTDB data */
  private String snClusterIngressUsername = "root";

  /** Password used to fetch IoTDB data */
  private String snClusterIngressPassword = "root";

  /** Concurrency of session scan. Used as session pool size and executor thread number. */
  private int sessionScanConcurrency = Runtime.getRuntime().availableProcessors();

  /** Max concurrent client connections for RPC */
  private int rpcMaxConcurrentClientNum = 1000;

  /** Thrift server await time for stop (ms) */
  private int thriftServerAwaitTimeForStopService = 60;

  /** Whether to enable Thrift compression */
  private boolean rpcThriftCompressionEnable = false;

  /** Thread pool size for stream task execution */
  private int executorThreadNum = 4;

  /**
   * The cluster ID that this DataNode joined in the cluster mode. DataNode will fetch cluster ID
   * from ConfigNode and cache it here when first time use it.
   */
  private String clusterId = "";

  /** System directory, including version file for each database and metadata */
  private String systemDir =
      IoTDBConstant.SN_DEFAULT_DATA_DIR + File.separator + IoTDBConstant.SYSTEM_FOLDER_NAME;

  private String sortTmpDir = SN_DEFAULT_DATA_DIR + File.separator + IoTDBConstant.TMP_FOLDER_NAME;

  /** StreamNode ID assigned by ConfigNode after registration */
  private int streamNodeId = -1;

  /** The maximum number of clients that can be allocated for a node. */
  private int maxClientNumForEachNode =
      ClientPoolProperty.DefaultProperty.MAX_CLIENT_NUM_FOR_EACH_NODE;

  /** The time of stream node waiting for the next retry to join into the cluster */
  private final long joinClusterRetryIntervalMs = TimeUnit.SECONDS.toMillis(1);

  /** Thrift socket and connection timeout between data node and config node. */
  private final int connectionTimeoutInMS = (int) TimeUnit.SECONDS.toMillis(60);

  private int driverTaskExecutorTimeSliceInMS = 4;

  /** Number of tasks executed per thread */
  private int executedTaskCountPerThread = 100;

  public int getDriverTaskExecutorTimeSliceInMS() {
    return driverTaskExecutorTimeSliceInMS;
  }

  public void setDriverTaskExecutorTimeSliceInMS(int driverTaskExecutorTimeSliceInMS) {
    this.driverTaskExecutorTimeSliceInMS = driverTaskExecutorTimeSliceInMS;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getSnInternalAddress() {
    return snInternalAddress;
  }

  public void setSnInternalAddress(String snInternalAddress) {
    this.snInternalAddress = snInternalAddress;
  }

  public int getSnInternalPort() {
    return snInternalPort;
  }

  public void setSnInternalPort(int snInternalPort) {
    this.snInternalPort = snInternalPort;
  }

  public TEndPoint getSnSeedConfigNode() {
    return snSeedConfigNode;
  }

  public void setSnSeedConfigNode(TEndPoint snSeedConfigNode) {
    this.snSeedConfigNode = snSeedConfigNode;
  }

  public List<String> getSnClusterIngressNodeUrls() {
    return snClusterIngressNodeUrls;
  }

  public void setSnClusterIngressNodeUrls(List<String> snClusterIngressNodeUrls) {
    this.snClusterIngressNodeUrls = snClusterIngressNodeUrls;
  }

  public String getSnClusterIngressUsername() {
    return snClusterIngressUsername;
  }

  public void setSnClusterIngressUsername(String snClusterIngressUsername) {
    this.snClusterIngressUsername = snClusterIngressUsername;
  }

  public String getSnClusterIngressPassword() {
    return snClusterIngressPassword;
  }

  public void setSnClusterIngressPassword(String snClusterIngressPassword) {
    this.snClusterIngressPassword = snClusterIngressPassword;
  }

  public int getSessionScanConcurrency() {
    return sessionScanConcurrency;
  }

  public void setSessionScanConcurrency(int sessionScanConcurrency) {
    this.sessionScanConcurrency = sessionScanConcurrency;
  }

  public int getRpcMaxConcurrentClientNum() {
    return rpcMaxConcurrentClientNum;
  }

  public void setRpcMaxConcurrentClientNum(int rpcMaxConcurrentClientNum) {
    this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
  }

  public int getThriftServerAwaitTimeForStopService() {
    return thriftServerAwaitTimeForStopService;
  }

  public void setThriftServerAwaitTimeForStopService(int thriftServerAwaitTimeForStopService) {
    this.thriftServerAwaitTimeForStopService = thriftServerAwaitTimeForStopService;
  }

  public boolean isRpcThriftCompressionEnable() {
    return rpcThriftCompressionEnable;
  }

  public void setRpcThriftCompressionEnable(boolean rpcThriftCompressionEnable) {
    this.rpcThriftCompressionEnable = rpcThriftCompressionEnable;
  }

  public int getExecutorThreadNum() {
    return executorThreadNum;
  }

  public void setExecutorThreadNum(int executorThreadNum) {
    this.executorThreadNum = executorThreadNum;
  }

  public int getStreamNodeId() {
    return streamNodeId;
  }

  public void setStreamNodeId(int streamNodeId) {
    this.streamNodeId = streamNodeId;
  }

  public int getMaxClientNumForEachNode() {
    return maxClientNumForEachNode;
  }

  public void setMaxClientNumForEachNode(int maxClientNumForEachNode) {
    this.maxClientNumForEachNode = maxClientNumForEachNode;
  }

  public String getSystemDir() {
    return systemDir;
  }

  public void setSystemDir(String systemDir) {
    this.systemDir = systemDir;
  }

  public String getSortTmpDir() {
    return sortTmpDir;
  }

  public void setSortTmpDir(String sortTmpDir) {
    this.sortTmpDir = sortTmpDir;
  }

  public String getClusterId() {
    return clusterId;
  }

  public void setClusterId(String clusterId) {
    this.clusterId = clusterId;
  }

  public long getJoinClusterRetryIntervalMs() {
    return joinClusterRetryIntervalMs;
  }

  public void setExecutedTaskCountPerThread(int executedTaskCountPerThread) {
    this.executedTaskCountPerThread = executedTaskCountPerThread;
  }

  public int getConnectionTimeoutInMS() {
    return connectionTimeoutInMS;
  }

  public int getExecutedTaskCountPerThread() {
    return executedTaskCountPerThread;
  }

  public TEndPoint getAddressAndPort() {
    return new TEndPoint(snInternalAddress, snInternalPort);
  }

  public String getConfigMessage() {
    StringBuilder configMessage = new StringBuilder();
    String configContent;
    for (Field configField : StreamNodeConfig.class.getDeclaredFields()) {
      try {
        String configType = configField.getGenericType().getTypeName();
        if (configType.contains(IoTDBConstant.STRING_2D_ARRAY_CLASS_NAME)) {
          String[][] configList = (String[][]) configField.get(this);
          StringBuilder builder = new StringBuilder();
          for (String[] strings : configList) {
            builder.append(Arrays.asList(strings)).append(";");
          }
          configContent = builder.toString();
        } else if (configType.contains(IoTDBConstant.STRING_ARRAY_CLASS_NAME)) {
          String[] configList = (String[]) configField.get(this);
          configContent = Arrays.asList(configList).toString();
        } else {
          configContent = configField.get(this).toString();
        }
        configMessage
            .append("\n\t")
            .append(configField.getName())
            .append("=")
            .append(configContent)
            .append(";");
      } catch (Exception e) {
        LOGGER.warn("Failed to get field {}", configField, e);
      }
    }
    return configMessage.toString();
  }
}
