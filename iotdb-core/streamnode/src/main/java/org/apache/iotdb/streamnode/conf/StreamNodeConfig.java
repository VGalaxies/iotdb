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

public class StreamNodeConfig {

  /** Cluster name this StreamNode belongs to */
  private String clusterName = "defaultCluster";

  /** Internal RPC address that StreamNode binds to */
  private String snInternalAddress = "127.0.0.1";

  /** Internal RPC port for CN->SN communication */
  private int snInternalPort = 10820;

  /** Seed ConfigNode endpoint (address:port) for registration */
  private String snSeedConfigNode = "127.0.0.1:10710";

  /** Max concurrent client connections for RPC */
  private int rpcMaxConcurrentClientNum = 1000;

  /** Thrift server await time for stop (ms) */
  private int thriftServerAwaitTimeForStopService = 60;

  /** Whether to enable Thrift compression */
  private boolean rpcThriftCompressionEnable = false;

  /** Thread pool size for stream task execution */
  private int executorThreadNum = 4;

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

  public String getSnSeedConfigNode() {
    return snSeedConfigNode;
  }

  public void setSnSeedConfigNode(String snSeedConfigNode) {
    this.snSeedConfigNode = snSeedConfigNode;
  }

  /** Parse the seed config node address from snSeedConfigNode (ip:port) */
  public String getSeedConfigNodeAddress() {
    return snSeedConfigNode.split(":")[0];
  }

  /** Parse the seed config node port from snSeedConfigNode (ip:port) */
  public int getSeedConfigNodePort() {
    return Integer.parseInt(snSeedConfigNode.split(":")[1]);
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
}
