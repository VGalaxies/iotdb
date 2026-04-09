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

  private String internalAddress = "127.0.0.1";
  private int internalPort = 10820;
  private int rpcPort = 10830;
  private String configNodeAddress = "127.0.0.1";
  private int configNodePort = 10710;
  private int executorThreadNum = 4;

  public String getInternalAddress() {
    return internalAddress;
  }

  public void setInternalAddress(String internalAddress) {
    this.internalAddress = internalAddress;
  }

  public int getInternalPort() {
    return internalPort;
  }

  public void setInternalPort(int internalPort) {
    this.internalPort = internalPort;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public void setRpcPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  public String getConfigNodeAddress() {
    return configNodeAddress;
  }

  public void setConfigNodeAddress(String configNodeAddress) {
    this.configNodeAddress = configNodeAddress;
  }

  public int getConfigNodePort() {
    return configNodePort;
  }

  public void setConfigNodePort(int configNodePort) {
    this.configNodePort = configNodePort;
  }

  public int getExecutorThreadNum() {
    return executorThreadNum;
  }

  public void setExecutorThreadNum(int executorThreadNum) {
    this.executorThreadNum = executorThreadNum;
  }
}
