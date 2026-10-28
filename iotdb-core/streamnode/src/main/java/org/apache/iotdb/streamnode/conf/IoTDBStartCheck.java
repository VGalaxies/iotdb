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

import org.apache.iotdb.commons.conf.AbstractIoTDBStartCheck;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.disk.utils.DirectoryChecker;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.file.SystemPropertiesHandler;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class IoTDBStartCheck extends AbstractIoTDBStartCheck {

  private static final StreamNodeConfig config = StreamNodeDescriptor.getInstance().getConfig();

  private static final String INTERNAL_ADDRESS = "sn_internal_address";
  private static final String INTERNAL_PORT = "sn_internal_port";

  // Mutable system parameters
  private static final Map<String, Supplier<String>> variableParamValueTable = new HashMap<>();

  static {
    variableParamValueTable.put(
        INTERNAL_ADDRESS, () -> String.valueOf(config.getSnInternalAddress()));
    variableParamValueTable.put(INTERNAL_PORT, () -> String.valueOf(config.getSnInternalPort()));
  }

  private static final String STREAM_NODE_ID = "stream_node_id";

  private IoTDBStartCheck() {
    logger.info(StreamNodeMessages.STARTING_STREAMNODE, IoTDBConstant.VERSION_WITH_BUILD);
  }

  public static IoTDBStartCheck getInstance() {
    return IoTDBConfigCheckHolder.INSTANCE;
  }

  public static void reinitializeStatics() {
    IoTDBConfigCheckHolder.INSTANCE = new IoTDBStartCheck();
  }

  private static class IoTDBConfigCheckHolder {

    private static IoTDBStartCheck INSTANCE = new IoTDBStartCheck();
  }

  @Override
  protected SystemPropertiesHandler getSystemPropertiesHandler() {
    return StreamNodeSystemPropertiesHandler.getInstance();
  }

  @Override
  protected Map<String, Supplier<String>> getVariableParamValueTable() {
    return variableParamValueTable;
  }

  @Override
  protected String getNodeIdKey() {
    return STREAM_NODE_ID;
  }

  @Override
  protected void loadNodeId(int nodeId) {
    config.setStreamNodeId(nodeId);
  }

  @Override
  protected void loadClusterId(String clusterId) {
    config.setClusterId(clusterId);
  }

  /** check and create directory before start Stream Node. */
  @Override
  public void checkDirectory() throws ConfigurationException, IOException {
    // check system dir
    DirectoryChecker.getInstance().registerDirectory(new File(config.getSystemDir()));
  }
}
