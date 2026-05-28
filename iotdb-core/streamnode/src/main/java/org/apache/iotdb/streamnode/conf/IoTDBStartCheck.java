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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.file.SystemPropertiesHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public class IoTDBStartCheck {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBStartCheck.class);

  private static final StreamNodeConfig config = StreamNodeDescriptor.getInstance().getConfig();

  // this file is located in data/system/schema/system.properties
  // If user delete folder "data", system.properties can reset.
  public static final String PROPERTIES_FILE_NAME = "system.properties";
  private static final String SYSTEM_DIR = config.getSystemDir();

  private Properties properties = new Properties();

  private final Map<String, Supplier<String>> systemProperties = new HashMap<>();
  private final SystemPropertiesHandler systemPropertiesHandler;

  private static final String INTERNAL_ADDRESS = "sn_internal_address";
  private static final String INTERNAL_PORT = "sn_internal_port";

  // Mutable system parameters
  private static final Map<String, Supplier<String>> variableParamValueTable = new HashMap<>();

  static {
    variableParamValueTable.put(
        INTERNAL_ADDRESS, () -> String.valueOf(config.getSnInternalAddress()));
    variableParamValueTable.put(INTERNAL_PORT, () -> String.valueOf(config.getSnInternalPort()));
  }

  private static final String IOTDB_VERSION_STRING = "iotdb_version";
  private static final String COMMIT_ID_STRING = "commit_id";
  private static final String STREAM_NODE_ID = "stream_node_id";
  private static final String CLUSTER_ID = "cluster_id";

  public static IoTDBStartCheck getInstance() {
    return IoTDBConfigCheckHolder.INSTANCE;
  }

  public static void reinitializeStatics() {
    IoTDBConfigCheckHolder.INSTANCE = new IoTDBStartCheck();
  }

  private static class IoTDBConfigCheckHolder {

    private static IoTDBStartCheck INSTANCE = new IoTDBStartCheck();
  }

  private String getVal(String paramName) {
    if (variableParamValueTable.containsKey(paramName)) {
      return variableParamValueTable.get(paramName).get();
    } else {
      return null;
    }
  }

  private IoTDBStartCheck() {
    logger.info("Starting IoTDB {}", IoTDBConstant.VERSION_WITH_BUILD);
    systemPropertiesHandler = StreamNodeSystemPropertiesHandler.getInstance();

    systemProperties.put(IOTDB_VERSION_STRING, () -> IoTDBConstant.VERSION);
    systemProperties.put(COMMIT_ID_STRING, () -> IoTDBConstant.BUILD_INFO);
    for (String param : variableParamValueTable.keySet()) {
      systemProperties.put(param, () -> getVal(param));
    }
  }

  /** repair broken properties */
  private void upgradePropertiesFileFromBrokenFile() throws IOException {
    systemProperties.forEach(
        (k, v) -> {
          if (!properties.containsKey(k)) {
            properties.setProperty(k, v.get());
          }
        });
    properties.setProperty(IOTDB_VERSION_STRING, IoTDBConstant.VERSION);
    properties.setProperty(COMMIT_ID_STRING, IoTDBConstant.BUILD_INFO);
    systemPropertiesHandler.overwrite(properties);
  }

  private void throwException(String parameter, Object badValue) throws ConfigurationException {
    throw new ConfigurationException(
        parameter,
        String.valueOf(badValue),
        properties.getProperty(parameter),
        parameter + "can't be modified after first startup");
  }

  public void serializeStreamNodeId(int streamNodeId) throws IOException {
    systemPropertiesHandler.put(STREAM_NODE_ID, String.valueOf(streamNodeId));
  }

  public void serializeClusterID(String clusterId) throws IOException {
    systemPropertiesHandler.put(CLUSTER_ID, clusterId);
  }

  public void serializeMutableSystemPropertiesIfNecessary() throws IOException {
    long startTime = System.currentTimeMillis();
    boolean needsSerialize = false;
    for (String param : variableParamValueTable.keySet()) {
      if (!properties.getProperty(param).equals(getVal(param))) {
        needsSerialize = true;
      }
    }

    if (needsSerialize) {
      generateOrOverwriteSystemPropertiesFile();
    }
    long endTime = System.currentTimeMillis();
    logger.info(
        "Serialize mutable system properties successfully, which takes {} ms.",
        (endTime - startTime));
  }

  /** check and create directory before start Stream Node. */
  public void checkDirectory() throws ConfigurationException, IOException {
    // check system dir
    DirectoryChecker.getInstance().registerDirectory(new File(config.getSystemDir()));
  }

  public void checkSystemConfig() throws IOException {
    // read properties from system.properties
    properties = systemPropertiesHandler.read();

    if (!systemPropertiesHandler.isFirstStart()) {
      // check whether upgrading from <=v0.9
      if (!properties.containsKey(IOTDB_VERSION_STRING)) {
        logger.error(
            "DO NOT UPGRADE IoTDB from v0.9 or lower version to v1.0!"
                + " Please upgrade to v0.10 first");
        System.exit(-1);
      }
      String versionString = properties.getProperty(IOTDB_VERSION_STRING);
      if (versionString.startsWith("0.")) {
        logger.error("IoTDB version is too old");
        System.exit(-1);
      }
      checkImmutableSystemProperties();
    }
  }

  /** Check all immutable properties */
  private void checkImmutableSystemProperties() throws IOException {
    for (Map.Entry<String, Supplier<String>> entry : systemProperties.entrySet()) {
      if (!properties.containsKey(entry.getKey())) {
        upgradePropertiesFileFromBrokenFile();
        logger.info("repair system.properties, lack {}", entry.getKey());
      }
    }

    // load configuration from system properties only when start as Data node
    if (properties.containsKey(STREAM_NODE_ID)) {
      config.setStreamNodeId(Integer.parseInt(properties.getProperty(STREAM_NODE_ID)));
    }
    if (properties.containsKey(CLUSTER_ID)) {
      config.setClusterId(properties.getProperty(CLUSTER_ID));
    }
  }

  public void generateOrOverwriteSystemPropertiesFile() throws IOException {
    systemProperties.forEach((k, v) -> properties.setProperty(k, v.get()));
    systemPropertiesHandler.overwrite(properties);
  }

  public Properties getProperties() {
    return properties;
  }
}
