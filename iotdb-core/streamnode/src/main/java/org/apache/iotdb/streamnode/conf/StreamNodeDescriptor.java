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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class StreamNodeDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamNodeDescriptor.class);

  private static final String CONF_FILE_NAME = "iotdb-streamnode.properties";

  private final StreamNodeConfig config;

  private StreamNodeDescriptor() {
    config = new StreamNodeConfig();
    loadProperties();
  }

  private void loadProperties() {
    // Try IOTDB_CONF env, then classpath
    String confDir = System.getProperty("IOTDB_CONF", null);
    if (confDir == null) {
      confDir = System.getenv("IOTDB_CONF");
    }

    Properties properties = new Properties();
    String filePath = null;
    if (confDir != null) {
      filePath = confDir + File.separator + CONF_FILE_NAME;
      File file = new File(filePath);
      if (file.exists()) {
        try (InputStream in = new FileInputStream(file)) {
          properties.load(in);
          LOGGER.info("Loaded StreamNode properties from {}", filePath);
        } catch (IOException e) {
          LOGGER.warn("Failed to load StreamNode properties from {}", filePath, e);
        }
      }
    }

    if (properties.isEmpty()) {
      try (InputStream in =
          StreamNodeDescriptor.class.getClassLoader().getResourceAsStream(CONF_FILE_NAME)) {
        if (in != null) {
          properties.load(in);
          LOGGER.info("Loaded StreamNode properties from classpath");
        }
      } catch (IOException e) {
        LOGGER.warn("Failed to load StreamNode properties from classpath", e);
      }
    }

    if (!properties.isEmpty()) {
      loadFromProperties(properties);
    } else {
      LOGGER.info("Using default StreamNode configuration");
    }
  }

  private void loadFromProperties(Properties properties) {
    config.setClusterName(properties.getProperty("cluster_name", config.getClusterName()));

    config.setSnInternalAddress(
        properties.getProperty("sn_internal_address", config.getSnInternalAddress()));

    config.setSnInternalPort(
        Integer.parseInt(
            properties.getProperty(
                "sn_internal_port", String.valueOf(config.getSnInternalPort()))));

    config.setSnSeedConfigNode(
        properties.getProperty("sn_seed_config_node", config.getSnSeedConfigNode()));

    config.setRpcMaxConcurrentClientNum(
        Integer.parseInt(
            properties.getProperty(
                "sn_rpc_max_concurrent_client_num",
                String.valueOf(config.getRpcMaxConcurrentClientNum()))));

    config.setRpcThriftCompressionEnable(
        Boolean.parseBoolean(
            properties.getProperty(
                "sn_rpc_thrift_compression_enable",
                String.valueOf(config.isRpcThriftCompressionEnable()))));

    config.setExecutorThreadNum(
        Integer.parseInt(
            properties.getProperty(
                "sn_executor_thread_num", String.valueOf(config.getExecutorThreadNum()))));
  }

  public StreamNodeConfig getConfig() {
    return config;
  }

  private static class StreamNodeDescriptorHolder {
    private static final StreamNodeDescriptor INSTANCE = new StreamNodeDescriptor();
  }

  public static StreamNodeDescriptor getInstance() {
    return StreamNodeDescriptorHolder.INSTANCE;
  }
}
