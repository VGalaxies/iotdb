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

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.conf.TrimProperties;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;

import org.apache.tsfile.utils.FilePathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

public class StreamNodeDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamNodeDescriptor.class);
  static final String STREAMNODE_CONF = "STREAMNODE_CONF";

  private final StreamNodeConfig config;
  private final StreamNodeMemoryConfig memoryConfig;

  private StreamNodeDescriptor() {
    config = new StreamNodeConfig();
    memoryConfig = new StreamNodeMemoryConfig();
    loadProperties();
  }

  private void loadProperties() {
    // Try STREAMNODE_CONF first, then STREAMNODE_CONF env
    String confDir = System.getProperty(STREAMNODE_CONF, null);
    if (confDir == null) {
      confDir = System.getenv(STREAMNODE_CONF);
    }

    Properties properties = new Properties();
    String filePath = null;
    if (confDir != null) {
      filePath = confDir + File.separator + CommonConfig.SYSTEM_CONFIG_NAME;
      File file = new File(filePath);
      if (file.exists()) {
        try (InputStream in = Files.newInputStream(file.toPath())) {
          properties.load(in);
          LOGGER.info("Loaded StreamNode properties from {}", filePath);
        } catch (IOException e) {
          LOGGER.warn("Failed to load StreamNode properties from {}", filePath, e);
        }
      }
    }

    if (properties.isEmpty()) {
      try (InputStream in =
          StreamNodeDescriptor.class
              .getClassLoader()
              .getResourceAsStream(CommonConfig.SYSTEM_CONFIG_NAME)) {
        if (in != null) {
          properties.load(in);
          LOGGER.info("Loaded StreamNode properties from classpath");
        }
      } catch (IOException e) {
        LOGGER.warn("Failed to load StreamNode properties from classpath", e);
      }
    }

    if (!properties.isEmpty()) {
      try {
        loadFromProperties(properties);
        ConfigurationFileUtils.updateAppliedProperties(properties, false);
      } catch (BadNodeUrlException | IOException e) {
        LOGGER.error("Couldn't load StreamNode conf file, reject StreamNode startup.", e);
        System.exit(-1);
      }
    } else {
      LOGGER.info("Using default StreamNode configuration");
    }

    // Resolve relative paths (data/streamnode) against STREAMNODE_HOME.
    // Placed here (outside loadFromProperties) so it runs on the default-config
    // path as well, aligned with ConfigNode which calls updatePath in finally.
    config.formulateFolders();

    memoryConfig.init(properties, config.getSessionScanConcurrency());
  }

  private void loadFromProperties(Properties properties) throws BadNodeUrlException, IOException {
    config.setClusterName(properties.getProperty("cluster_name", config.getClusterName()));

    config.setSnInternalAddress(
        properties.getProperty("sn_internal_address", config.getSnInternalAddress()));

    config.setSnInternalPort(
        Integer.parseInt(
            properties.getProperty(
                "sn_internal_port", String.valueOf(config.getSnInternalPort()))));

    String seedConfigNode = properties.getProperty("sn_seed_config_node");
    if (seedConfigNode != null) {
      config.setSnSeedConfigNode(NodeUrlUtils.parseTEndPointUrls(seedConfigNode).get(0));
    }

    config.setSnClusterIngressNodeUrls(
        Arrays.stream(
                properties
                    .getProperty(
                        "sn_cluster_ingress_node_urls",
                        String.join(",", config.getSnClusterIngressNodeUrls()))
                    .split(","))
            .map(String::trim)
            .collect(Collectors.toList()));

    config.setSnClusterIngressUsername(
        properties.getProperty(
            "sn_cluster_ingress_username", config.getSnClusterIngressUsername()));

    config.setSnClusterIngressPassword(
        properties.getProperty(
            "sn_cluster_ingress_password", config.getSnClusterIngressPassword()));

    config.setSnWriteBackSessionMaxRetryCount(
        Integer.parseInt(
            properties.getProperty(
                "sn_write_back_session_max_retry_count",
                String.valueOf(config.getSnWriteBackSessionMaxRetryCount()))));
    config.setSnWriteBackSessionRetryIntervalMs(
        Long.parseLong(
            properties.getProperty(
                "sn_write_back_session_retry_interval_ms",
                String.valueOf(config.getSnWriteBackSessionRetryIntervalMs()))));
    config.setSnWriteBackSessionConnectionTimeoutMs(
        Integer.parseInt(
            properties.getProperty(
                "sn_write_back_session_connection_timeout_ms",
                String.valueOf(config.getSnWriteBackSessionConnectionTimeoutMs()))));
    config.setSnWriteBackMaxRetryAttempts(
        Integer.parseInt(
            properties.getProperty(
                "sn_write_back_max_retry_attempts",
                String.valueOf(config.getSnWriteBackMaxRetryAttempts()))));
    config.setSnWriteBackRetryBackoffBaseMs(
        Long.parseLong(
            properties.getProperty(
                "sn_write_back_retry_backoff_base_ms",
                String.valueOf(config.getSnWriteBackRetryBackoffBaseMs()))));

    int sessionScanConcurrency =
        Integer.parseInt(
            properties.getProperty(
                "sn_session_scan_concurrency", String.valueOf(config.getSessionScanConcurrency())));
    if (sessionScanConcurrency <= 0) {
      sessionScanConcurrency = Runtime.getRuntime().availableProcessors();
    }
    config.setSessionScanConcurrency(sessionScanConcurrency);

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

    int executorThreadNum =
        Integer.parseInt(
            properties.getProperty(
                "sn_executor_thread_num", String.valueOf(config.getExecutorThreadNum())));
    if (executorThreadNum <= 0) {
      executorThreadNum = Runtime.getRuntime().availableProcessors();
    }
    config.setExecutorThreadNum(executorThreadNum);

    config.setDriverTaskExecutionTimeSliceInMS(
        Integer.parseInt(
            properties.getProperty(
                "sn_driver_task_execution_time_slice_in_ms",
                String.valueOf(config.getDriverTaskExecutionTimeSliceInMS()))));

    // tmp file path for streamnode
    config.setTmpDir(properties.getProperty("sn_tmp_dir", config.getTmpDir()));

    String systemDir = properties.getProperty("sn_system_dir");
    if (systemDir == null) {
      systemDir = properties.getProperty("base_dir");
      if (systemDir != null) {
        systemDir = FilePathUtils.regularizePath(systemDir) + IoTDBConstant.SYSTEM_FOLDER_NAME;
      } else {
        systemDir = config.getSystemDir();
      }
    }
    config.setSystemDir(systemDir);

    // Load metric configuration (reporter list, level, prometheus port, etc.) using the
    // "sn_" prefix. Must run before MetricService starts so reporters are created correctly.
    MetricConfigDescriptor.getInstance().loadProps(properties, "sn_");
  }

  /**
   * Get props url location.
   *
   * @param configFileName configuration file name
   * @return url object if location exit, otherwise null.
   */
  public static URL getPropsUrl(String configFileName) {
    String urlString = System.getProperty(STREAMNODE_CONF, null);
    if (urlString == null) {
      urlString = System.getenv(STREAMNODE_CONF);
    }
    if (urlString == null) {
      return null;
    }
    // If a config location was provided, but it doesn't end with a properties file,
    // append the default location.
    if (!urlString.endsWith(".properties")) {
      urlString += (File.separatorChar + configFileName);
    }
    // If the url doesn't start with "file:" or "classpath:", it's provided as a no path.
    // So we need to add it to make it a real URL.
    if (!urlString.startsWith("file:") && !urlString.startsWith("classpath:")) {
      urlString = "file:" + urlString;
    }
    try {
      return new URL(urlString);
    } catch (MalformedURLException e) {
      LOGGER.warn("Failed to get props url", e);
      return null;
    }
  }

  /**
   * Reload the configuration file and apply hot modified properties.
   *
   * @throws QueryProcessException when fail to reload configuration file
   */
  public synchronized void loadHotModifiedProps() throws QueryProcessException {
    URL url = getPropsUrl(CommonConfig.SYSTEM_CONFIG_NAME);
    if (url == null) {
      LOGGER.warn("Could not load configuration file.");
      return;
    }

    TrimProperties commonProperties = new TrimProperties();
    try (InputStream inputStream = url.openStream()) {
      LOGGER.info("Reloading configuration from {}", url);
      commonProperties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      ConfigurationFileUtils.loadConfigurationDefaultValueFromTemplate();
      loadHotModifiedProps(commonProperties);
    } catch (Exception e) {
      LOGGER.warn("Failed to reload configuration file {}", url, e);
      throw new QueryProcessException(
          String.format("Failed to reload configuration file %s because: %s", url, e.getMessage()));
    } finally {
      ConfigurationFileUtils.releaseDefault();
    }
  }

  /**
   * Load hot modified properties. Currently, all sn_* parameters take effect only on restart or
   * first start, so no parameter is hot reloadable. This entry is reserved for future hot
   * reloadable parameters.
   *
   * @param properties the properties to be hot reloaded
   */
  public synchronized void loadHotModifiedProps(TrimProperties properties) {
    ConfigurationFileUtils.updateAppliedProperties(properties, true);
  }

  public StreamNodeConfig getConfig() {
    return config;
  }

  public StreamNodeMemoryConfig getMemoryConfig() {
    return memoryConfig;
  }

  private static class StreamNodeDescriptorHolder {
    private static final StreamNodeDescriptor INSTANCE = new StreamNodeDescriptor();
  }

  public static StreamNodeDescriptor getInstance() {
    return StreamNodeDescriptorHolder.INSTANCE;
  }
}
