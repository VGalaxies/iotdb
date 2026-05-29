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

package org.apache.iotdb.streamnode;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.ServerCommandLine;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.concurrent.IoTDBDefaultThreadExceptionHandler;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemPropertiesHandler;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TRuntimeConfiguration;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TSystemConfigurationResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.streamnode.client.ConfigNodeInfo;
import org.apache.iotdb.streamnode.client.StreamNodeClient;
import org.apache.iotdb.streamnode.client.StreamNodeClientManager;
import org.apache.iotdb.streamnode.conf.IoTDBStartCheck;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.conf.StreamNodeStartupCheck;
import org.apache.iotdb.streamnode.conf.StreamNodeSystemPropertiesHandler;
import org.apache.iotdb.streamnode.manager.StreamTaskManager;
import org.apache.iotdb.streamnode.service.StreamNodeRPCService;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.commons.conf.IoTDBConstant.DEFAULT_CLUSTER_NAME;
import static org.apache.iotdb.commons.queryengine.utils.DateTimeUtils.initTimestampPrecision;
import static org.apache.iotdb.commons.utils.StatusUtils.retrieveExitStatusCode;

public class StreamNode extends ServerCommandLine implements StreamNodeMBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamNode.class);
  private static final StreamNodeConfig config = StreamNodeDescriptor.getInstance().getConfig();

  private final RegisterManager registerManager = new RegisterManager();

  private final SystemPropertiesHandler systemPropertiesHandler =
      StreamNodeSystemPropertiesHandler.getInstance();

  /**
   * When joining a cluster or getting configuration this node will retry at most "DEFAULT_RETRY"
   * times before returning a failure to the client.
   */
  private static final int DEFAULT_RETRY = 200;

  private static final long DEFAULT_RETRY_INTERVAL_IN_MS = config.getJoinClusterRetryIntervalMs();

  private static final String REGISTER_INTERRUPTION =
      "Unexpected interruption when waiting to register to the cluster";

  private static Thread watcherThread;

  private final String mbeanName =
      String.format(
          "%s:%s=%s",
          IoTDBConstant.IOTDB_SERVICE_JMX_NAME,
          IoTDBConstant.JMX_TYPE,
          ServiceType.STREAM_NODE.getJmxName());

  private StreamNode() {
    super("StreamNode");
    StreamNodeHolder.INSTANCE = this;
  }

  @Override
  protected void start() {
    LOGGER.info("Starting StreamNode...");
    LOGGER.info(
        "StreamNode config: cluster_name={}, sn_internal_address={}, sn_internal_port={},"
            + " sn_seed_config_node={}",
        config.getClusterName(),
        config.getSnInternalAddress(),
        config.getSnInternalPort(),
        config.getSnSeedConfigNode());
    boolean isFirstStart;
    try {
      // Check if this StreamNode is start for the first time and do other pre-checks
      isFirstStart = prepareStreamNode();

      if (isFirstStart) {
        LOGGER.info("StreamNode is starting for the first time...");
        ConfigNodeInfo.getInstance()
            .updateConfigNodeList(Collections.singletonList(config.getSnSeedConfigNode()));
      } else {
        LOGGER.info("StreamNode is restarting...");
        // Load registered ConfigNodes from system.properties
        ConfigNodeInfo.getInstance().loadConfigNodeList();
      }

      // Pull and check system configurations from ConfigNode-leader
      pullAndCheckSystemConfigurations();

      if (isFirstStart) {
        sendRegisterRequestToConfigNode(true);
        IoTDBStartCheck.getInstance().generateOrOverwriteSystemPropertiesFile();
        ConfigNodeInfo.getInstance().storeConfigNodeList();
        sendRegisterRequestToConfigNode(false);
      } else {
        // Send restart request of this StreamNode
        sendRestartRequestToConfigNode();
      }

      // Active StreamNode
      active();

      // Serialize mutable system properties
      IoTDBStartCheck.getInstance().serializeMutableSystemPropertiesIfNecessary();
      ConfigurationFileUtils.updateAppliedProperties(
          IoTDBStartCheck.getInstance().getProperties(), false);

      LOGGER.info("StreamNode configuration: {}", config.getConfigMessage());
      LOGGER.info("Congratulations, IoTDB StreamNode is set up successfully. Now, enjoy yourself!");
    } catch (Throwable e) {
      int exitStatusCode = retrieveExitStatusCode(e);
      LOGGER.error("Fail to start server", e);
      stop();
      System.exit(exitStatusCode);
    }
    LOGGER.info("StreamNode started successfully");
  }

  /**
   * Register services and set up StreamNode.
   *
   * @throws StartupException if start up failed.
   */
  private void active() throws StartupException, IOException {
    try {
      processPid();
      setUp();
    } catch (StartupException | IOException e) {
      LOGGER.error("Meet error while starting up.", e);
      throw e;
    }
    LOGGER.info("IoTDB StreamNode has started.");
  }

  private void registerInternalRPCService() throws StartupException {
    registerManager.register(StreamNodeRPCService.getInstance());
    LOGGER.info(
        "StreamNode RPC service listening on {}:{}",
        config.getSnInternalAddress(),
        config.getSnInternalPort());
  }

  void processPid() {
    String pidFile = System.getProperty(IoTDBConstant.IOTDB_PIDFILE);
    if (pidFile != null) {
      new File(pidFile).deleteOnExit();
    }
  }

  private void setUp() throws StartupException, IOException {
    LOGGER.info("Setting up IoTDB StreamNode...");
    registerManager.register(new JMXService());
    JMXService.registerMBean(getInstance(), mbeanName);

    addShutDownHook();
    setUncaughtExceptionHandler();

    registerManager.register(MetricService.getInstance());
    registerManager.register(StreamTaskManager.getInstance());
    registerInternalRPCService();
    LOGGER.info("Successfully setup internal services.");
  }

  private void addShutDownHook() {
    Runtime.getRuntime().addShutdownHook(new StreamNodeShutdownHook(generateStreamNodeLocation()));
  }

  private void setUncaughtExceptionHandler() {
    Thread.setDefaultUncaughtExceptionHandler(new IoTDBDefaultThreadExceptionHandler());
  }

  @Override
  protected void remove(Set<Integer> nodeIds) {
    throw new UnsupportedOperationException();
  }

  public void stop() {
    LOGGER.info("Stopping StreamNode...");
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    LOGGER.info("StreamNode stopped");
  }

  /** Prepare cluster IoTDB-StreamNode */
  private boolean prepareStreamNode() throws StartupException, IOException {
    long startTime = System.currentTimeMillis();

    // Startup checks
    StreamNodeStartupCheck checks = new StreamNodeStartupCheck(IoTDBConstant.SN_ROLE, config);
    checks.startUpCheck();
    long endTime = System.currentTimeMillis();
    LOGGER.info(
        "The StreamNode is prepared successfully, which takes {} ms", (endTime - startTime));
    return StreamNodeSystemPropertiesHandler.getInstance().isFirstStart();
  }

  /**
   * Pull global configurations from ConfigNode-leader and apply them. StreamNode only needs
   * timestampPrecision from the global config, unlike DataNode which also needs consensus protocol
   * and other configs.
   */
  private void pullAndCheckSystemConfigurations() throws StartupException {
    LOGGER.info("Pulling system configurations from the ConfigNode-leader...");
    long startTime = System.currentTimeMillis();
    TSystemConfigurationResp configurationResp = null;
    int retry = DEFAULT_RETRY;
    while (retry > 0) {
      try (StreamNodeClient configNodeClient =
          StreamNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        configurationResp = configNodeClient.getSystemConfiguration();
        break;
      } catch (TException | ClientManagerException e) {
        LOGGER.warn(
            "Cannot pull system configurations from ConfigNode-leader, because: {}",
            e.getMessage());
        retry--;
      }

      try {
        Thread.sleep(DEFAULT_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn(REGISTER_INTERRUPTION, e);
        retry = -1;
      }
    }

    if (configurationResp == null
        || !configurationResp.isSetStatus()
        || configurationResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.error(
          "Cannot pull system configurations from ConfigNode-leader after {} retries.",
          DEFAULT_RETRY);
      throw new StartupException(
          "Cannot pull system configurations from ConfigNode-leader. "
              + "Please check whether the sn_seed_config_node in iotdb-streamnode.properties is correct or alive.");
    }

    /* Load system configurations */
    CommonDescriptor.getInstance().loadGlobalConfig(configurationResp.globalConfig);

    /* Check system configurations */
    try {
      IoTDBStartCheck.getInstance().checkSystemConfig();
      IoTDBStartCheck.getInstance().checkDirectory();
    } catch (Exception e) {
      throw new StartupException(e.getMessage());
    }

    ConfigurationFileUtils.updateAppliedPropertiesFromCN(configurationResp);
    // init
    initTimestampPrecision();
    long endTime = System.currentTimeMillis();
    LOGGER.info(
        "Successfully pull system configurations from ConfigNode-leader, which takes {} ms",
        (endTime - startTime));
  }

  /**
   * Register this StreamNode into cluster.
   *
   * @param isPreCheck do pre-check before formal registration
   * @throws StartupException if register failed.
   * @throws IOException if serialize cluster name and streamnode id failed.
   */
  private void sendRegisterRequestToConfigNode(boolean isPreCheck)
      throws StartupException, IOException {
    LOGGER.info("Sending register request to ConfigNode-leader...");
    long startTime = System.currentTimeMillis();
    /* Send register request */
    int retry = DEFAULT_RETRY;
    TStreamNodeRegisterReq req = new TStreamNodeRegisterReq();
    req.setPreCheck(isPreCheck);
    req.setStreamNodeConfiguration(generateStreamNodeConfiguration());
    req.setClusterName(config.getClusterName());
    req.setVersionInfo(new TNodeVersionInfo(IoTDBConstant.VERSION, IoTDBConstant.BUILD_INFO));
    TStreamNodeRegisterResp streamNodeRegisterResp = null;
    while (retry > 0) {
      try (StreamNodeClient configNodeClient =
          StreamNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        streamNodeRegisterResp = configNodeClient.registerStreamNode(req);
        break;
      } catch (TException | ClientManagerException e) {
        LOGGER.warn("Cannot register to the cluster, because: {}", e.getMessage());
        retry--;
      }

      try {
        // Wait to start the next try
        Thread.sleep(DEFAULT_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn(REGISTER_INTERRUPTION, e);
        retry = -1;
      }
    }
    if (streamNodeRegisterResp == null) {
      // All tries failed
      LOGGER.error("Cannot register into cluster after {} retries.", DEFAULT_RETRY);
      throw new StartupException(
          "Cannot register into the cluster. "
              + "Please check whether the sn_seed_config_node in iotdb-stream.properties is correct or alive.");
    }

    if (streamNodeRegisterResp.getStatus().getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      if (isPreCheck) {
        LOGGER.info("Successfully pass the precheck, will do the formal registration soon.");
        return;
      }
      /* Store runtime configurations when register success */
      int streamNodeID = streamNodeRegisterResp.getStreamNodeId();
      config.setStreamNodeId(streamNodeID);
      IoTDBStartCheck.getInstance().serializeStreamNodeId(streamNodeID);
      storeRuntimeConfigurations(
          streamNodeRegisterResp.getConfigNodeList(),
          streamNodeRegisterResp.getRuntimeConfiguration());
      long endTime = System.currentTimeMillis();

      LOGGER.info(
          "Successfully register to the cluster: {} , which takes {} ms.",
          config.getClusterName(),
          (endTime - startTime));
    } else {
      /* Throw exception when register failed */
      LOGGER.error(streamNodeRegisterResp.getStatus().getMessage());
      throw new StartupException("Cannot register to the cluster.");
    }
  }

  private void sendRestartRequestToConfigNode() throws StartupException {
    LOGGER.info("Sending restart request to ConfigNode-leader...");
    long startTime = System.currentTimeMillis();
    /* Send restart request */
    int retry = DEFAULT_RETRY;
    TStreamNodeRestartReq req = new TStreamNodeRestartReq();
    req.setClusterName(
        config.getClusterName() == null ? DEFAULT_CLUSTER_NAME : config.getClusterName());
    req.setStreamNodeConfiguration(generateStreamNodeConfiguration());
    req.setVersionInfo(new TNodeVersionInfo(IoTDBConstant.VERSION, IoTDBConstant.BUILD_INFO));
    req.setClusterId(config.getClusterId());
    TStreamNodeRestartResp streamNodeRestartResp = null;
    while (retry > 0) {
      try (StreamNodeClient configNodeClient =
          StreamNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        streamNodeRestartResp = configNodeClient.restartStreamNode(req);
        break;
      } catch (TException | ClientManagerException e) {
        LOGGER.warn(
            "Cannot send restart request to the ConfigNode-leader, because: {}", e.getMessage());
        retry--;
      }

      try {
        // Wait to start the next try
        Thread.sleep(DEFAULT_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn(REGISTER_INTERRUPTION, e);
        retry = -1;
      }
    }
    if (streamNodeRestartResp == null) {
      // All tries failed
      LOGGER.error(
          "Cannot send restart StreamNode request to ConfigNode-leader after {} retries.",
          DEFAULT_RETRY);
      throw new StartupException(
          "Cannot send restart StreamNode request to ConfigNode-leader. "
              + "Please check whether the dn_seed_config_node in iotdb-stream.properties is correct or alive.");
    }

    if (streamNodeRestartResp.getStatus().getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      /* Store runtime configurations when restart request is accepted */
      storeRuntimeConfigurations(
          streamNodeRestartResp.getConfigNodeList(),
          streamNodeRestartResp.getRuntimeConfiguration());
      long endTime = System.currentTimeMillis();
      LOGGER.info(
          "Restart request to cluster: {} is accepted, which takes {} ms.",
          config.getClusterName(),
          (endTime - startTime));
    } else {
      /* Throw exception when restart is rejected */
      throw new StartupException(streamNodeRestartResp.getStatus().getMessage());
    }
  }

  protected void storeRuntimeConfigurations(
      List<TConfigNodeLocation> configNodeLocations, TRuntimeConfiguration runtimeConfiguration)
      throws StartupException {
    /* Store ConfigNodeList */
    List<TEndPoint> configNodeList = new ArrayList<>();
    for (TConfigNodeLocation configNodeLocation : configNodeLocations) {
      configNodeList.add(configNodeLocation.getInternalEndPoint());
    }
    ConfigNodeInfo.getInstance().updateConfigNodeList(configNodeList);

    /* Store cluster ID */
    String clusterId = runtimeConfiguration.getClusterId();
    storeClusterID(clusterId);
  }

  private void storeClusterID(String clusterID) throws StartupException {
    try {
      if (config.getClusterId().isEmpty()) {
        config.setClusterId(clusterID);
        IoTDBStartCheck.getInstance().serializeClusterID(clusterID);
      }
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  public static void main(String[] args) {
    final Thread hookThread = Thread.currentThread();
    watcherThread =
        new Thread(
            () -> {
              while (!Thread.interrupted() && hookThread.isAlive()) {
                try {
                  Thread.sleep(10000);
                  final StackTraceElement[] stackTrace = hookThread.getStackTrace();
                  final StringBuilder stackTraceBuilder =
                      new StringBuilder("Stack trace of main thread:\n");
                  for (final StackTraceElement traceElement : stackTrace) {
                    stackTraceBuilder.append(traceElement.toString()).append("\n");
                  }
                  LOGGER.info(stackTraceBuilder.toString());
                } catch (final InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
              }
            },
            "StreamNodeStartWatcher");
    watcherThread.setDaemon(true);
    watcherThread.start();

    LOGGER.info("IoTDB-StreamNode default charset is: {}", Charset.defaultCharset().displayName());
    StreamNode streamNode = new StreamNode();
    int returnCode = streamNode.run(args);
    watcherThread.interrupt();
    if (returnCode != 0) {
      System.exit(returnCode);
    }
  }

  public TStreamNodeLocation generateStreamNodeLocation() {
    TStreamNodeLocation location = new TStreamNodeLocation();
    location.setStreamNodeId(config.getStreamNodeId());
    location.setInternalEndPoint(
        new TEndPoint(config.getSnInternalAddress(), config.getSnInternalPort()));
    return location;
  }

  public TStreamNodeConfiguration generateStreamNodeConfiguration() {
    TStreamNodeLocation location = generateStreamNodeLocation();

    TNodeResource resource = new TNodeResource();
    resource.setCpuCoreNum(Runtime.getRuntime().availableProcessors());
    resource.setMaxMemory(Runtime.getRuntime().totalMemory());

    return new TStreamNodeConfiguration(location, resource);
  }

  @Override
  public int getStreamNodeId() {
    return config.getStreamNodeId();
  }

  @Override
  public String getInternalAddress() {
    return config.getSnInternalAddress();
  }

  @Override
  public int getInternalPort() {
    return config.getSnInternalPort();
  }

  @Override
  public int getTaskCount() {
    return StreamTaskManager.getInstance().getTaskNum();
  }

  @Override
  public int getRunningTaskCount() {
    return StreamTaskManager.getInstance().getRunningTaskNum();
  }

  @Override
  public String getClusterName() {
    return config.getClusterName();
  }

  @Override
  public String getClusterId() {
    return config.getClusterId();
  }

  @Override
  public String getNodeStatus() {
    return CommonDescriptor.getInstance().getConfig().getNodeStatus().getStatus();
  }

  private static class StreamNodeHolder {
    private static StreamNode INSTANCE;

    private StreamNodeHolder() {
      // Empty constructor
    }
  }

  public static StreamNode getInstance() {
    return StreamNodeHolder.INSTANCE;
  }
}
