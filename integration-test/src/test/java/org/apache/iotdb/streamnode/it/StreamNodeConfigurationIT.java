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

package org.apache.iotdb.streamnode.it;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.StreamNodeWrapper;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for StreamNode configuration management. Verifies that {@code set configuration}
 * persists a restart-effective parameter into the StreamNode's iotdb-system.properties file, and
 * that after restarting the StreamNode, {@code show configuration} reflects the updated value.
 */
@Category({StreamClusterIT.class})
public class StreamNodeConfigurationIT {

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(60, TimeUnit.SECONDS);

  private static final String CONFIG_KEY = "sn_executor_thread_num";
  private static final String DEFAULT_VALUE = "0";
  private static final String UPDATED_VALUE = "5";

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1S cluster environment (1 ConfigNode, 1 DataNode, 1 StreamNode)
    StreamEnvFactory.getEnv(1)
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass("org.apache.iotdb.consensus.iot.IoTConsensus");
    StreamEnvFactory.getEnv(1).initClusterEnvironment(1, 1, 30);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    StreamEnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void restoreConfig() {
    // Restore the default value so later tests in this class are not affected.
    try {
      int streamNodeId = getStreamNodeId();
      try (Connection connection =
              StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "set configuration \"%s\"='%s' on %d", CONFIG_KEY, DEFAULT_VALUE, streamNodeId));
      }
    } catch (Exception e) {
      // Best-effort restore; the actual test failure (if any) must not be masked.
    }
  }

  @Test
  public void testSetConfigurationAndRestartStreamNode() throws Exception {
    int streamNodeId = getStreamNodeId();

    // 1. set configuration: sn_executor_thread_num is restart-effective, so it must be persisted
    // to the StreamNode's iotdb-system.properties file without being hot-loaded.
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "set configuration \"%s\"='%s' on %d", CONFIG_KEY, UPDATED_VALUE, streamNodeId));
    }

    // 2. Verify the value has been written into the StreamNode's config file.
    StreamNodeWrapper streamNodeWrapper = getStreamNodeWrapper();
    assertTrue(
        "The updated configuration should be written into the StreamNode's config file.",
        checkConfigFileContains(streamNodeWrapper, CONFIG_KEY + "=" + UPDATED_VALUE));

    // 3. Restart the StreamNode so that the restart-effective parameter takes effect.
    streamNodeWrapper.stop();
    streamNodeWrapper.start();
    AWAIT.until(() -> streamNodeWrapper.isAlive());

    // 4. Wait until the StreamNode has rejoined the cluster and its status becomes Running, then
    // show configuration should reflect the updated value after restart.
    AWAIT.until(() -> isStreamNodeRunning(streamNodeId));
    assertAppliedConfiguration(streamNodeId, CONFIG_KEY, UPDATED_VALUE);
  }

  private static boolean isStreamNodeRunning(int nodeId) {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("show streamnodes")) {
      while (rs.next()) {
        if (nodeId == rs.getInt(ColumnHeaderConstant.NODE_ID)) {
          return NodeStatus.Running.getStatus().equals(rs.getString(ColumnHeaderConstant.STATUS));
        }
      }
    } catch (SQLException e) {
      return false;
    }
    return false;
  }

  /** Get the StreamNode id from SHOW STREAMNODES. */
  private static int getStreamNodeId() throws SQLException {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("show streamnodes")) {
      assertTrue("There should be at least one StreamNode.", rs.next());
      int nodeId = rs.getInt(ColumnHeaderConstant.NODE_ID);
      assertTrue("StreamNode id should be non-negative.", nodeId >= 0);
      return nodeId;
    }
  }

  private static StreamNodeWrapper getStreamNodeWrapper() {
    List<StreamNodeWrapper> streamNodeWrappers =
        ((AbstractEnv) StreamEnvFactory.getEnv())
            .getNodeWrapperList().stream()
                .filter(wrapper -> wrapper instanceof StreamNodeWrapper)
                .map(wrapper -> (StreamNodeWrapper) wrapper)
                .collect(Collectors.toList());
    assertEquals("There should be exactly one StreamNode wrapper.", 1, streamNodeWrappers.size());
    return streamNodeWrappers.get(0);
  }

  private static boolean checkConfigFileContains(
      AbstractNodeWrapper nodeWrapper, String... contents) {
    try {
      String systemPropertiesPath =
          nodeWrapper.getNodePath()
              + File.separator
              + "conf"
              + File.separator
              + CommonConfig.SYSTEM_CONFIG_NAME;
      File f = new File(systemPropertiesPath);
      String fileContent = new String(Files.readAllBytes(f.toPath()));
      for (String content : contents) {
        if (!fileContent.contains(content)) {
          return false;
        }
      }
      return true;
    } catch (IOException ignore) {
      return false;
    }
  }

  private static void assertAppliedConfiguration(int nodeId, String key, String value)
      throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("show configuration on " + nodeId)) {
      while (rs.next()) {
        if (key.equals(rs.getString(1))) {
          assertEquals(value, rs.getString(2));
          return;
        }
      }
    }
    Assert.fail("Cannot find applied configuration: " + key + " on node " + nodeId);
  }
}
