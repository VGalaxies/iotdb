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

import org.apache.iotdb.common.rpc.thrift.TStreamNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class StreamNodeTest {

  private StreamNodeConfig config;
  private StreamNode streamNode;
  private Constructor<StreamNode> ctor;

  @Before
  public void setUp() throws Exception {
    config = StreamNodeDescriptor.getInstance().getConfig();
    config.setStreamNodeId(42);
    config.setSnInternalAddress("127.0.0.1");
    config.setSnInternalPort(10821);
    config.setClusterName("test-cluster");
    config.setClusterId("test-cluster-id");

    // Create StreamNode instance via reflection (private constructor).
    ctor = StreamNode.class.getDeclaredConstructor();
    ctor.setAccessible(true);
    streamNode = ctor.newInstance();
  }

  @After
  public void tearDown() {
    // Reset the singleton holder so other tests don't see our instance.
    try {
      final Class<?> holderClass =
          Class.forName("org.apache.iotdb.streamnode.StreamNode$StreamNodeHolder");
      final Field instanceField = holderClass.getDeclaredField("INSTANCE");
      instanceField.setAccessible(true);
      instanceField.set(null, null);
    } catch (Exception ignored) {
      // best-effort cleanup
    }
  }

  // ==================== MBean methods ====================

  @Test
  public void testGetStreamNodeId() {
    assertEquals(42, streamNode.getStreamNodeId());
  }

  @Test
  public void testGetInternalAddress() {
    assertEquals("127.0.0.1", streamNode.getInternalAddress());
  }

  @Test
  public void testGetInternalPort() {
    assertEquals(10821, streamNode.getInternalPort());
  }

  @Test
  public void testGetClusterName() {
    assertEquals("test-cluster", streamNode.getClusterName());
  }

  @Test
  public void testGetClusterId() {
    assertEquals("test-cluster-id", streamNode.getClusterId());
  }

  @Test
  public void testGetNodeStatus() {
    // NodeStatus is set by CommonDescriptor; default is Running.
    assertNotNull(streamNode.getNodeStatus());
  }

  // ==================== generateStreamNodeLocation ====================

  @Test
  public void testGenerateStreamNodeLocation() throws Exception {
    final Method method = StreamNode.class.getDeclaredMethod("generateStreamNodeLocation");
    method.setAccessible(true);
    final TStreamNodeLocation location = (TStreamNodeLocation) method.invoke(streamNode);
    assertEquals(42, location.getStreamNodeId());
    assertNotNull(location.getInternalEndPoint());
    assertEquals("127.0.0.1", location.getInternalEndPoint().getIp());
    assertEquals(10821, location.getInternalEndPoint().getPort());
  }

  // ==================== generateStreamNodeConfiguration ====================

  @Test
  public void testGenerateStreamNodeConfiguration() throws Exception {
    final Method method = StreamNode.class.getDeclaredMethod("generateStreamNodeConfiguration");
    method.setAccessible(true);
    final TStreamNodeConfiguration config = (TStreamNodeConfiguration) method.invoke(streamNode);
    assertNotNull(config.getLocation());
    assertNotNull(config.getResource());
    assertEquals(42, config.getLocation().getStreamNodeId());
    assertTrue(config.getResource().getCpuCoreNum() > 0);
    assertTrue(config.getResource().getMaxMemory() > 0);
  }

  // ==================== remove ====================

  @Test(expected = UnsupportedOperationException.class)
  public void testRemoveThrowsUnsupportedOperation() {
    streamNode.remove(null);
  }

  // ==================== processPid ====================

  @Test
  public void testProcessPidRegistersDeleteOnExit() throws Exception {
    // processPid() reads the IOTDB_PIDFILE system property and calls File.deleteOnExit().
    // deleteOnExit() does not create the file on disk; it only registers a shutdown hook.
    // Verify the method executes without exception when the property is set.
    final String testPid = "target/streamnode-test.pid";
    System.setProperty(org.apache.iotdb.commons.conf.IoTDBConstant.IOTDB_PIDFILE, testPid);
    try {
      final Method method = StreamNode.class.getDeclaredMethod("processPid");
      method.setAccessible(true);
      // Should not throw.
      method.invoke(streamNode);
    } finally {
      System.clearProperty(org.apache.iotdb.commons.conf.IoTDBConstant.IOTDB_PIDFILE);
    }
  }

  // ==================== getInstance / singleton ====================

  @Test
  public void testGetInstanceReturnsSameObject() {
    assertSame(streamNode, StreamNode.getInstance());
  }

  @Test
  public void testGetServiceType() {
    // ServiceType.STREAM_NODE is the expected JMX type for StreamNode.
    assertEquals(ServiceType.STREAM_NODE.getJmxName(), "StreamNode");
  }

  @Test
  public void testGetLocalTempDataDirs() {
    final List<String> dirs = streamNode.getLocalTempDataDirs();
    assertEquals(1, dirs.size());
    assertEquals(config.getTmpDir(), dirs.get(0));
  }

  @Test
  public void testCleanupLocalTempDirsDeletesExistingDir() throws Exception {
    String originTmpDir = config.getTmpDir();
    final Path tmpDir = Files.createTempDirectory("sn-cleanup-test-");
    config.setTmpDir(tmpDir.toString());
    try {
      final File fileInDir = new File(tmpDir.toFile(), "stale-file.txt");
      assertTrue(fileInDir.createNewFile());
      new File(tmpDir.toFile(), "subdir").mkdir();
      assertTrue(tmpDir.toFile().exists());

      streamNode.cleanupLocalTempDirs();

      assertFalse("Directory should be deleted after cleanup", tmpDir.toFile().exists());
    } finally {
      Files.deleteIfExists(tmpDir);
      config.setTmpDir(originTmpDir); // restore default
    }
  }

  @Test
  public void testCleanupLocalTempDirsNonExistentDirSkips() {
    String originTmpDir = config.getTmpDir();
    final String nonExistentDir = "target/nonexistent-sn-cleanup-dir";
    config.setTmpDir(nonExistentDir);
    try {
      streamNode.cleanupLocalTempDirs();
      // non-existent dir is skipped silently: neither created nor deleted
      assertFalse(new File(nonExistentDir).exists());
    } finally {
      config.setTmpDir(originTmpDir); // restore default
    }
  }
}
