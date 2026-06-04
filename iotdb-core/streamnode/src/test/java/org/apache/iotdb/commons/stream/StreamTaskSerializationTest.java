/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.stream;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

public class StreamTaskSerializationTest {

  @Test
  public void testStreamTaskRoundTripKeepsDefinitionFields() throws Exception {
    final StreamTask task = buildTask("task_a");

    final StreamTask restored = StreamTask.deserialize(task.toByteBuffer());

    Assert.assertEquals(task.getId(), restored.getId());
    Assert.assertEquals(task.getTaskName(), restored.getTaskName());
    Assert.assertEquals(task.getCreator(), restored.getCreator());
    Assert.assertEquals(task.getCreationTime(), restored.getCreationTime());
    Assert.assertEquals(task.getSubQuery(), restored.getSubQuery());

    final IoTDBSubscriptionSource source = (IoTDBSubscriptionSource) restored.getSource();
    Assert.assertEquals("db", source.getDatabase());
    Assert.assertEquals("table1", source.getTableName());
    Assert.assertTrue(source.getPreFilter() instanceof Identifier);
    Assert.assertEquals("status", ((Identifier) source.getPreFilter()).getValue());
    Assert.assertEquals(Collections.singletonList("status"), source.getPartitionColumns());

    final PeriodWindow window = (PeriodWindow) restored.getWindow();
    Assert.assertEquals(1000L, window.getPeriodMs());
    Assert.assertEquals(10L, window.getOriginMs());

    final IoTDBTarget target = (IoTDBTarget) restored.getTarget();
    Assert.assertEquals("target_db", target.getDatabase());
    Assert.assertEquals("target_table", target.getTableName());
    Assert.assertEquals(Arrays.asList("s1", "s2"), target.getColumnNames());

    Assert.assertEquals(Long.valueOf(5L), restored.getProperties().getWatermarkMs());
  }

  @Test
  public void testStreamTaskSerializationDoesNotCloseOuterStream() throws Exception {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final DataOutputStream dos = new DataOutputStream(baos);

    buildTask("task_a").serialize(dos);
    buildTask("task_b").serialize(dos);

    final ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    Assert.assertEquals("task_a", StreamTask.deserialize(buffer).getTaskName());
    Assert.assertEquals("task_b", StreamTask.deserialize(buffer).getTaskName());
  }

  @Test
  public void testAllWindowTypesRoundTrip() throws Exception {
    assertWindowRoundTrip(new TumbleWindow("time", 1000, 0), TumbleWindow.class);
    assertWindowRoundTrip(new HopWindow("time", 1000, 500, 0), HopWindow.class);
    assertWindowRoundTrip(new VariationWindow("value", 0.5), VariationWindow.class);
    assertWindowRoundTrip(new CapacityWindow(10, Arrays.asList("a", "b")), CapacityWindow.class);
    assertWindowRoundTrip(new AsofWindow(AsofWindow.AfterMatchMode.CLEAR), AsofWindow.class);
  }

  private void assertWindowRoundTrip(
      final StreamWindow window, final Class<? extends StreamWindow> expectedClass)
      throws Exception {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final DataOutputStream dos = new DataOutputStream(baos);
    window.serialize(dos);
    dos.flush();
    Assert.assertEquals(
        expectedClass, StreamWindow.deserialize(ByteBuffer.wrap(baos.toByteArray())).getClass());
  }

  private StreamTask buildTask(final String taskName) {
    final StreamTask task = new StreamTask();
    task.setId(12L);
    task.setTaskName(taskName);
    task.setCreator("creator");
    task.setCreationTime(123L);
    task.setSubQuery("select count(*) from table1");
    task.setDatabase("db");
    task.setTypeProvider(new StreamNodeTableTypeProvider(Collections.emptyMap()));
    task.setCalcPlan(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    task.setSource(
        new IoTDBSubscriptionSource(
            "db",
            "table1",
            new Identifier("status"),
            Collections.singletonList("status"),
            "127.0.0.1",
            6667,
            "root",
            "root"));
    task.setWindow(new PeriodWindow(1000L, 10L));
    task.setTarget(new IoTDBTarget("target_db", "target_table", Arrays.asList("s1", "s2")));
    task.setProperties(
        new StreamProperties(5L, -1L, false, null, -1L, StreamProperties.EventType.WINDOW_CLOSE));
    return task;
  }
}
