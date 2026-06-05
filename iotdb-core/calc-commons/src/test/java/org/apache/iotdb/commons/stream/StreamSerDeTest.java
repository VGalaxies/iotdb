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

package org.apache.iotdb.commons.stream;

import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;

import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.tsfile.enums.TSDataType.INT64;

public class StreamSerDeTest {

  @Test
  public void streamWindowTest() throws IOException {
    assertStreamWindowRoundTrip(new PeriodWindow(1000L, 42L));
    assertStreamWindowRoundTrip(new TumbleWindow("time", 5000L, 7L));
    assertStreamWindowRoundTrip(new TumbleWindow(null, 3000L, 0L));
    assertStreamWindowRoundTrip(new HopWindow("ts", 2000L, 1000L, 3L));
    assertStreamWindowRoundTrip(new VariationWindow("col", 1.5));
    assertStreamWindowRoundTrip(new CapacityWindow(100, Arrays.asList("a", "b")));
    assertStreamWindowRoundTrip(new CapacityWindow(50, null));
    assertStreamWindowRoundTrip(new AsofWindow(AsofWindow.AfterMatchMode.KEEP));
    assertStreamWindowRoundTrip(new AsofWindow(AsofWindow.AfterMatchMode.CLEAR));
  }

  @Test(expected = IOException.class)
  public void streamWindowDeserializeRejectsUnknownOrdinal() throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(4);
    buf.putInt(StreamWindowType.values().length + 10);
    buf.flip();
    StreamWindow.deserialize(buf);
  }

  @Test
  public void streamSourceTest() throws IOException {
    List<String> outputFields = Arrays.asList("start_time", "row_num");
    List<Type> fieldTypes = Arrays.asList(TypeFactory.getType(INT64), TypeFactory.getType(INT64));
    IoTDBSubscriptionSource src =
        new IoTDBSubscriptionSource(
            "db1", "t1", new Identifier("f"), Arrays.asList("p1", "p2"), outputFields, fieldTypes);
    IoTDBSubscriptionSource copy =
        (IoTDBSubscriptionSource)
            StreamSource.deserialize(writeSingleSourceBuffer(src).duplicate());
    Assert.assertEquals(src.getDatabase(), copy.getDatabase());
    Assert.assertEquals(src.getTableName(), copy.getTableName());
    Assert.assertEquals(src.getPartitionColumns(), copy.getPartitionColumns());
    Assert.assertEquals(src.getOutputFields(), copy.getOutputFields());
    Assert.assertEquals(src.getFieldTypes(), copy.getFieldTypes());
    Assert.assertEquals(
        "IoTDBSubscriptionSource{database='db1', tableName='t1', preFilter=f, partitionColumns=[p1, p2], outputFields=[start_time, row_num], fieldTypes=[INT64, INT64]}",
        src.toString());
    Assert.assertNotNull(copy.getPreFilter());
    Assert.assertTrue(copy.getPreFilter() instanceof Identifier);
    Assert.assertEquals(
        ((Identifier) src.getPreFilter()).getValue(),
        ((Identifier) copy.getPreFilter()).getValue());

    IoTDBSubscriptionSource nullFilterSource =
        new IoTDBSubscriptionSource("db1", "t2", null, Arrays.asList("p3"), null, null);
    IoTDBSubscriptionSource nullFilterCopy =
        (IoTDBSubscriptionSource)
            StreamSource.deserialize(writeSingleSourceBuffer(nullFilterSource).duplicate());
    Assert.assertEquals(nullFilterSource.getDatabase(), nullFilterCopy.getDatabase());
    Assert.assertEquals(nullFilterSource.getTableName(), nullFilterCopy.getTableName());
    Assert.assertNull(nullFilterCopy.getPreFilter());
    Assert.assertEquals(
        nullFilterSource.getPartitionColumns(), nullFilterCopy.getPartitionColumns());
  }

  @Test
  public void streamTargetTest() throws IOException {
    IoTDBTarget tgt = new IoTDBTarget("db2", "sink", Arrays.asList("c1"));
    IoTDBTarget copy =
        (IoTDBTarget) StreamTarget.deserialize(writeSingleTargetBuffer(tgt).duplicate());
    Assert.assertEquals(tgt.getDatabase(), copy.getDatabase());
    Assert.assertEquals(tgt.getTableName(), copy.getTableName());
    Assert.assertEquals(tgt.getColumnNames(), copy.getColumnNames());
    Assert.assertEquals(
        "IoTDBTarget{nodeUrls='127.0.0.1:6667', database='db2', tableName='sink', columnNames=[c1]}",
        tgt.toString());

    IoTDBTarget nullableCols = new IoTDBTarget("db2", "sink2", null);
    IoTDBTarget copy2 =
        (IoTDBTarget) StreamTarget.deserialize(writeSingleTargetBuffer(nullableCols).duplicate());
    Assert.assertNull(copy2.getColumnNames());
  }

  @Test
  public void streamPropertiesTest() throws IOException {
    StreamProperties props =
        new StreamProperties(10L, 20L, true, 30L, 40L, StreamProperties.EventType.WINDOW_CLOSE);
    StreamProperties copy = StreamProperties.deserialize(writePropertiesBuffer(props).duplicate());
    Assert.assertEquals(props.getWatermarkMs(), copy.getWatermarkMs());
    Assert.assertEquals(props.getExpiredTimeMs(), copy.getExpiredTimeMs());
    Assert.assertEquals(props.isIgnoreDisorder(), copy.isIgnoreDisorder());
    Assert.assertEquals(props.getFillHistoryStartTime(), copy.getFillHistoryStartTime());
    Assert.assertEquals(props.getMaxDelayMs(), copy.getMaxDelayMs());
    Assert.assertEquals(props.getEventType(), copy.getEventType());

    StreamProperties minimal = new StreamProperties(null, null, false, null, null, null);
    StreamProperties copyMin =
        StreamProperties.deserialize(writePropertiesBuffer(minimal).duplicate());
    Assert.assertNull(copyMin.getWatermarkMs());
    Assert.assertFalse(copyMin.isIgnoreDisorder());
    Assert.assertNull(copyMin.getEventType());
  }

  @Test
  public void streamTaskTest() throws IOException {
    ByteBuffer calc = ByteBuffer.wrap(new byte[] {0x01, 0x02, 0x03});
    StreamTask original = new StreamTask();
    original.setId(9L);
    original.setTaskName("root.db.s1");
    original.setDatabase("root.db");
    original.setCreationTime(123456789L);
    original.setCreator("user");
    original.setSource(null);
    original.setWindow(new PeriodWindow(1001L, 0L));
    original.setSubQuery("SELECT 1");
    original.setTypeProvider(new StreamNodeTableTypeProvider(Collections.emptyMap()));
    original.setCalcPlan(calc.duplicate());
    original.setTarget(new IoTDBTarget("root.db", "out", null));
    original.setStatus(StreamTaskStatus.RUNNING);
    original.setRunningOn("dn1");
    original.setEpoch(2);
    original.setProperties(null);

    StreamTask restored =
        StreamTask.deserialize(ByteBuffer.wrap(serializeStreamTaskToBytes(original)));

    assertStreamTaskFieldsEqual(original, restored);
  }

  @Test
  public void streamTaskWithSourceAndProperties() throws IOException {
    ByteBuffer calc = ByteBuffer.wrap(new byte[] {(byte) 0xff});
    IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource(
            "srcDb", "srcTbl", new Identifier("x"), Collections.singletonList("pk"), null, null);
    StreamProperties props =
        new StreamProperties(1L, 2L, false, null, 9L, StreamProperties.EventType.WINDOW_OPEN);

    StreamTask original = new StreamTask();
    original.setId(1L);
    original.setTaskName("t");
    original.setDatabase("d");
    original.setCreationTime(0L);
    original.setCreator("c");
    original.setSource(source);
    original.setWindow(new HopWindow("time", 50L, 25L, 0L));
    original.setSubQuery("sql");
    original.setTypeProvider(new StreamNodeTableTypeProvider(Collections.emptyMap()));
    original.setCalcPlan(calc.duplicate());
    original.setTarget(new IoTDBTarget("d", "tgt", Collections.emptyList()));
    original.setStatus(null);
    original.setRunningOn(null);
    original.setEpoch(0);
    original.setProperties(props);

    StreamTask restored =
        StreamTask.deserialize(ByteBuffer.wrap(serializeStreamTaskToBytes(original)));

    assertStreamTaskFieldsEqual(original, restored);
  }

  @Test
  public void readFromDistributedCreate() throws IOException {
    IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource("sdb", "st", new Identifier("pf"), null, null, null);
    ByteBuffer streamSource = writeSingleSourceBuffer(source);
    ByteBuffer eventWindow = writeSingleWindowBuffer(new VariationWindow("vcol", 0.25));
    ByteBuffer calcPlan = ByteBuffer.wrap(new byte[] {7, 8});
    IoTDBTarget target = new IoTDBTarget("tdb", "tt", Arrays.asList("c"));
    ByteBuffer streamSink = writeSingleTargetBuffer(target);
    StreamNodeTableTypeProvider typeProvider =
        new StreamNodeTableTypeProvider(
            Collections.singletonMap(new Symbol("count"), TypeFactory.getType(INT64)));
    ByteBuffer typeProviderBuffer = writeTypeProviderBuffer(typeProvider);

    StreamTask task =
        StreamTask.readFromDistributedCreate(
            "my.stream",
            "alice",
            streamSource.duplicate(),
            eventWindow.duplicate(),
            "SELECT * FROM t",
            calcPlan.duplicate(),
            streamSink.duplicate(),
            typeProviderBuffer.duplicate());

    Assert.assertEquals("my.stream", task.getTaskName());
    Assert.assertEquals("alice", task.getCreator());
    Assert.assertEquals("SELECT * FROM t", task.getSubQuery());
    assertCalcPlanBytesEqual(calcPlan.duplicate(), task.getCalcPlan());
    Assert.assertEquals(
        typeProvider.allTableModelTypes(), task.getTypeProvider().allTableModelTypes());
    Assert.assertTrue(task.toByteBuffer().hasRemaining());

    IoTDBSubscriptionSource src2 =
        (IoTDBSubscriptionSource) Objects.requireNonNull(task.getSource());
    Assert.assertEquals("sdb", src2.getDatabase());
    Assert.assertEquals("st", src2.getTableName());

    VariationWindow w = (VariationWindow) task.getWindow();
    Assert.assertEquals("vcol", w.getColumn());
    Assert.assertEquals(0.25, w.getDelta(), 0.0);

    IoTDBTarget tgt2 = (IoTDBTarget) task.getTarget();
    Assert.assertEquals("tdb", tgt2.getDatabase());
    Assert.assertEquals("tt", tgt2.getTableName());
    Assert.assertEquals(Arrays.asList("c"), tgt2.getColumnNames());
  }

  @Test
  public void readFromDistributedCreateWithoutTypeProviderCanSerialize() throws IOException {
    ByteBuffer eventWindow = writeSingleWindowBuffer(new PeriodWindow(1L, 0L));
    ByteBuffer calcPlan = ByteBuffer.wrap(new byte[] {1});
    ByteBuffer streamSink = writeSingleTargetBuffer(new IoTDBTarget("tdb", "tt", null));

    StreamTask task =
        StreamTask.readFromDistributedCreate(
            "legacy.stream",
            "alice",
            null,
            eventWindow.duplicate(),
            "SELECT 1",
            calcPlan.duplicate(),
            streamSink.duplicate());

    Assert.assertTrue(task.getTypeProvider().allTableModelTypes().isEmpty());
    Assert.assertTrue(task.toByteBuffer().hasRemaining());
  }

  private static void assertStreamWindowRoundTrip(StreamWindow window) throws IOException {
    ByteBuffer buf = writeSingleWindowBuffer(window).duplicate();
    StreamWindow copy = StreamWindow.deserialize(buf);
    assertStreamWindowEquals(window, copy);
  }

  private static ByteBuffer writeSingleWindowBuffer(StreamWindow window) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    window.serialize(dos);
    dos.flush();
    return ByteBuffer.wrap(baos.toByteArray());
  }

  private static ByteBuffer writeSingleSourceBuffer(StreamSource source) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    source.serialize(dos);
    dos.flush();
    return ByteBuffer.wrap(baos.toByteArray());
  }

  private static ByteBuffer writeSingleTargetBuffer(StreamTarget target) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    target.serialize(dos);
    dos.flush();
    return ByteBuffer.wrap(baos.toByteArray());
  }

  private static ByteBuffer writePropertiesBuffer(StreamProperties props) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    props.serialize(dos);
    dos.flush();
    return ByteBuffer.wrap(baos.toByteArray());
  }

  private static ByteBuffer writeTypeProviderBuffer(StreamNodeTableTypeProvider typeProvider)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    typeProvider.serialize(dos);
    dos.flush();
    return ByteBuffer.wrap(baos.toByteArray());
  }

  private static byte[] serializeStreamTaskToBytes(StreamTask task) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    task.serialize(dos);
    dos.flush();
    return baos.toByteArray();
  }

  private static void assertStreamTaskFieldsEqual(StreamTask expected, StreamTask actual)
      throws IOException {
    Assert.assertEquals(expected.getId(), actual.getId());
    Assert.assertEquals(expected.getTaskName(), actual.getTaskName());
    Assert.assertEquals(expected.getDatabase(), actual.getDatabase());
    Assert.assertEquals(expected.getCreationTime(), actual.getCreationTime());
    Assert.assertEquals(expected.getCreator(), actual.getCreator());
    Assert.assertEquals(expected.getSubQuery(), actual.getSubQuery());
    Assert.assertEquals(expected.getEpoch(), actual.getEpoch());
    Assert.assertEquals(expected.getRunningOn(), actual.getRunningOn());
    Assert.assertEquals(expected.getStatus(), actual.getStatus());
    Assert.assertEquals(
        expected.getTypeProvider().allTableModelTypes(),
        actual.getTypeProvider().allTableModelTypes());

    assertCalcPlanBytesEqual(expected.getCalcPlan(), actual.getCalcPlan());

    if (expected.getSource() == null) {
      Assert.assertNull(actual.getSource());
    } else {
      ByteBuffer es = writeSingleSourceBuffer(expected.getSource());
      ByteBuffer as = writeSingleSourceBuffer(Objects.requireNonNull(actual.getSource()));
      Assert.assertArrayEquals(toByteArray(es.duplicate()), toByteArray(as.duplicate()));
    }

    ByteBuffer ew = writeSingleWindowBuffer(expected.getWindow());
    ByteBuffer aw = writeSingleWindowBuffer(actual.getWindow());
    Assert.assertArrayEquals(toByteArray(ew.duplicate()), toByteArray(aw.duplicate()));

    ByteBuffer et = writeSingleTargetBuffer(expected.getTarget());
    ByteBuffer at = writeSingleTargetBuffer(actual.getTarget());
    Assert.assertArrayEquals(toByteArray(et.duplicate()), toByteArray(at.duplicate()));

    if (expected.getProperties() == null) {
      Assert.assertNull(actual.getProperties());
    } else {
      ByteBuffer ep = writePropertiesBuffer(expected.getProperties());
      ByteBuffer ap = writePropertiesBuffer(Objects.requireNonNull(actual.getProperties()));
      Assert.assertArrayEquals(toByteArray(ep.duplicate()), toByteArray(ap.duplicate()));
    }
  }

  private static void assertCalcPlanBytesEqual(ByteBuffer expected, ByteBuffer actual) {
    // Serialize consumes the task's calcPlan buffer position; rewind for comparison.
    ByteBuffer e = expected.duplicate();
    ByteBuffer a = actual.duplicate();
    e.rewind();
    a.rewind();
    Assert.assertEquals(e.remaining(), a.remaining());
    byte[] eb = new byte[e.remaining()];
    byte[] ab = new byte[a.remaining()];
    e.get(eb);
    a.get(ab);
    Assert.assertArrayEquals(eb, ab);
  }

  private static byte[] toByteArray(ByteBuffer buf) {
    byte[] b = new byte[buf.remaining()];
    buf.get(b);
    return b;
  }

  private static void assertStreamWindowEquals(StreamWindow expected, StreamWindow actual) {
    Assert.assertEquals(expected.getType(), actual.getType());
    if (expected instanceof PeriodWindow) {
      PeriodWindow e = (PeriodWindow) expected;
      PeriodWindow a = (PeriodWindow) actual;
      Assert.assertEquals(e.getPeriodMs(), a.getPeriodMs());
      Assert.assertEquals(e.getOriginMs(), a.getOriginMs());
    } else if (expected instanceof TumbleWindow) {
      TumbleWindow e = (TumbleWindow) expected;
      TumbleWindow a = (TumbleWindow) actual;
      Assert.assertEquals(e.getTimeColumn(), a.getTimeColumn());
      Assert.assertEquals(e.getSizeMs(), a.getSizeMs());
      Assert.assertEquals(e.getOriginMs(), a.getOriginMs());
    } else if (expected instanceof HopWindow) {
      HopWindow e = (HopWindow) expected;
      HopWindow a = (HopWindow) actual;
      Assert.assertEquals(e.getTimeColumn(), a.getTimeColumn());
      Assert.assertEquals(e.getSizeMs(), a.getSizeMs());
      Assert.assertEquals(e.getSlideMs(), a.getSlideMs());
      Assert.assertEquals(e.getOriginMs(), a.getOriginMs());
    } else if (expected instanceof VariationWindow) {
      VariationWindow e = (VariationWindow) expected;
      VariationWindow a = (VariationWindow) actual;
      Assert.assertEquals(e.getColumn(), a.getColumn());
      Assert.assertEquals(e.getDelta(), a.getDelta(), 0.0);
    } else if (expected instanceof CapacityWindow) {
      CapacityWindow e = (CapacityWindow) expected;
      CapacityWindow a = (CapacityWindow) actual;
      Assert.assertEquals(e.getCapacity(), a.getCapacity());
      Assert.assertEquals(e.getColumns(), a.getColumns());
    } else if (expected instanceof AsofWindow) {
      AsofWindow e = (AsofWindow) expected;
      AsofWindow a = (AsofWindow) actual;
      Assert.assertEquals(e.getAfterMatchMode(), a.getAfterMatchMode());
    } else {
      Assert.fail("unknown window type: " + expected.getClass());
    }
  }
}
