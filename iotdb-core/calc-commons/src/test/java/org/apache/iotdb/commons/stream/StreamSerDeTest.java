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

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static org.apache.tsfile.enums.TSDataType.INT64;

public class StreamSerDeTest {

  @Test
  public void streamTaskEpochAdvancesMonotonicallyAndChecksOverflow() {
    final StreamTask task = new StreamTask();
    task.setEpoch(5);

    task.advanceEpochToAtLeast(3);
    Assert.assertEquals(5, task.getEpoch());
    task.advanceEpochToAtLeast(8);
    Assert.assertEquals(8, task.getEpoch());
    Assert.assertEquals(9, StreamTask.incrementEpochChecked(task.getEpoch()));
    Assert.assertThrows(
        ArithmeticException.class, () -> StreamTask.incrementEpochChecked(Integer.MAX_VALUE));
  }

  @Test
  public void streamWindowTest() throws IOException {
    assertStreamWindowRoundTrip(new PeriodWindow(0, 1000L, 42L));
    assertStreamWindowRoundTrip(new TumbleWindow(5000L, 7L));
    assertStreamWindowRoundTrip(new TumbleWindow(3000L, 0L));
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
    IoTDBSubscriptionSource src =
        new IoTDBSubscriptionSource(
            "db1",
            "t1",
            new Identifier("f"),
            Arrays.asList("p1", "p2"),
            "127.0.0.1",
            6667,
            "root",
            "root");
    // subscribeColumns is NOT serialized with the source; it is re-derived after the StreamTask
    // is deserialized (see StreamTask#bindSubscribeColumnsToSource).
    src.setSubscribeColumns(Arrays.asList("start_time", "row_num"));
    IoTDBSubscriptionSource copy =
        (IoTDBSubscriptionSource)
            StreamSource.deserialize(writeSingleSourceBuffer(src).duplicate());
    Assert.assertEquals(src.getDatabase(), copy.getDatabase());
    Assert.assertEquals(src.getTableName(), copy.getTableName());
    Assert.assertEquals(src.getPartitionColumns(), copy.getPartitionColumns());
    // subscribeColumns is intentionally not carried through source serialization.
    Assert.assertNull(copy.getSubscribeColumns());
    Assert.assertEquals(src.getHost(), copy.getHost());
    Assert.assertEquals(src.getRpcPort(), copy.getRpcPort());
    Assert.assertEquals(src.getUser(), copy.getUser());
    Assert.assertEquals(src.getEncryptedPassword(), copy.getEncryptedPassword());
    Assert.assertEquals(
        "IoTDBSubscriptionSource{database='db1', tableName='t1', preFilter=f, partitionColumns=[p1, p2], subscribeColumns=[start_time, row_num], host='127.0.0.1', rpcPort=6667, user='root'}",
        src.toString());
    Assert.assertNotNull(copy.getPreFilter());
    Assert.assertTrue(copy.getPreFilter() instanceof Identifier);
    Assert.assertEquals(
        ((Identifier) src.getPreFilter()).getValue(),
        ((Identifier) copy.getPreFilter()).getValue());

    IoTDBSubscriptionSource nullFilterSource =
        new IoTDBSubscriptionSource(
            "db1", "t2", null, Arrays.asList("p3"), "127.0.0.1", 6667, "root", "root");
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
    IoTDBTarget tgt =
        new IoTDBTarget(
            "127.0.0.1:6667", "db2", "sink", Arrays.asList("c1"), null, null, Arrays.asList(0));
    IoTDBTarget copy =
        (IoTDBTarget) StreamTarget.deserialize(writeSingleTargetBuffer(tgt).duplicate());
    Assert.assertEquals(tgt.getDatabase(), copy.getDatabase());
    Assert.assertEquals(tgt.getTableName(), copy.getTableName());
    Assert.assertEquals(tgt.getColumnNames(), copy.getColumnNames());
    Assert.assertEquals(
        tgt.getColumnIndex2TsBlockColumnIndexList(), copy.getColumnIndex2TsBlockColumnIndexList());
    Assert.assertEquals(
        "IoTDBTarget{nodeUrls='127.0.0.1:6667', database='db2', tableName='sink', columnNames=[c1], columnDataTypes=null, columnCategories=null, columnIndex2TsBlockColumnIndexList=[0]}",
        tgt.toString());

    IoTDBTarget nullableCols = new IoTDBTarget("db2", "sink2", null);
    IoTDBTarget copy2 =
        (IoTDBTarget) StreamTarget.deserialize(writeSingleTargetBuffer(nullableCols).duplicate());
    Assert.assertNull(copy2.getColumnNames());

    IoTDBTarget targetWithTypes =
        new IoTDBTarget(
            "127.0.0.1:6667",
            "db3",
            "sink3",
            Arrays.asList("c1", "c2", "c3"),
            Arrays.asList(INT64, TSDataType.TEXT, TSDataType.BOOLEAN),
            Arrays.asList(ColumnCategory.TIME, ColumnCategory.TAG, ColumnCategory.ATTRIBUTE),
            Arrays.asList(0, 1, 2));
    IoTDBTarget copy3 =
        (IoTDBTarget)
            StreamTarget.deserialize(writeSingleTargetBuffer(targetWithTypes).duplicate());
    Assert.assertEquals(targetWithTypes.getDatabase(), copy3.getDatabase());
    Assert.assertEquals(targetWithTypes.getTableName(), copy3.getTableName());
    Assert.assertEquals(targetWithTypes.getColumnNames(), copy3.getColumnNames());
    Assert.assertEquals(targetWithTypes.getColumnDataTypes(), copy3.getColumnDataTypes());
    Assert.assertEquals(targetWithTypes.getColumnCategories(), copy3.getColumnCategories());
    Assert.assertEquals(
        targetWithTypes.getColumnIndex2TsBlockColumnIndexList(),
        copy3.getColumnIndex2TsBlockColumnIndexList());
  }

  /**
   * Verifies all Stream properties, including the subscription WAL retention snapshot, survive
   * serialization.
   */
  @Test
  public void streamPropertiesTest() throws IOException {
    StreamProperties props = new StreamProperties(10L, 50L, 40L, 60L, true);
    props.setSubscriptionConsensusWalRetentionSizeInBytes(1024L);
    props.setSubscriptionConsensusWalRetentionTimeMs(3600L);
    props.setSubscriptionPollThreadCount(3);
    props.setFillHistory(true);
    props.setFillHistoryStartTime(1234L);
    StreamProperties copy = StreamProperties.deserialize(writePropertiesBuffer(props).duplicate());
    Assert.assertEquals(props.getWatermark(), copy.getWatermark());
    Assert.assertEquals(props.getAllowedLateness(), copy.getAllowedLateness());
    Assert.assertEquals(props.getMaxDelayWindowCloseMs(), copy.getMaxDelayWindowCloseMs());
    Assert.assertEquals(props.getMaxIdleWindowRetainMs(), copy.getMaxIdleWindowRetainMs());
    Assert.assertEquals(props.isLeftClosed(), copy.isLeftClosed());
    Assert.assertEquals(
        props.getSubscriptionConsensusWalRetentionSizeInBytes(),
        copy.getSubscriptionConsensusWalRetentionSizeInBytes());
    Assert.assertEquals(
        props.getSubscriptionConsensusWalRetentionTimeMs(),
        copy.getSubscriptionConsensusWalRetentionTimeMs());
    Assert.assertEquals(
        props.getSubscriptionPollThreadCount(), copy.getSubscriptionPollThreadCount());
    Assert.assertEquals(props.isFillHistory(), copy.isFillHistory());
    Assert.assertEquals(props.getFillHistoryStartTime(), copy.getFillHistoryStartTime());

    props.setSubscriptionPollThreadCount(-1);
    copy = StreamProperties.deserialize(writePropertiesBuffer(props).duplicate());
    Assert.assertEquals(
        props.getSubscriptionPollThreadCount(), copy.getSubscriptionPollThreadCount());

    StreamProperties minimal = new StreamProperties(0, 0, 0, 0, false);
    StreamProperties copyMin =
        StreamProperties.deserialize(writePropertiesBuffer(minimal).duplicate());
    Assert.assertEquals(0, copyMin.getWatermark());
    Assert.assertEquals(0, copyMin.getAllowedLateness());
    Assert.assertEquals(0, copyMin.getMaxDelayWindowCloseMs());
    Assert.assertEquals(0, copyMin.getMaxIdleWindowRetainMs());
    Assert.assertNull(copyMin.getSubscriptionConsensusWalRetentionSizeInBytes());
    Assert.assertNull(copyMin.getSubscriptionConsensusWalRetentionTimeMs());
    Assert.assertNull(copyMin.getSubscriptionPollThreadCount());
    Assert.assertFalse(copyMin.isFillHistory());
    Assert.assertNull(copyMin.getFillHistoryStartTime());
  }

  /** Verifies a payload written before WAL retention fields were added still deserializes. */
  @Test
  public void streamPropertiesBackwardCompatibilityTest() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeLong(0L);
    dos.writeLong(0L);
    dos.writeLong(0L);
    dos.writeLong(0L);
    dos.writeBoolean(false);
    dos.flush();

    StreamProperties copy = StreamProperties.deserialize(ByteBuffer.wrap(baos.toByteArray()));
    Assert.assertNull(copy.getSubscriptionConsensusWalRetentionSizeInBytes());
    Assert.assertNull(copy.getSubscriptionConsensusWalRetentionTimeMs());
    Assert.assertNull(copy.getSubscriptionPollThreadCount());
    Assert.assertFalse(copy.isFillHistory());
    Assert.assertNull(copy.getFillHistoryStartTime());
  }

  @Test
  public void streamPropertiesFillHistoryWithoutStartTime() throws IOException {
    StreamProperties props = new StreamProperties();
    props.setFillHistory(true);
    StreamProperties copy = StreamProperties.deserialize(writePropertiesBuffer(props).duplicate());
    Assert.assertTrue(copy.isFillHistory());
    Assert.assertNull(copy.getFillHistoryStartTime());
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
    original.setWindow(new PeriodWindow(0, 1001L, 0L));
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
            "srcDb",
            "srcTbl",
            new Identifier("x"),
            Collections.singletonList("pk"),
            "127.0.0.1",
            6667,
            "root",
            "root");
    StreamProperties props = new StreamProperties(1L, 0, 9L, 0, false);

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

  /** Verifies the distributed-create payload reconstructs type and WAL retention properties. */
  @Test
  public void readFromDistributedCreate() throws IOException {
    IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource(
            "sdb", "st", new Identifier("pf"), null, "127.0.0.1", 6667, "root", "root");
    ByteBuffer streamSource = writeSingleSourceBuffer(source);
    ByteBuffer eventWindow = writeSingleWindowBuffer(new VariationWindow("vcol", 0.25));
    ByteBuffer calcPlan = ByteBuffer.wrap(new byte[] {7, 8});
    IoTDBTarget target = new IoTDBTarget("tdb", "tt", Arrays.asList("c"));
    ByteBuffer streamSink = writeSingleTargetBuffer(target);
    StreamNodeTableTypeProvider typeProvider =
        new StreamNodeTableTypeProvider(
            Collections.singletonMap(new Symbol("count"), TypeFactory.getType(INT64)));
    ByteBuffer typeProviderBuffer = writeTypeProviderBuffer(typeProvider);
    StreamProperties properties = new StreamProperties();
    properties.setSubscriptionConsensusWalRetentionSizeInBytes(2048L);
    properties.setSubscriptionConsensusWalRetentionTimeMs(-1L);
    properties.setSubscriptionPollThreadCount(3);
    ByteBuffer propertiesBuffer = writePropertiesBuffer(properties);

    StreamTask task =
        StreamTask.readFromDistributedCreate(
            "my.stream",
            "alice",
            streamSource.duplicate(),
            eventWindow.duplicate(),
            "SELECT * FROM t",
            calcPlan.duplicate(),
            streamSink.duplicate(),
            typeProviderBuffer.duplicate(),
            propertiesBuffer.duplicate());

    Assert.assertEquals("my.stream", task.getTaskName());
    Assert.assertEquals("alice", task.getCreator());
    Assert.assertEquals("SELECT * FROM t", task.getSubQuery());
    assertCalcPlanBytesEqual(calcPlan.duplicate(), task.getCalcPlan());
    Assert.assertEquals(
        typeProvider.allTableModelTypes(), task.getTypeProvider().allTableModelTypes());
    Assert.assertEquals(
        properties.getSubscriptionConsensusWalRetentionSizeInBytes(),
        task.getProperties().getSubscriptionConsensusWalRetentionSizeInBytes());
    Assert.assertEquals(
        properties.getSubscriptionConsensusWalRetentionTimeMs(),
        task.getProperties().getSubscriptionConsensusWalRetentionTimeMs());
    Assert.assertEquals(
        properties.getSubscriptionPollThreadCount(),
        task.getProperties().getSubscriptionPollThreadCount());
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
    ByteBuffer eventWindow = writeSingleWindowBuffer(new PeriodWindow(0, 1L, 0L));
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
      Assert.assertEquals(e.getSize(), a.getSize());
      Assert.assertEquals(e.getOrigin(), a.getOrigin());
    } else if (expected instanceof HopWindow) {
      HopWindow e = (HopWindow) expected;
      HopWindow a = (HopWindow) actual;
      Assert.assertEquals(e.getTimeColumn(), a.getTimeColumn());
      Assert.assertEquals(e.getSize(), a.getSize());
      Assert.assertEquals(e.getSlide(), a.getSlide());
      Assert.assertEquals(e.getOrigin(), a.getOrigin());
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
