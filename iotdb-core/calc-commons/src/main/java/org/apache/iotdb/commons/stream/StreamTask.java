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

import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;

import org.apache.tsfile.utils.PublicBAOS;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

public class StreamTask {

  private long id;
  private String taskName;
  private String database;
  private long creationTime;
  private String creator;
  private StreamSource source;
  private StreamWindow window;
  private String subQuery;

  private StreamNodeTableTypeProvider typeProvider;
  private ByteBuffer calcPlan;

  private StreamTarget target;
  private StreamTaskStatus status;
  private String runningOn;
  private int epoch;
  private long leaderTerm;
  private long cnStartTime;
  private long lastUpTime;
  private long lastDownTime;
  private long lastHeartbeatTime;
  private String lastDownReason;
  private StreamProperties properties;

  public StreamTask() {}

  public StreamTask(
      long id,
      String taskName,
      String database,
      long creationTime,
      String creator,
      StreamSource source,
      StreamWindow window,
      String subQuery,
      StreamTarget target,
      StreamTaskStatus status,
      String runningOn,
      int epoch,
      StreamProperties properties,
      StreamNodeTableTypeProvider streamNodeTableTypeProvider,
      ByteBuffer calcPlan) {
    this.id = id;
    this.taskName = taskName;
    this.database = database;
    this.creationTime = creationTime;
    this.creator = creator;
    this.source = source;
    this.window = window;
    this.subQuery = subQuery;
    this.typeProvider = streamNodeTableTypeProvider;
    setCalcPlan(calcPlan);
    this.target = target;
    this.status = status;
    this.runningOn = runningOn;
    this.epoch = epoch;
    this.properties = properties;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(long creationTime) {
    this.creationTime = creationTime;
  }

  public String getCreator() {
    return creator;
  }

  public void setCreator(String creator) {
    this.creator = creator;
  }

  public StreamSource getSource() {
    return source;
  }

  public void setSource(StreamSource source) {
    this.source = source;
  }

  public StreamWindow getWindow() {
    return window;
  }

  public void setWindow(StreamWindow window) {
    this.window = window;
  }

  public String getSubQuery() {
    return subQuery;
  }

  public void setSubQuery(String subQuery) {
    this.subQuery = subQuery;
  }

  public StreamNodeTableTypeProvider getTypeProvider() {
    return typeProvider;
  }

  public void setTypeProvider(StreamNodeTableTypeProvider typeProvider) {
    this.typeProvider = typeProvider;
  }

  public ByteBuffer getCalcPlan() {
    return calcPlan == null ? ByteBuffer.allocate(0) : calcPlan.duplicate();
  }

  public void setCalcPlan(ByteBuffer calcPlan) {
    this.calcPlan = calcPlan == null ? ByteBuffer.allocate(0) : calcPlan.duplicate();
  }

  public StreamTarget getTarget() {
    return target;
  }

  public void setTarget(StreamTarget target) {
    this.target = target;
  }

  public StreamTaskStatus getStatus() {
    return status;
  }

  public void setStatus(StreamTaskStatus status) {
    this.status = status;
  }

  public String getRunningOn() {
    return runningOn;
  }

  public void setRunningOn(String runningOn) {
    this.runningOn = runningOn;
  }

  public int getEpoch() {
    return epoch;
  }

  public void setEpoch(int epoch) {
    this.epoch = epoch;
  }

  public StreamProperties getProperties() {
    return properties;
  }

  public void setProperties(StreamProperties properties) {
    this.properties = properties;
  }

  public long getLastUpTime() {
    return lastUpTime;
  }

  public void setLastUpTime(long lastUpTime) {
    this.lastUpTime = lastUpTime;
  }

  public long getLastDownTime() {
    return lastDownTime;
  }

  public void setLastDownTime(long lastDownTime) {
    this.lastDownTime = lastDownTime;
  }

  public String getLastDownReason() {
    return lastDownReason;
  }

  public void setLastDownReason(String lastDownReason) {
    this.lastDownReason = lastDownReason;
  }

  public long getLeaderTerm() {
    return leaderTerm;
  }

  public void setLeaderTerm(long leaderTerm) {
    this.leaderTerm = leaderTerm;
  }

  public long getCnStartTime() {
    return cnStartTime;
  }

  public void setCnStartTime(long cnStartTime) {
    this.cnStartTime = cnStartTime;
  }

  public long getLastHeartbeatTime() {
    return lastHeartbeatTime;
  }

  public void setLastHeartbeatTime(long lastHeartbeatTime) {
    this.lastHeartbeatTime = lastHeartbeatTime;
  }

  public ByteBuffer toByteBuffer() {
    try (PublicBAOS baos = new PublicBAOS();
        DataOutputStream dos = new DataOutputStream(baos)) {
      serialize(dos);
      return ByteBuffer.wrap(baos.getBuf(), 0, baos.size());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeLong(id);
    BasicStructureSerDeUtil.write(taskName, stream);
    BasicStructureSerDeUtil.write(database, stream);
    stream.writeLong(creationTime);
    BasicStructureSerDeUtil.write(creator, stream);
    if (source == null) {
      stream.writeBoolean(false);
    } else {
      stream.writeBoolean(true);
      source.serialize(stream);
    }
    if (window == null) {
      throw new IOException("stream window is required for serialization");
    }
    window.serialize(stream);
    BasicStructureSerDeUtil.write(subQuery, stream);

    getOrCreateTypeProvider().serialize(stream);
    writeCalcPlan(stream, calcPlan);
    if (target == null) {
      throw new IOException("stream target is required for serialization");
    }
    target.serialize(stream);
    if (status == null) {
      stream.writeShort(-1);
    } else {
      stream.writeShort(status.ordinal());
    }
    BasicStructureSerDeUtil.write(runningOn, stream);
    stream.writeInt(epoch);
    if (properties == null) {
      stream.writeBoolean(false);
    } else {
      stream.writeBoolean(true);
      properties.serialize(stream);
    }
  }

  public static StreamTask deserialize(ByteBuffer byteBuffer) throws IOException {
    StreamTask task = new StreamTask();
    task.setId(byteBuffer.getLong());
    String taskNameDes = BasicStructureSerDeUtil.readString(byteBuffer);
    if (taskNameDes == null) {
      throw new IOException("unexpected null task name in stream task deserialization");
    }
    task.setTaskName(taskNameDes);
    task.setDatabase(BasicStructureSerDeUtil.readString(byteBuffer));
    task.setCreationTime(byteBuffer.getLong());
    task.setCreator(BasicStructureSerDeUtil.readString(byteBuffer));
    if (byteBuffer.get() != 0) {
      task.setSource(StreamSource.deserialize(byteBuffer));
    }
    task.setWindow(StreamWindow.deserialize(byteBuffer));
    task.setSubQuery(BasicStructureSerDeUtil.readString(byteBuffer));
    task.setTypeProvider(StreamNodeTableTypeProvider.deserialize(byteBuffer));
    task.setCalcPlan(readCalcPlan(byteBuffer));
    task.setTarget(StreamTarget.deserialize(byteBuffer));
    short statusOrdinal = byteBuffer.getShort();
    if (statusOrdinal >= 0 && statusOrdinal < StreamTaskStatus.values().length) {
      task.setStatus(StreamTaskStatus.values()[statusOrdinal]);
    }

    task.setRunningOn(BasicStructureSerDeUtil.readString(byteBuffer));
    task.setEpoch(byteBuffer.getInt());
    if (byteBuffer.get() != 0) {
      task.setProperties(StreamProperties.deserialize(byteBuffer));
    }
    return task;
  }

  /**
   * Assembles a {@link StreamTask} from the DN→CN {@code createStream} RPC payload (see {@code
   * TCreateStreamReq}).
   */
  public static StreamTask readFromDistributedCreate(
      String streamName,
      String creator,
      ByteBuffer streamSource,
      ByteBuffer eventWindow,
      String calcSql,
      ByteBuffer calcPlan,
      ByteBuffer streamSink)
      throws IOException {
    return readFromDistributedCreate(
        streamName, creator, streamSource, eventWindow, calcSql, calcPlan, streamSink, null);
  }

  public static StreamTask readFromDistributedCreate(
      String streamName,
      String creator,
      ByteBuffer streamSource,
      ByteBuffer eventWindow,
      String calcSql,
      ByteBuffer calcPlan,
      ByteBuffer streamSink,
      ByteBuffer typeProvider)
      throws IOException {
    StreamTask task = new StreamTask();
    task.setTaskName(streamName);
    task.setCreator(creator);
    task.setSubQuery(calcSql);
    task.setCalcPlan(calcPlan);
    if (typeProvider != null && typeProvider.hasRemaining()) {
      task.setTypeProvider(StreamNodeTableTypeProvider.deserialize(typeProvider.duplicate()));
    } else {
      task.setTypeProvider(new StreamNodeTableTypeProvider(Collections.emptyMap()));
    }
    if (streamSource != null && streamSource.hasRemaining()) {
      task.setSource(StreamSource.deserialize(streamSource.duplicate()));
    }
    if (eventWindow == null || !eventWindow.hasRemaining()) {
      throw new IOException("eventWindow is required");
    }
    task.setWindow(StreamWindow.deserialize(eventWindow.duplicate()));
    if (streamSink == null || !streamSink.hasRemaining()) {
      throw new IOException("streamSink is required");
    }
    task.setTarget(StreamTarget.deserialize(streamSink.duplicate()));
    return task;
  }

  private StreamNodeTableTypeProvider getOrCreateTypeProvider() {
    if (typeProvider == null) {
      typeProvider = new StreamNodeTableTypeProvider(Collections.emptyMap());
    }
    return typeProvider;
  }

  private static void writeCalcPlan(DataOutputStream stream, ByteBuffer calcPlan)
      throws IOException {
    ByteBuffer calcPlanToWrite = calcPlan == null ? ByteBuffer.allocate(0) : calcPlan.duplicate();
    int len = calcPlanToWrite.remaining();
    stream.writeInt(len);
    if (len > 0) {
      byte[] chunk = new byte[len];
      calcPlanToWrite.get(chunk);
      stream.write(chunk);
    }
  }

  private static ByteBuffer readCalcPlan(ByteBuffer buf) throws IOException {
    if (buf.remaining() < Integer.BYTES) {
      throw new IOException("unexpected end of buffer");
    }
    int len = buf.getInt();
    if (len < 0) {
      throw new IOException("invalid calcPlan length: " + len);
    }
    if (len == 0) {
      return ByteBuffer.allocate(0);
    }
    if (buf.remaining() < len) {
      throw new IOException("unexpected end of buffer reading calcPlan payload");
    }
    byte[] b = new byte[len];
    buf.get(b);
    return ByteBuffer.wrap(b);
  }
}
