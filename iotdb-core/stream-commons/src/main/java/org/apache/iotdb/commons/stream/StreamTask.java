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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamTask {

  private long id;
  private String taskName;
  private String database;
  private long creationTime;
  private String creator;
  private StreamSource source;
  private StreamWindow window;
  private String subQuery;
  private StreamTarget target;
  private StreamTaskStatus status;
  private String runningOn;
  private int epoch;
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
      StreamProperties properties) {
    this.id = id;
    this.taskName = taskName;
    this.database = database;
    this.creationTime = creationTime;
    this.creator = creator;
    this.source = source;
    this.window = window;
    this.subQuery = subQuery;
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

  public void serialize(OutputStream outputStream) throws IOException {
    // TODO: implement serialization
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public static StreamTask deserialize(InputStream inputStream) throws IOException {
    // TODO: implement deserialization
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
