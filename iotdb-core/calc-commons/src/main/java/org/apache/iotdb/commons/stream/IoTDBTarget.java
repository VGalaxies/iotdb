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

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class IoTDBTarget extends StreamTarget {

  private String nodeUrls;
  private String database;
  private String tableName;
  private List<String> columnNames;
  private List<TSDataType> columnDataTypes;
  private List<ColumnCategory> columnCategories;

  public IoTDBTarget(String database, String tableName, List<String> columnNames) {
    this("127.0.0.1:6667", database, tableName, columnNames, null, null);
  }

  public IoTDBTarget(String nodeUrls, String database, String tableName, List<String> columnNames) {
    this(nodeUrls, database, tableName, columnNames, null, null);
  }

  public IoTDBTarget(
      String nodeUrls,
      String database,
      String tableName,
      List<String> columnNames,
      List<TSDataType> columnDataTypes,
      List<ColumnCategory> columnCategories) {
    this.nodeUrls = nodeUrls;
    this.database = database;
    this.tableName = tableName;
    this.columnNames = columnNames;
    this.columnDataTypes = columnDataTypes;
    this.columnCategories = columnCategories;
  }

  @Override
  public StreamTargetType getType() {
    return StreamTargetType.IOTDB_LOCAL;
  }

  public String getNodeUrls() {
    return nodeUrls;
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<TSDataType> getColumnDataTypes() {
    return columnDataTypes;
  }

  public List<ColumnCategory> getColumnCategories() {
    return columnCategories;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().ordinal(), stream);
    BasicStructureSerDeUtil.write(nodeUrls, stream);
    BasicStructureSerDeUtil.write(database, stream);
    BasicStructureSerDeUtil.write(tableName, stream);
    BasicStructureSerDeUtil.writeNullableStringList(columnNames, stream);
  }

  public static IoTDBTarget deserialize(ByteBuffer byteBuffer) throws IOException {
    String nodeUrls = ReadWriteIOUtils.readString(byteBuffer);
    String database = BasicStructureSerDeUtil.readString(byteBuffer);
    String tableName = BasicStructureSerDeUtil.readString(byteBuffer);
    List<String> columnNames = BasicStructureSerDeUtil.readStringList(byteBuffer);
    return new IoTDBTarget(nodeUrls, database, tableName, columnNames);
  }
}
