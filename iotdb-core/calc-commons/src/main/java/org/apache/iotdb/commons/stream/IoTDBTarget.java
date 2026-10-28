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
import org.apache.iotdb.commons.utils.TestOnly;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class IoTDBTarget extends StreamTarget {

  private final String nodeUrls;
  private final String database;
  private final String tableName;
  private final List<String> columnNames;
  private final List<TSDataType> columnDataTypes;
  private final List<ColumnCategory> columnCategories;
  private final List<Integer> columnIndex2TsBlockColumnIndexList;

  @TestOnly
  public IoTDBTarget(String database, String tableName, List<String> columnNames) {
    this("127.0.0.1:6667", database, tableName, columnNames, null, null, null);
  }

  public IoTDBTarget(
      String nodeUrls,
      String database,
      String tableName,
      List<String> columnNames,
      List<TSDataType> columnDataTypes,
      List<ColumnCategory> columnCategories,
      List<Integer> columnIndex2TsBlockColumnIndexList) {
    this.nodeUrls = nodeUrls;
    this.database = database;
    this.tableName = tableName;
    this.columnNames = columnNames;
    this.columnDataTypes = columnDataTypes;
    this.columnCategories = columnCategories;
    this.columnIndex2TsBlockColumnIndexList = columnIndex2TsBlockColumnIndexList;
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

  public List<Integer> getColumnIndex2TsBlockColumnIndexList() {
    return columnIndex2TsBlockColumnIndexList;
  }

  /**
   * Returns a copy of this target with the node URLs replaced.
   *
   * <p>Used by the StreamNode to inject DataNode endpoints resolved from the cluster at sink-start
   * time, overriding the placeholder value baked in at CREATE STREAM time. All other target
   * metadata (database, table, column schema) is preserved.
   */
  public IoTDBTarget withNodeUrls(String resolvedNodeUrls) {
    return new IoTDBTarget(
        resolvedNodeUrls,
        database,
        tableName,
        columnNames,
        columnDataTypes,
        columnCategories,
        columnIndex2TsBlockColumnIndexList);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().ordinal(), stream);
    BasicStructureSerDeUtil.write(nodeUrls, stream);
    BasicStructureSerDeUtil.write(database, stream);
    BasicStructureSerDeUtil.write(tableName, stream);
    BasicStructureSerDeUtil.writeNullableStringList(columnNames, stream);

    if (columnDataTypes == null) {
      BasicStructureSerDeUtil.write(-1, stream);
    } else {
      BasicStructureSerDeUtil.write(columnDataTypes.size(), stream);
      for (TSDataType dataType : columnDataTypes) {
        ReadWriteIOUtils.write(dataType, stream);
      }
    }

    if (columnCategories == null) {
      BasicStructureSerDeUtil.write(-1, stream);
    } else {
      BasicStructureSerDeUtil.write(columnCategories.size(), stream);
      for (ColumnCategory category : columnCategories) {
        BasicStructureSerDeUtil.write(category.ordinal(), stream);
      }
    }

    if (columnIndex2TsBlockColumnIndexList == null) {
      BasicStructureSerDeUtil.write(-1, stream);
    } else {
      BasicStructureSerDeUtil.write(columnIndex2TsBlockColumnIndexList.size(), stream);
      for (Integer integer : columnIndex2TsBlockColumnIndexList) {
        BasicStructureSerDeUtil.write(integer, stream);
      }
    }
  }

  public static IoTDBTarget deserialize(ByteBuffer byteBuffer) throws IOException {
    String nodeUrls = ReadWriteIOUtils.readString(byteBuffer);
    String database = BasicStructureSerDeUtil.readString(byteBuffer);
    String tableName = BasicStructureSerDeUtil.readString(byteBuffer);
    List<String> columnNames = BasicStructureSerDeUtil.readStringList(byteBuffer);

    List<TSDataType> columnDataTypes = null;
    int dataTypeSize = byteBuffer.getInt();
    if (dataTypeSize >= 0) {
      columnDataTypes = new ArrayList<>(dataTypeSize);
      for (int i = 0; i < dataTypeSize; i++) {
        columnDataTypes.add(ReadWriteIOUtils.readDataType(byteBuffer));
      }
    }

    List<ColumnCategory> columnCategories = null;
    int categorySize = byteBuffer.getInt();
    if (categorySize >= 0) {
      columnCategories = new ArrayList<>(categorySize);
      for (int i = 0; i < categorySize; i++) {
        int ordinal = BasicStructureSerDeUtil.readInt(byteBuffer);
        columnCategories.add(ColumnCategory.values()[ordinal]);
      }
    }

    List<Integer> columnIndex2TsBlockColumnIndexList = null;
    int size = byteBuffer.getInt();
    if (size >= 0) {
      columnIndex2TsBlockColumnIndexList = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        columnIndex2TsBlockColumnIndexList.add(BasicStructureSerDeUtil.readInt(byteBuffer));
      }
    }
    return new IoTDBTarget(
        nodeUrls,
        database,
        tableName,
        columnNames,
        columnDataTypes,
        columnCategories,
        columnIndex2TsBlockColumnIndexList);
  }

  @Override
  public String toString() {
    return "IoTDBTarget{"
        + "nodeUrls='"
        + nodeUrls
        + '\''
        + ", database='"
        + database
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", columnNames="
        + columnNames
        + ", columnDataTypes="
        + columnDataTypes
        + ", columnCategories="
        + columnCategories
        + ", columnIndex2TsBlockColumnIndexList="
        + columnIndex2TsBlockColumnIndexList
        + '}';
  }
}
