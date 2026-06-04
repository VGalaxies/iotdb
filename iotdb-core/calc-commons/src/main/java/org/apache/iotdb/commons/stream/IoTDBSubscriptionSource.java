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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;

import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class IoTDBSubscriptionSource extends StreamSource {

  private String database;
  private String tableName;
  @Nullable private Expression preFilter;
  @Nullable private List<String> partitionColumns;
  @Nullable private List<String> outputFields;
  @Nullable private List<Type> fieldTypes;
  private String host = "127.0.0.1";
  private int rpcPort = 6667;
  private String user = "root";
  private String encryptedPassword = "root";

  public IoTDBSubscriptionSource(
      String database,
      String tableName,
      Expression preFilter,
      List<String> partitionColumns,
      @Nullable List<String> outputFields,
      @Nullable List<Type> fieldTypes) {
    this.database = database;
    this.tableName = tableName;
    this.preFilter = preFilter;
    this.partitionColumns = partitionColumns;
    this.outputFields = outputFields;
    this.fieldTypes = fieldTypes;
  }

  public IoTDBSubscriptionSource(
      String database,
      String tableName,
      Expression preFilter,
      List<String> partitionColumns,
      String host,
      int rpcPort,
      String user,
      String encryptedPassword) {
    this(database, tableName, preFilter, partitionColumns, null, null);
    this.host = host;
    this.rpcPort = rpcPort;
    this.user = user;
    this.encryptedPassword = encryptedPassword;
  }

  @Override
  public StreamSourceType getType() {
    return StreamSourceType.IOTDB_SUBSCRIPTION;
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public Expression getPreFilter() {
    return preFilter;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  @Nullable
  public List<String> getOutputFields() {
    return outputFields;
  }

  @Nullable
  public List<Type> getFieldTypes() {
    return fieldTypes;
  }

  public String getHost() {
    return host;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public String getUser() {
    return user;
  }

  public String getEncryptedPassword() {
    return encryptedPassword;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().ordinal(), stream);
    BasicStructureSerDeUtil.write(database, stream);
    BasicStructureSerDeUtil.write(tableName, stream);
    ReadWriteIOUtils.write(preFilter != null, stream);
    if (preFilter != null) {
      Expression.serialize(preFilter, stream);
    }
    BasicStructureSerDeUtil.writeNullableStringList(partitionColumns, stream);
    BasicStructureSerDeUtil.writeNullableStringList(outputFields, stream);
    BasicStructureSerDeUtil.writeNullableTypeList(fieldTypes, stream);
  }

  public static IoTDBSubscriptionSource deserialize(ByteBuffer byteBuffer) throws IOException {
    String database = BasicStructureSerDeUtil.readString(byteBuffer);
    String tableName = BasicStructureSerDeUtil.readString(byteBuffer);
    if (database == null || tableName == null) {
      throw new IOException("unexpected null database or table name in stream source payload");
    }
    Expression preFilter = null;
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      preFilter = Expression.deserialize(byteBuffer);
    }
    List<String> partitionColumns = BasicStructureSerDeUtil.readStringList(byteBuffer);
    List<String> outputFields =
        byteBuffer.hasRemaining() ? BasicStructureSerDeUtil.readStringList(byteBuffer) : null;
    List<Type> fieldTypes =
        byteBuffer.hasRemaining() ? BasicStructureSerDeUtil.readTypeList(byteBuffer) : null;
    return new IoTDBSubscriptionSource(
        database, tableName, preFilter, partitionColumns, outputFields, fieldTypes);
  }
}
