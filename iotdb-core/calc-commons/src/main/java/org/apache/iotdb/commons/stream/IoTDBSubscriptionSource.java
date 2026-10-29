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

import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public class IoTDBSubscriptionSource extends StreamSource {

  private static final String STREAM_TOPIC_PREFIX = "__stream__";

  private final String database;
  private final String tableName;
  @Nullable private final Expression preFilter;
  @Nullable private final List<String> partitionColumns;
  // Columns to subscribe from the source table = event input columns ∪ partition columns.
  // Used by the subscription source to build the column whitelist filter. This field is NOT
  // serialized: it is re-derived after the StreamTask is deserialized (see
  // StreamTask#bindSubscribeColumnsToSource).
  @Nullable private List<String> subscribeColumns;
  private final String host;
  private final int rpcPort;
  private final String user;
  private final String encryptedPassword;
  // Derived from the enclosing StreamTask's epoch; it is not serialized separately.
  private long ownerEpoch = -1L;

  public IoTDBSubscriptionSource(
      String database,
      String tableName,
      @Nullable Expression preFilter,
      @Nullable List<String> partitionColumns,
      String host,
      int rpcPort,
      String user,
      String encryptedPassword) {
    this.database = database;
    this.tableName = tableName;
    this.preFilter = preFilter;
    this.partitionColumns = partitionColumns;
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
  public List<String> getSubscribeColumns() {
    return subscribeColumns;
  }

  public void setSubscribeColumns(@Nullable List<String> subscribeColumns) {
    this.subscribeColumns = subscribeColumns;
  }

  /** Builds the table topic attributes used by a Stream task's subscription source. */
  public Properties getTopicProperties(@Nullable final StreamProperties streamProperties) {
    final Properties topicProperties = new Properties();
    topicProperties.setProperty(TopicConstant.DATABASE_KEY, database);
    topicProperties.setProperty(TopicConstant.TABLE_KEY, tableName);
    topicProperties.setProperty(
        SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    if (streamProperties != null && streamProperties.isFillHistory()) {
      topicProperties.setProperty(TopicConstant.MODE_KEY, TopicConstant.MODE_INITIAL_VALUE);
      if (streamProperties.getFillHistoryStartTime() != null) {
        topicProperties.setProperty(
            TopicConstant.START_TIME_KEY,
            String.valueOf(streamProperties.getFillHistoryStartTime()));
      }
    } else {
      topicProperties.setProperty(TopicConstant.MODE_KEY, TopicConstant.MODE_CONSENSUS_VALUE);
      topicProperties.setProperty(TopicConstant.RETAIN_PROGRESS_AFTER_UNSUBSCRIBE_KEY, "true");
    }
    // Stream windows may hold events until they close; disable timeout-based redelivery.
    topicProperties.setProperty(TopicConstant.RECYCLE_UNCOMMITTED_EVENT_INTERVAL_MS_KEY, "-1");
    if (subscribeColumns != null && !subscribeColumns.isEmpty()) {
      final String columnFilter = buildColumnFilter(subscribeColumns);
      if (columnFilter != null) {
        topicProperties.setProperty(TopicConstant.COLUMN_FILTER_KEY, columnFilter);
      }
    }
    if (streamProperties != null) {
      if (streamProperties.getSubscriptionConsensusWalRetentionSizeInBytes() != null) {
        topicProperties.setProperty(
            TopicConstant.RETENTION_BYTES_KEY,
            String.valueOf(streamProperties.getSubscriptionConsensusWalRetentionSizeInBytes()));
      }
      if (streamProperties.getSubscriptionConsensusWalRetentionTimeMs() != null) {
        topicProperties.setProperty(
            TopicConstant.RETENTION_MS_KEY,
            String.valueOf(streamProperties.getSubscriptionConsensusWalRetentionTimeMs()));
      }
    }
    return topicProperties;
  }

  private static String buildColumnFilter(final List<String> columns) {
    final String quotedColumns =
        columns.stream()
            .filter(Objects::nonNull)
            .filter(column -> column.isEmpty() || !column.trim().isEmpty())
            .map(column -> column.isEmpty() ? column : column.trim())
            .distinct()
            .map(IoTDBSubscriptionSource::quoteColumnName)
            .collect(Collectors.joining(", "));
    if (quotedColumns.isEmpty()) {
      return null;
    }
    // Retain all TAG columns: table row identity also needs TAGs unused by the Stream.
    return "category = \"TAG\" OR column_name IN (" + quotedColumns + ')';
  }

  private static String quoteColumnName(final String column) {
    final String columnName =
        TsFileConstant.TIME_COLUMN_ID.equals(column) ? TsTable.TIME_COLUMN_NAME : column;
    return "\"" + columnName.replace("\"", "\"\"") + "\"";
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

  public long getOwnerEpoch() {
    return ownerEpoch;
  }

  public void setOwnerEpoch(final long ownerEpoch) {
    this.ownerEpoch = ownerEpoch;
  }

  /** Returns the fixed subscription topic owned by the specified Stream task. */
  public static String getTopicName(final String streamName) {
    return STREAM_TOPIC_PREFIX + streamName;
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
    ReadWriteIOUtils.write(host, stream);
    ReadWriteIOUtils.write(rpcPort, stream);
    ReadWriteIOUtils.write(user, stream);
    ReadWriteIOUtils.write(encryptedPassword, stream);
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
    String host = ReadWriteIOUtils.readString(byteBuffer);
    int rpcPort = ReadWriteIOUtils.readInt(byteBuffer);
    String user = ReadWriteIOUtils.readString(byteBuffer);
    String encryptedPassword = ReadWriteIOUtils.readString(byteBuffer);
    return new IoTDBSubscriptionSource(
        database, tableName, preFilter, partitionColumns, host, rpcPort, user, encryptedPassword);
  }

  @Override
  public String toString() {
    return "IoTDBSubscriptionSource{"
        + "database='"
        + database
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", preFilter="
        + preFilter
        + ", partitionColumns="
        + partitionColumns
        + ", subscribeColumns="
        + subscribeColumns
        + ", host='"
        + host
        + '\''
        + ", rpcPort="
        + rpcPort
        + ", user='"
        + user
        + '\''
        + '}';
  }
}
