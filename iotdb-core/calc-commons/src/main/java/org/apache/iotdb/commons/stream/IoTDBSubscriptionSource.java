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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class IoTDBSubscriptionSource extends StreamSource {

  private String database;
  private String tableName;
  private String preFilter; // nullable, serialized WHERE expression
  private List<String> partitionColumns; // nullable

  // Connection info for the source IoTDB instance
  private String host;
  private int rpcPort;
  private String user;
  private String encryptedPassword;

  public IoTDBSubscriptionSource(
      String database,
      String tableName,
      String preFilter,
      List<String> partitionColumns,
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

  public String getPreFilter() {
    return preFilter;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
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
  public void serialize(OutputStream outputStream) throws IOException {
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    // Serialize type
    dataOutputStream.writeInt(getType().ordinal());

    // Serialize database
    dataOutputStream.writeUTF(database);

    // Serialize tableName
    dataOutputStream.writeUTF(tableName);

    // Serialize preFilter (nullable)
    dataOutputStream.writeBoolean(preFilter != null);
    if (preFilter != null) {
      dataOutputStream.writeUTF(preFilter);
    }

    // Serialize partitionColumns (nullable)
    dataOutputStream.writeBoolean(partitionColumns != null);
    if (partitionColumns != null) {
      dataOutputStream.writeInt(partitionColumns.size());
      for (String column : partitionColumns) {
        dataOutputStream.writeUTF(column);
      }
    }

    // Serialize connection info
    dataOutputStream.writeUTF(host != null ? host : "");
    dataOutputStream.writeInt(rpcPort);
    dataOutputStream.writeUTF(user != null ? user : "");
    dataOutputStream.writeUTF(encryptedPassword != null ? encryptedPassword : "");
  }

  public static IoTDBSubscriptionSource deserialize(InputStream inputStream) throws IOException {
    DataInputStream dataInputStream = new DataInputStream(inputStream);
    // Deserialize database
    String database = dataInputStream.readUTF();

    // Deserialize tableName
    String tableName = dataInputStream.readUTF();

    // Deserialize preFilter (nullable)
    String preFilter = null;
    if (dataInputStream.readBoolean()) {
      preFilter = dataInputStream.readUTF();
    }

    // Deserialize partitionColumns (nullable)
    List<String> partitionColumns = null;
    if (dataInputStream.readBoolean()) {
      int partitionColumnsSize = dataInputStream.readInt();
      partitionColumns = new ArrayList<>(partitionColumnsSize);
      for (int i = 0; i < partitionColumnsSize; i++) {
        partitionColumns.add(dataInputStream.readUTF());
      }
    }

    // Deserialize connection info
    String host = dataInputStream.readUTF();
    int rpcPort = dataInputStream.readInt();
    String user = dataInputStream.readUTF();
    String encryptedPassword = dataInputStream.readUTF();

    return new IoTDBSubscriptionSource(
        database, tableName, preFilter, partitionColumns, host, rpcPort, user, encryptedPassword);
  }
}
