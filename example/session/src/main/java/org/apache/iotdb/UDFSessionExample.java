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

package org.apache.iotdb;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UDFSessionExample {

  private static final String LOCAL_HOST = "127.0.0.1";

  public static void main(final String[] args) throws Exception {
    Session session =
        new Session.Builder()
            .host(LOCAL_HOST)
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_1_0)
            .build();
    session.open(false);

    String deviceId = "root.eg.etth";
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("model");
    types.add(TSDataType.DOUBLE);

    try {
      session.executeNonQueryStatement(
          "create function UDTFVARMATrain as 'org.apache.iotdb.library.anomaly.UDTFVARMATrain'");
      session.executeNonQueryStatement(
          "create function UDTFVARMAPredict as 'org.apache.iotdb.library.anomaly.UDTFVARMAPredict'");
    } catch (final Exception e) {
      e.printStackTrace();
    }

    SessionDataSet dataSet =
        session.executeQueryStatement(
            "select UDTFVARMATrain(s0,s1,s2,s3,s4,s5,s6,'p'='3','q'='1','eta'='1.0') from root.eg.etth");
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      session.insertAlignedRecord(
          deviceId,
          record.getTimestamp(),
          measurements,
          types,
          Collections.singletonList(record.getField(0).getDoubleV()));
    }
  }
}
