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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata;

import org.apache.iotdb.common.rpc.thrift.TStreamInfo;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ShowStreamsTaskTest {

  @Test
  public void testBuildTsBlockIncludesPropertiesAsLastColumn() throws Exception {
    final String properties =
        "StreamProperties{watermark=1000, allowedLateness=5000, maxDelayWindowClose=4000, maxIdleWindowRetain=0, leftClosed=false}";
    final TStreamInfo streamInfo =
        new TStreamInfo()
            .setStreamName("stream1")
            .setStatus("CREATED")
            .setCreationTime(1L)
            .setCreator("root")
            .setSubQuery("SELECT * FROM source")
            .setRunningOn("")
            .setLastUpTime(0L)
            .setLastDownTime(0L)
            .setLastDownReason("")
            .setLastHeartbeatTime(0L)
            .setProperties(properties);
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    ShowStreamsTask.buildTsBlock(Collections.singletonList(streamInfo), future);

    final ConfigTaskResult result = future.get();
    final TsBlock resultSet = result.getResultSet();
    assertEquals("properties", result.getResultSetHeader().getRespColumns().get(13));
    assertEquals(14, resultSet.getValueColumnCount());
    assertEquals(
        properties,
        resultSet.getColumn(13).getBinary(0).getStringValue(TSFileConfig.STRING_CHARSET));
  }
}
