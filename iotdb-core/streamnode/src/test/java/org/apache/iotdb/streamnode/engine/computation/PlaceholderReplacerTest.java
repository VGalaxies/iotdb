/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.streamnode.engine.computation;

import org.apache.iotdb.commons.stream.TabletColumnPartitionKey;
import org.apache.iotdb.streamnode.engine.window.WindowEvent;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class PlaceholderReplacerTest {

  @Test
  public void testReplaceBracketPlaceholders() {
    PlaceholderReplacer replacer = new PlaceholderReplacer();
    WindowEvent event = new WindowEvent(100, 200, Arrays.asList(), 10);
    TabletColumnPartitionKey key = new TabletColumnPartitionKey(Arrays.asList("d1", 7));

    String sql = "select ${start_time}, ${END_TIME}, ${ROW_NUM}, ${1}, ${2} from table1";
    String result = replacer.replace(sql, event, key);

    Assert.assertEquals("select 100, 200, 10, 'd1', 7 from table1", result);
  }
}
