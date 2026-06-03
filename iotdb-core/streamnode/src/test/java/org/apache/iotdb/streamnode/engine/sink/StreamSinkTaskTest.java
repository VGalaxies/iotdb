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

package org.apache.iotdb.streamnode.engine.sink;

import org.apache.iotdb.streamnode.engine.task.StreamSubTaskContext;

import com.google.common.util.concurrent.Futures;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;

public class StreamSinkTaskTest {

  @Test
  public void testPushNullBlockCreatesCommitOnlyEntry() {
    SinkPipeline pipeline = Mockito.mock(SinkPipeline.class);
    Mockito.when(pipeline.push(Mockito.any())).thenReturn(Futures.immediateVoidFuture());
    StreamSubTaskContext subTaskContext = Mockito.mock(StreamSubTaskContext.class);
    StreamSinkTask sinkTask = new StreamSinkTask(pipeline, 1, subTaskContext);

    sinkTask.push(null, Collections.singletonList(1L));

    ArgumentCaptor<SinkEntry> entryCaptor = ArgumentCaptor.forClass(SinkEntry.class);
    Mockito.verify(pipeline).push(entryCaptor.capture());
    SinkEntry entry = entryCaptor.getValue();
    Assert.assertFalse(entry.getTsBlock().isPresent());
    Assert.assertEquals(0, entry.getMemorySizeInBytes());
    Assert.assertEquals(Collections.singletonList(1L), entry.getCommitIds());
    Assert.assertEquals(1, entry.getSubTaskId());
  }
}
