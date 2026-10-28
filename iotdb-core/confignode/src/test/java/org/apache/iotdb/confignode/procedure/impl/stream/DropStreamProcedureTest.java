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

package org.apache.iotdb.confignode.procedure.impl.stream;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.stream.StreamManager;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.DropTopicProcedure;
import org.apache.iotdb.confignode.procedure.state.stream.DropStreamState;

import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DropStreamProcedureTest {

  @Test
  public void testRollbackUnmarksStreamBeingDropped() throws Exception {
    final ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    final StreamManager streamManager = mock(StreamManager.class);
    when(env.getConfigManager()).thenReturn(configManager);
    when(configManager.getStreamManager()).thenReturn(streamManager);

    final DropStreamProcedure procedure = new DropStreamProcedure("stream_0");
    procedure.rollbackState(env, DropStreamState.DROP_RUNTIME_AND_TOPIC);

    verify(streamManager).unmarkStreamBeingDropped("stream_0");
  }

  /**
   * Verifies that Stream metadata is retained until the managed table-model Topic child procedure
   * finishes, and that the child procedure explicitly targets the table model.
   */
  @Test
  public void testDropTopicProcedureBeforeWritingStreamMetadataToConsensus() throws Exception {
    final ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    final StreamManager streamManager = mock(StreamManager.class);
    final ConsensusManager consensusManager = mock(ConsensusManager.class);
    when(env.getConfigManager()).thenReturn(configManager);
    when(configManager.getStreamManager()).thenReturn(streamManager);
    when(configManager.getConsensusManager()).thenReturn(consensusManager);
    when(streamManager.dropStreamTask("stream_0")).thenReturn(StatusUtils.OK);
    when(streamManager.isSubscriptionSource("stream_0")).thenReturn(true);
    when(consensusManager.write(org.mockito.ArgumentMatchers.any())).thenReturn(StatusUtils.OK);

    final TestDropStreamProcedure procedure = new TestDropStreamProcedure("stream_0");
    procedure.executeState(env, DropStreamState.DROP_RUNTIME_AND_TOPIC);

    verify(streamManager).dropStreamTask("stream_0");
    Assert.assertEquals(
        new DropTopicProcedure(IoTDBSubscriptionSource.getTopicName("stream_0"), true),
        procedure.getChildProcedure());
    verify(consensusManager, never()).write(org.mockito.ArgumentMatchers.any());

    procedure.executeState(env, DropStreamState.DROP_STREAM_METADATA);

    verify(consensusManager).write(org.mockito.ArgumentMatchers.any());
  }

  /** Verifies that a failed runtime cleanup does not schedule metadata or Topic deletion. */
  @Test
  public void testDropRuntimeTaskFailureSkipsTopicAndMetadataRemoval() throws Exception {
    final ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    final StreamManager streamManager = mock(StreamManager.class);
    final ConsensusManager consensusManager = mock(ConsensusManager.class);
    when(env.getConfigManager()).thenReturn(configManager);
    when(configManager.getStreamManager()).thenReturn(streamManager);
    when(configManager.getConsensusManager()).thenReturn(consensusManager);
    when(streamManager.dropStreamTask("stream_0"))
        .thenReturn(new TSStatus(500).setMessage("runtime cleanup failed"));

    final TestDropStreamProcedure procedure = new TestDropStreamProcedure("stream_0");
    procedure.executeState(env, DropStreamState.DROP_RUNTIME_AND_TOPIC);

    Assert.assertNull(procedure.getChildProcedure());
    verify(consensusManager, never()).write(org.mockito.ArgumentMatchers.any());
  }

  /** Verifies that only subscription-backed Streams schedule managed Topic deletion. */
  @Test
  public void testNonSubscriptionSourceSkipsTopicProcedure() throws Exception {
    final ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    final StreamManager streamManager = mock(StreamManager.class);
    when(env.getConfigManager()).thenReturn(configManager);
    when(configManager.getStreamManager()).thenReturn(streamManager);
    when(streamManager.dropStreamTask("stream_0")).thenReturn(StatusUtils.OK);
    when(streamManager.isSubscriptionSource("stream_0")).thenReturn(false);

    final TestDropStreamProcedure procedure = new TestDropStreamProcedure("stream_0");
    procedure.executeState(env, DropStreamState.DROP_RUNTIME_AND_TOPIC);

    Assert.assertNull(procedure.getChildProcedure());
  }

  private static class TestDropStreamProcedure extends DropStreamProcedure {

    private Procedure<ConfigNodeProcedureEnv> childProcedure;

    private TestDropStreamProcedure(final String streamName) {
      super(streamName);
    }

    private void executeState(final ConfigNodeProcedureEnv env, final DropStreamState state)
        throws InterruptedException {
      super.executeFromState(env, state);
    }

    @Override
    protected void addChildProcedure(final Procedure<ConfigNodeProcedureEnv> procedure) {
      childProcedure = procedure;
    }

    private Procedure<ConfigNodeProcedureEnv> getChildProcedure() {
      return childProcedure;
    }
  }
}
