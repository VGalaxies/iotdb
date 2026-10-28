package org.apache.iotdb.confignode.procedure.impl.node;

import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RemoveStreamNodeHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.RemoveStreamNodeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public class RemoveStreamNodeProcedure extends AbstractNodeProcedure<RemoveStreamNodeState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveStreamNodeProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private TStreamNodeLocation removedStreamNode;

  /**
   * Task names successfully stopped during STREAM_TASKS_RUNTIME_MIGRATE Phase 1 and still needing
   * restart on the original StreamNode if Phase 1 retries are exhausted. Cleared once Phase 1
   * completes for all tasks (Phase 2 failures must not restart on the node being removed). Does not
   * include tasks that were already STOPPED before remove started. Persisted across procedure
   * retries / leader switch.
   */
  private final Set<String> stoppedTasksToRestart = new LinkedHashSet<>();

  public RemoveStreamNodeProcedure(TStreamNodeLocation removedStreamNode) {
    super();
    this.removedStreamNode = removedStreamNode;
  }

  public RemoveStreamNodeProcedure() {
    super();
  }

  public TStreamNodeLocation getRemovedStreamNode() {
    return removedStreamNode;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RemoveStreamNodeState state)
      throws InterruptedException {
    if (removedStreamNode == null) {
      return Flow.NO_MORE_STATE;
    }

    RemoveStreamNodeHandler removeStreamNodeHandler = env.getRemoveStreamNodeHandler();
    try {
      switch (state) {
        case REMOVE_STREAM_NODE_PREPARE:
          // Mark the StreamNode as Removing in the ConfigNode's load cache so that the
          // StreamManager won't assign new tasks to it until the node is
          // removed from the cluster.
          removeStreamNodeHandler.changeStreamNodeStatus(removedStreamNode, NodeStatus.Removing);
          setNextState(RemoveStreamNodeState.STREAM_TASKS_RUNTIME_MIGRATE);
          break;
        case STREAM_TASKS_RUNTIME_MIGRATE:
          removeStreamNodeHandler.migrateRuntimeStreamTasks(
              removedStreamNode, stoppedTasksToRestart);
          setNextState(RemoveStreamNodeState.STOP_STREAM_NODE);
          break;
        case STOP_STREAM_NODE:
          removeStreamNodeHandler.removeStreamNodePersistence(removedStreamNode);
          removeStreamNodeHandler.stopStreamNode(removedStreamNode);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(
              String.format(
                  ProcedureMessages.UNKNOWN_STATE_DURING_EXECUTING_REMOVESTREAMNODEPROCEDURE,
                  state));
      }
    } catch (Exception e) {
      LOGGER.error(
          ProcedureMessages.RETRIEVABLE_ERROR_TRYING_TO_REMOVE_STREAMNODE_STATE,
          removedStreamNode,
          state,
          e);
      if (getCycles() < RETRY_THRESHOLD) {
        setNextState(getCurrentState());
      } else {
        // Only Phase 1 may leave STOPPED tasks that need restart on the original node.
        if (state == RemoveStreamNodeState.STREAM_TASKS_RUNTIME_MIGRATE) {
          removeStreamNodeHandler.restartStoppedStreamTasks(stoppedTasksToRestart);
        }
        env.getRemoveStreamNodeHandler()
            .changeStreamNodeStatus(removedStreamNode, NodeStatus.Running);
        setFailure(
            new ProcedureException(
                String.format(
                    ProcedureMessages.FAIL_TO_REMOVE_STREAMNODE_AT_STATE,
                    removedStreamNode,
                    state,
                    e.getMessage())));
        return Flow.NO_MORE_STATE;
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv env, RemoveStreamNodeState removeStreamNodeState)
      throws IOException, InterruptedException, ProcedureException {
    // no need to rollback
  }

  @Override
  protected RemoveStreamNodeState getState(int stateId) {
    return RemoveStreamNodeState.values()[stateId];
  }

  @Override
  protected int getStateId(RemoveStreamNodeState removeStreamNodeState) {
    return removeStreamNodeState.ordinal();
  }

  @Override
  protected RemoveStreamNodeState getInitialState() {
    return RemoveStreamNodeState.REMOVE_STREAM_NODE_PREPARE;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.REMOVE_STREAM_NODE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftCommonsSerDeUtils.serializeTStreamNodeLocation(removedStreamNode, stream);
    stream.writeInt(stoppedTasksToRestart.size());
    for (String taskName : stoppedTasksToRestart) {
      ReadWriteIOUtils.write(taskName, stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    removedStreamNode = ThriftCommonsSerDeUtils.deserializeTStreamNodeLocation(byteBuffer);
    stoppedTasksToRestart.clear();
    if (byteBuffer.hasRemaining()) {
      final int size = byteBuffer.getInt();
      for (int i = 0; i < size; i++) {
        stoppedTasksToRestart.add(ReadWriteIOUtils.readString(byteBuffer));
      }
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof RemoveStreamNodeProcedure) {
      RemoveStreamNodeProcedure thatProc = (RemoveStreamNodeProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && Objects.equals(thatProc.removedStreamNode, this.removedStreamNode)
          && Objects.equals(thatProc.stoppedTasksToRestart, this.stoppedTasksToRestart);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.removedStreamNode, this.stoppedTasksToRestart);
  }
}
