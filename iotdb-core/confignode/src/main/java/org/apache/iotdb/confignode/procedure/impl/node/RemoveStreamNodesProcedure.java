package org.apache.iotdb.confignode.procedure.impl.node;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.write.streamnode.RemoveStreamNodePlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.RemoveStreamNodeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RemoveStreamNodesProcedure extends AbstractNodeProcedure<RemoveStreamNodeState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveStreamNodesProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private TStreamNodeLocation removedStreamNode;

  public RemoveStreamNodesProcedure(TStreamNodeLocation removedStreamNode) {
    super();
    this.removedStreamNode = removedStreamNode;
  }

  public RemoveStreamNodesProcedure() {
    super();
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RemoveStreamNodeState state)
      throws InterruptedException {
    if (removedStreamNode == null) {
      return Flow.NO_MORE_STATE;
    }

    try {
      switch (state) {
        case NODE_STOP:
          setNextState(RemoveStreamNodeState.NODE_REMOVE);
          break;
        case NODE_REMOVE:
          TSStatus response =
              env.getConfigManager()
                  .getConsensusManager()
                  .write(new RemoveStreamNodePlan(removedStreamNode));
          if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            throw new ProcedureException(
                String.format(
                    "Fail to remove [%s] StreamNode on Config Nodes [%s]",
                    removedStreamNode, response.getMessage()));
          }
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(
              String.format("Unknown state during executing removeStreamNodeProcedure, %s", state));
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOGGER.error(
            "Retrievable error trying to remove StreamNode [{}], state [{}]",
            removedStreamNode,
            state,
            e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  String.format(
                      "Fail to remove StreamNode [%s] at STATE [%s], %s",
                      removedStreamNode, state, e.getMessage())));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, RemoveStreamNodeState removeStreamNodeState)
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
    return RemoveStreamNodeState.NODE_STOP;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.REMOVE_STREAM_NODE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftCommonsSerDeUtils.serializeTStreamNodeLocation(removedStreamNode, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    removedStreamNode = ThriftCommonsSerDeUtils.deserializeTStreamNodeLocation(byteBuffer);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof RemoveStreamNodesProcedure) {
      RemoveStreamNodesProcedure thatProc = (RemoveStreamNodesProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && Objects.equals(thatProc.removedStreamNode, this.removedStreamNode);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.removedStreamNode);
  }
}
