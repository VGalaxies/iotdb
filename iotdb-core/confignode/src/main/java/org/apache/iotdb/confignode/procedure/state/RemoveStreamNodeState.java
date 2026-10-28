package org.apache.iotdb.confignode.procedure.state;

public enum RemoveStreamNodeState {
  REMOVE_STREAM_NODE_PREPARE,
  STREAM_TASKS_RUNTIME_MIGRATE,
  STOP_STREAM_NODE
}
