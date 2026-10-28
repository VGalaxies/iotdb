package org.apache.iotdb.confignode.consensus.request.write.streamnode;

import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RemoveStreamNodePlan extends ConfigPhysicalPlan {
  private TStreamNodeLocation streamNodeLocation;

  public RemoveStreamNodePlan() {
    super(ConfigPhysicalPlanType.RemoveStreamNode);
  }

  public RemoveStreamNodePlan(TStreamNodeLocation streamNodeLocation) {
    this();
    this.streamNodeLocation = streamNodeLocation;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ThriftCommonsSerDeUtils.serializeTStreamNodeLocation(streamNodeLocation, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    streamNodeLocation = ThriftCommonsSerDeUtils.deserializeTStreamNodeLocation(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RemoveStreamNodePlan that = (RemoveStreamNodePlan) o;
    return Objects.equals(streamNodeLocation, that.streamNodeLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), streamNodeLocation);
  }

  public TStreamNodeLocation getStreamNodeLocation() {
    return streamNodeLocation;
  }

  @Override
  public String toString() {
    return "{RemoveStreamNodeReq: " + "TStreamNodeLocation: " + this.getStreamNodeLocation() + "}";
  }
}
