package org.apache.iotdb.confignode.consensus.request.write.streamnode;

import org.apache.iotdb.common.rpc.thrift.TStreamNodeConfiguration;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class UpdateStreamNodePlan extends ConfigPhysicalPlan {
  private TStreamNodeConfiguration streamNodeConfiguration;

  public UpdateStreamNodePlan() {
    super(ConfigPhysicalPlanType.UpdateStreamNodeConfiguration);
  }

  public UpdateStreamNodePlan(TStreamNodeConfiguration streamNodeConfiguration) {
    this();
    this.streamNodeConfiguration = streamNodeConfiguration;
  }

  public TStreamNodeConfiguration getStreamNodeConfiguration() {
    return streamNodeConfiguration;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ThriftCommonsSerDeUtils.serializeTStreamNodeConfiguration(streamNodeConfiguration, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    streamNodeConfiguration = ThriftCommonsSerDeUtils.deserializeTStreamNodeConfiguration(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!getType().equals(((UpdateStreamNodePlan) o).getType())) {
      return false;
    }
    UpdateStreamNodePlan that = (UpdateStreamNodePlan) o;
    return streamNodeConfiguration.equals(that.streamNodeConfiguration);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), streamNodeConfiguration);
  }
}
