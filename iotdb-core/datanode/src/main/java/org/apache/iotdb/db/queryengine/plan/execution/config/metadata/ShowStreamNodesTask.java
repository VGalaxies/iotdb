package org.apache.iotdb.db.queryengine.plan.execution.config.metadata;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TShowStreamNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeInfo;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.BytesUtils;

import java.util.List;
import java.util.stream.Collectors;

public class ShowStreamNodesTask implements IConfigTask {

  public ShowStreamNodesTask() {
    // do nothing
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showStreamNodes();
  }

  public static void buildTsBlock(
      TShowStreamNodesResp showStreamNodesResp, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showStreamNodesColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    if (showStreamNodesResp.getStreamNodesInfoList() != null) {
      for (TStreamNodeInfo streamNodeInfo : showStreamNodesResp.getStreamNodesInfoList()) {
        builder.getTimeColumnBuilder().writeLong(0);
        builder.getColumnBuilder(0).writeInt(streamNodeInfo.getStreamNodeId());
        builder.getColumnBuilder(1).writeBinary(BytesUtils.valueOf(streamNodeInfo.getStatus()));
        builder
            .getColumnBuilder(2)
            .writeBinary(BytesUtils.valueOf(streamNodeInfo.getInternalAddress()));
        builder.getColumnBuilder(3).writeInt(streamNodeInfo.getInternalPort());

        builder.declarePosition();
      }
    }
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowAINodesHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
