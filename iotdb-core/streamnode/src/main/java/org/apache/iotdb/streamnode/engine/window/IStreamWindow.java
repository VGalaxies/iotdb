package org.apache.iotdb.streamnode.engine.window;

import org.apache.iotdb.commons.stream.PartitionKey;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.Optional;

public interface IStreamWindow {
  ListenableFuture<?> isBlocked();

  // consume data and return whether new event can be generated
  Optional<IEventInfo> getEvent();

  ListenableFuture<?> push(PartitionKey key, TsBlock tsBlock);
}
