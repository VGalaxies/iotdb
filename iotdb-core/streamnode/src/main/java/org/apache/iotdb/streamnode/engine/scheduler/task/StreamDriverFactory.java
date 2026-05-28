package org.apache.iotdb.streamnode.engine.scheduler.task;

import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;

public class StreamDriverFactory {
  /**
   * Create a new stream driver with the given configuration.
   *
   * @param partitionKey the partition key for this driver
   * @param streamName the name of the stream
   * @param streamSubTask the sub-task containing window, computation engine, write-back engine and
   *     other configuration
   * @return a new IStreamDriver instance
   */
  public static IStreamDriver createDriver(
      PartitionKey partitionKey, String streamName, StreamSubTask streamSubTask) {
    // TODO: Implement
    return null;
  }
}
