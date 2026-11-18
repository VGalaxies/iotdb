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

package org.apache.iotdb.streamnode.engine.window.distributed;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks per-partition (DataNode, Region) progress and evaluates Algorithm 4.1 distributed window
 * close readiness without duplicating message buffers in StreamNode.
 */
public class PartitionProgressTracker {

  public static final long DEFAULT_STALE_PARTITION_TIMEOUT_MS = 30_000L;

  private final long stalePartitionTimeoutMs;
  private final ConcurrentHashMap<PartitionId, FragmentState> livePartitions =
      new ConcurrentHashMap<>();

  public PartitionProgressTracker() {
    this(DEFAULT_STALE_PARTITION_TIMEOUT_MS);
  }

  public PartitionProgressTracker(long stalePartitionTimeoutMs) {
    this.stalePartitionTimeoutMs = Math.max(0L, stalePartitionTimeoutMs);
  }

  public long getStalePartitionTimeoutMs() {
    return stalePartitionTimeoutMs;
  }

  public void onHeartbeat(int dataNodeId, String regionId, long watermarkTs, long nowWallMs) {
    onHeartbeat(new PartitionId(dataNodeId, regionId), watermarkTs, nowWallMs);
  }

  public void onHeartbeat(PartitionId partitionId, long watermarkTs, long nowWallMs) {
    Objects.requireNonNull(partitionId);
    livePartitions.compute(
        partitionId,
        (id, existing) -> {
          if (existing == null) {
            return new FragmentState(watermarkTs, 0L, false, true, nowWallMs);
          }
          existing.updateHeartbeat(watermarkTs, nowWallMs);
          return existing;
        });
  }

  public void onDataBatch(
      int dataNodeId, String regionId, long maxBatchTs, long addedRowCount, long nowWallMs) {
    onDataBatch(new PartitionId(dataNodeId, regionId), maxBatchTs, addedRowCount, nowWallMs);
  }

  public void onDataBatch(
      PartitionId partitionId, long maxBatchTs, long addedRowCount, long nowWallMs) {
    Objects.requireNonNull(partitionId);
    livePartitions.compute(
        partitionId,
        (id, existing) -> {
          if (existing == null) {
            return new FragmentState(
                maxBatchTs, Math.max(0L, addedRowCount), true, false, nowWallMs);
          }
          existing.updateDataBatch(maxBatchTs, addedRowCount, nowWallMs);
          return existing;
        });
  }

  public int getTrackedPartitionCount() {
    return livePartitions.size();
  }

  public Map<PartitionId, FragmentState> snapshotPartitions() {
    Map<PartitionId, FragmentState> snapshot = new LinkedHashMap<>();
    livePartitions.forEach((k, v) -> snapshot.put(k, v.copy()));
    return Collections.unmodifiableMap(snapshot);
  }

  /**
   * Computes {@code min_p f_p.progress} across active non-stale partitions. Returns {@link
   * Long#MIN_VALUE} when no active partitions are tracked or when any active partition has not yet
   * reported progress.
   */
  public long computeAlignedProgress(long nowWallMs) {
    if (livePartitions.isEmpty()) {
      return Long.MIN_VALUE;
    }
    long minProgress = Long.MAX_VALUE;
    int activeCount = 0;
    for (FragmentState state : livePartitions.values()) {
      FragmentState snap = state.copy();
      if (stalePartitionTimeoutMs > 0
          && snap.getLastUpdateWallTimeMs() > 0
          && (nowWallMs - snap.getLastUpdateWallTimeMs()) > stalePartitionTimeoutMs) {
        continue;
      }
      activeCount++;
      if (snap.getProgress() == Long.MIN_VALUE) {
        return Long.MIN_VALUE;
      }
      minProgress = Math.min(minProgress, snap.getProgress());
    }
    return activeCount == 0 ? Long.MIN_VALUE : minProgress;
  }

  /** Evaluates Algorithm 4.1 distributed fragment-owner window close readiness. */
  public CloseDecision evaluateCloseDecision(
      WindowRecord record, long currentEpoch, long currentMemberVersion) {
    Objects.requireNonNull(record);
    if (record.getEpoch() != currentEpoch) {
      return new CloseDecision(
          CloseDecision.Action.REJECT, CloseDecision.WaitReason.STALE_EPOCH, null);
    }
    if (record.getMemberVersion() != currentMemberVersion) {
      return new CloseDecision(
          CloseDecision.Action.WAIT, CloseDecision.WaitReason.MEMBER_VERSION_MISMATCH, null);
    }
    for (Map.Entry<PartitionId, FragmentState> entry : record.getFragments().entrySet()) {
      PartitionId pid = entry.getKey();
      FragmentState frag = entry.getValue();
      if (frag.getRowCount() > 0 && !frag.isStateReady()) {
        return new CloseDecision(
            CloseDecision.Action.WAIT, CloseDecision.WaitReason.FRAGMENT_STATE_NOT_READY, pid);
      }
      if (frag.getRowCount() == 0 && !frag.isEmptyDeclared()) {
        if (frag.getProgress() < record.getEndTime()) {
          if (frag.getProgress() == Long.MIN_VALUE) {
            return new CloseDecision(
                CloseDecision.Action.WAIT, CloseDecision.WaitReason.FRAGMENT_SILENT, pid);
          }
          return new CloseDecision(
              CloseDecision.Action.WAIT, CloseDecision.WaitReason.FRAGMENT_PROGRESS_LAGGING, pid);
        }
        if (!frag.isStateReady()) {
          return new CloseDecision(
              CloseDecision.Action.WAIT, CloseDecision.WaitReason.FRAGMENT_SILENT, pid);
        }
      }
      if (frag.getProgress() < record.getEndTime()) {
        return new CloseDecision(
            CloseDecision.Action.WAIT, CloseDecision.WaitReason.FRAGMENT_PROGRESS_LAGGING, pid);
      }
    }
    return new CloseDecision(CloseDecision.Action.CLOSE, CloseDecision.WaitReason.NONE, null);
  }

  public static final class PartitionId {
    private final int dataNodeId;
    private final String regionId;

    public PartitionId(int dataNodeId, String regionId) {
      this.dataNodeId = dataNodeId;
      this.regionId = regionId == null ? "" : regionId;
    }

    public PartitionId(int dataNodeId, int regionId) {
      this(dataNodeId, String.valueOf(regionId));
    }

    public int getDataNodeId() {
      return dataNodeId;
    }

    public String getRegionId() {
      return regionId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PartitionId)) {
        return false;
      }
      PartitionId that = (PartitionId) o;
      return dataNodeId == that.dataNodeId && Objects.equals(regionId, that.regionId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(dataNodeId, regionId);
    }

    @Override
    public String toString() {
      return "PartitionId{dataNodeId=" + dataNodeId + ", regionId='" + regionId + "'}";
    }
  }

  public static final class FragmentState {
    private long progress;
    private long rowCount;
    private boolean stateReady;
    private boolean emptyDeclared;
    private long lastUpdateWallTimeMs;

    public FragmentState(
        long progress,
        long rowCount,
        boolean stateReady,
        boolean emptyDeclared,
        long lastUpdateWallTimeMs) {
      this.progress = progress;
      this.rowCount = rowCount;
      this.stateReady = stateReady;
      this.emptyDeclared = emptyDeclared;
      this.lastUpdateWallTimeMs = lastUpdateWallTimeMs;
    }

    public synchronized void updateHeartbeat(long watermarkTs, long nowWallMs) {
      if (watermarkTs > this.progress) {
        this.progress = watermarkTs;
      }
      this.emptyDeclared = true;
      this.lastUpdateWallTimeMs = Math.max(this.lastUpdateWallTimeMs, nowWallMs);
    }

    public synchronized void updateDataBatch(long maxBatchTs, long addedRowCount, long nowWallMs) {
      if (maxBatchTs > this.progress) {
        this.progress = maxBatchTs;
      }
      this.rowCount += Math.max(0L, addedRowCount);
      this.stateReady = true;
      this.lastUpdateWallTimeMs = Math.max(this.lastUpdateWallTimeMs, nowWallMs);
    }

    public synchronized FragmentState copy() {
      return new FragmentState(progress, rowCount, stateReady, emptyDeclared, lastUpdateWallTimeMs);
    }

    public synchronized long getProgress() {
      return progress;
    }

    public synchronized long getRowCount() {
      return rowCount;
    }

    public synchronized boolean isStateReady() {
      return stateReady;
    }

    public synchronized boolean isEmptyDeclared() {
      return emptyDeclared;
    }

    public synchronized long getLastUpdateWallTimeMs() {
      return lastUpdateWallTimeMs;
    }
  }

  public static final class WindowRecord {
    private final long windowId;
    private final long startTime;
    private final long endTime;
    private final int ownerNodeId;
    private final long epoch;
    private final long memberVersion;
    private final Map<PartitionId, FragmentState> fragments;

    public WindowRecord(
        long windowId,
        long startTime,
        long endTime,
        int ownerNodeId,
        long epoch,
        long memberVersion,
        Map<PartitionId, FragmentState> fragments) {
      this.windowId = windowId;
      this.startTime = startTime;
      this.endTime = endTime;
      this.ownerNodeId = ownerNodeId;
      this.epoch = epoch;
      this.memberVersion = memberVersion;
      this.fragments =
          fragments == null
              ? Collections.emptyMap()
              : Collections.unmodifiableMap(new LinkedHashMap<>(fragments));
    }

    public long getWindowId() {
      return windowId;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getEndTime() {
      return endTime;
    }

    public int getOwnerNodeId() {
      return ownerNodeId;
    }

    public long getEpoch() {
      return epoch;
    }

    public long getMemberVersion() {
      return memberVersion;
    }

    public Map<PartitionId, FragmentState> getFragments() {
      return fragments;
    }
  }

  public static final class CloseDecision {
    public enum Action {
      CLOSE,
      WAIT,
      REJECT
    }

    public enum WaitReason {
      NONE,
      STALE_EPOCH,
      MEMBER_VERSION_MISMATCH,
      FRAGMENT_PROGRESS_LAGGING,
      FRAGMENT_SILENT,
      FRAGMENT_STATE_NOT_READY
    }

    private final Action action;
    private final WaitReason waitReason;
    private final PartitionId blockingPartition;

    public CloseDecision(Action action, WaitReason waitReason, PartitionId blockingPartition) {
      this.action = Objects.requireNonNull(action);
      this.waitReason = Objects.requireNonNull(waitReason);
      this.blockingPartition = blockingPartition;
    }

    public Action getAction() {
      return action;
    }

    public WaitReason getWaitReason() {
      return waitReason;
    }

    public PartitionId getBlockingPartition() {
      return blockingPartition;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CloseDecision)) {
        return false;
      }
      CloseDecision that = (CloseDecision) o;
      return action == that.action
          && waitReason == that.waitReason
          && Objects.equals(blockingPartition, that.blockingPartition);
    }

    @Override
    public int hashCode() {
      return Objects.hash(action, waitReason, blockingPartition);
    }

    @Override
    public String toString() {
      return "CloseDecision{action="
          + action
          + ", waitReason="
          + waitReason
          + ", blockingPartition="
          + blockingPartition
          + '}';
    }
  }
}
