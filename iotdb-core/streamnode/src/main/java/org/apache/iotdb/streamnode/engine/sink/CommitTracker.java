/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.streamnode.engine.sink;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

public class CommitTracker {
  private final SourceCommitCallback callback;
  private final Map<Long, CommitState> commitStates = new HashMap<>();
  private final TreeSet<Long> fullyCommittedIds = new TreeSet<>();
  private volatile long committedId = -1;
  private final ReentrantLock lock = new ReentrantLock();

  public CommitTracker(SourceCommitCallback callback) {
    this.callback = callback;
  }

  /** The actual number of subtasks registered for each commitId during data distribution */
  public void registerCommitId(long commitId, int actualSubTaskCount) {
    lock.lock();
    try {
      commitStates.computeIfAbsent(commitId, k -> new CommitState(actualSubTaskCount));
    } finally {
      lock.unlock();
    }
  }

  /**
   * Mark that a subtask has completed a commit to a group of commit IDs (data has been successfully
   * written to IoTDB)
   */
  public long markCommitted(Collection<Long> commitIds, int subTaskId) {
    long resultCommittedId;
    java.util.ArrayList<Long> callbacks = new java.util.ArrayList<>();

    lock.lock();
    try {
      for (long commitId : commitIds) {
        CommitState state = commitStates.get(commitId);
        if (state == null) continue;
        state.committedSubTasks.add(subTaskId);
        if (state.committedSubTasks.size() == state.actualSubTaskCount) {
          fullyCommittedIds.add(commitId);
          commitStates.remove(commitId);
        }
      }
      while (!fullyCommittedIds.isEmpty() && fullyCommittedIds.first() == committedId + 1) {
        committedId = fullyCommittedIds.pollFirst();
        callbacks.add(committedId);
      }
      resultCommittedId = committedId;
    } finally {
      lock.unlock();
    }

    for (long id : callbacks) {
      callback.onCommitted(id);
    }

    return resultCommittedId;
  }

  private static class CommitState {
    final int actualSubTaskCount;
    final Set<Integer> committedSubTasks = new HashSet<>();

    CommitState(int n) {
      this.actualSubTaskCount = n;
    }
  }

  public long getCommittedId() {
    return committedId;
  }

  public void setCommittedId(long committedId) {
    this.committedId = committedId;
  }
}
