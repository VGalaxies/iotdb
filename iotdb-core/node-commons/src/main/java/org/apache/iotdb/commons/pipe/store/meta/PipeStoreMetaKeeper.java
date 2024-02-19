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

package org.apache.iotdb.commons.pipe.store.meta;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeMetaKeeper;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class PipeStoreMetaKeeper {

  protected final Map<String, String> kvs;

  private final ReentrantReadWriteLock pipeStoreMetaKeeperLock;

  public PipeStoreMetaKeeper() {
    this.kvs = new ConcurrentHashMap<>();
    this.pipeStoreMetaKeeperLock = new ReentrantReadWriteLock(true);
  }

  public void clear() {
    this.kvs.clear();
  }

  /////////////////////////////////  Lock  /////////////////////////////////

  public void acquireReadLock() {
    pipeStoreMetaKeeperLock.readLock().lock();
  }

  public void releaseReadLock() {
    pipeStoreMetaKeeperLock.readLock().unlock();
  }

  public void acquireWriteLock() {
    pipeStoreMetaKeeperLock.writeLock().lock();
  }

  public void releaseWriteLock() {
    pipeStoreMetaKeeperLock.writeLock().unlock();
  }

  /////////////////////////////////  Snapshot  /////////////////////////////////

  public void processTakeSnapshot(FileOutputStream fileOutputStream) throws IOException {
    ReadWriteIOUtils.write(kvs.size(), fileOutputStream);
    for (Map.Entry<String, String> entry : kvs.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), fileOutputStream);
      ReadWriteIOUtils.write(entry.getValue(), fileOutputStream);
    }
  }

  public void processLoadSnapshot(FileInputStream fileInputStream) throws IOException {
    clear();

    final int size = ReadWriteIOUtils.readInt(fileInputStream);
    for (int i = 0; i < size; i++) {
      final String key = ReadWriteIOUtils.readString(fileInputStream);
      kvs.put(key, ReadWriteIOUtils.readString(fileInputStream));
    }
  }

  /////////////////////////////////  Override  /////////////////////////////////

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipeStoreMetaKeeper that = (PipeStoreMetaKeeper) o;
    return Objects.equals(kvs, that.kvs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kvs);
  }

  @Override
  public String toString() {
    return "PipeStoreMetaKeeper{" + "kvs=" + kvs + '}';
  }
}
