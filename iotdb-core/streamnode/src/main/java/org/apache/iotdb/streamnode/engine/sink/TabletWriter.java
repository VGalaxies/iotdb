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

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.streamnode.exception.NonRetryableException;
import org.apache.iotdb.streamnode.exception.RetryableException;

import org.apache.tsfile.write.record.Tablet;

public class TabletWriter {
  private final ITableSessionPool sessionPool;

  public TabletWriter(ITableSessionPool sessionPool) {
    this.sessionPool = sessionPool;
  }

  public ITableSessionPool getSessionPool() {
    return sessionPool;
  }

  public void write(Tablet tablet) throws Exception {
    try {
      try (ITableSession session = sessionPool.getSession()) {
        session.insert(tablet);
      }
    } catch (IoTDBConnectionException e) {
      throw new RetryableException("Connection error", e);
    } catch (StatementExecutionException e) {
      if (isRetryable(e)) throw new RetryableException("Retryable", e);
      throw new NonRetryableException("Statement error", e);
    }
  }

  private boolean isRetryable(StatementExecutionException exception) {
    String message = exception.getMessage();
    if (message != null) {
      return message.contains("Retryable")
          || message.contains("timeout")
          || message.contains("Connection")
          || message.contains("Network");
    }
    return false;
  }
}
