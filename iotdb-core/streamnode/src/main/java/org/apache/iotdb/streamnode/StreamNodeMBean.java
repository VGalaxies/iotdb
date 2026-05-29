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

package org.apache.iotdb.streamnode;

public interface StreamNodeMBean {
  /**
   * Get the StreamNode ID assigned by ConfigNode after registration.
   *
   * @return StreamNode ID, -1 if not registered yet
   */
  int getStreamNodeId();

  /**
   * Get the internal RPC address that StreamNode binds to.
   *
   * @return Internal address
   */
  String getInternalAddress();

  /**
   * Get the internal RPC port for CN->SN communication.
   *
   * @return Internal port
   */
  int getInternalPort();

  /**
   * Get the number of currently total stream tasks.
   *
   * @return Task count
   */
  int getTaskCount();

  /**
   * Get the number of currently running stream tasks.
   *
   * @return Running task count
   */
  int getRunningTaskCount();

  /**
   * Get the cluster name this StreamNode belongs to.
   *
   * @return Cluster name
   */
  String getClusterName();

  /**
   * Get the cluster ID this StreamNode joined.
   *
   * @return Cluster ID
   */
  String getClusterId();

  /**
   * Get the node status
   *
   * @return
   */
  String getNodeStatus();
}
