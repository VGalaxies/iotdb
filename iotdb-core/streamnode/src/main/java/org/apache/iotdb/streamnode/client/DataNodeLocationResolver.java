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
 * specific language and permissions and limitations
 * under the License.
 */

package org.apache.iotdb.streamnode.client;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.confignode.rpc.thrift.TGetDataNodeLocationsResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Resolves writable DataNode endpoints from the ConfigNode so that the stream sink can write
 * results back to the real cluster instead of a hardcoded address.
 */
public final class DataNodeLocationResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeLocationResolver.class);

  private DataNodeLocationResolver() {
    // Utility class
  }

  /**
   * Fetches the readable DataNode client RPC endpoints from the ConfigNode and formats them as
   * "ip:port" session URLs.
   *
   * <p>Returns an empty list if no ConfigNode is reachable (e.g. in unit tests without a cluster)
   * or no readable DataNode exists; callers should fall back to the target's configured nodeUrls in
   * that case.
   */
  public static List<String> getReadableDataNodeUrls() {
    try (ConfigNodeClient client =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetDataNodeLocationsResp resp = client.getReadableDataNodeLocations();
      if (resp == null
          || resp.getStatus() == null
          || resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "Failed to fetch readable DataNode locations from ConfigNode: {}",
            resp == null ? "null response" : resp.getStatus());
        return Collections.emptyList();
      }
      List<String> urls = new ArrayList<>();
      for (TDataNodeLocation location : resp.getDataNodeLocationList()) {
        TEndPoint endpoint = location.getClientRpcEndPoint();
        if (endpoint != null) {
          urls.add(endpoint.getIp() + ":" + endpoint.getPort());
        }
      }
      return urls;
    } catch (Exception e) {
      LOGGER.warn(
          "Unable to reach ConfigNode for readable DataNode locations, "
              + "falling back to configured target nodeUrls",
          e);
      return Collections.emptyList();
    }
  }
}
