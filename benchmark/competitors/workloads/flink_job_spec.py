#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Apache Flink 1.19 + IoTDB external-stream competitor SQL specification for Exp-5."""

FLINK_IOTDB_SQL = """
-- Flink external pipeline reading CDC/subscription stream from IoTDB DataNode and writing
-- 10s & 1m TUMBLE window aggregations back over network RPC (2 extra network hops vs StreamNode).
CREATE TABLE iotdb_sensor_source (
    event_time TIMESTAMP(3),
    device_id STRING,
    val DOUBLE,
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'iotdb',
    'nodeUrls' = 'iotdb-ours:6667'
);

CREATE TABLE iotdb_sink_10s (
    win_start TIMESTAMP(3),
    cnt BIGINT,
    avg_v DOUBLE
) WITH (
    'connector' = 'iotdb',
    'nodeUrls' = 'iotdb-ours:6667'
);

INSERT INTO iotdb_sink_10s
SELECT window_start, COUNT(*), AVG(val)
FROM TABLE(
    TUMBLE(TABLE iotdb_sensor_source, DESCRIPTOR(event_time), INTERVAL '10' SECONDS)
)
GROUP BY window_start, window_end;
"""

if __name__ == "__main__":
    print(FLINK_IOTDB_SQL.strip())
