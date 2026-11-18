--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--

-- TDengine 3.x Continuous Stream Competitor Workload

CREATE DATABASE IF NOT EXISTS bench_exp5 PRECISION 'ms' KEEP 365d;
USE bench_exp5;

CREATE STABLE sensor_raw (ts TIMESTAMP, val DOUBLE) TAGS (device_id BINARY(32));
CREATE TABLE sink_10s (ts TIMESTAMP, cnt BIGINT, avg_v DOUBLE, p99 DOUBLE);
CREATE TABLE sink_1m  (ts TIMESTAMP, cnt BIGINT, avg_v DOUBLE, p99 DOUBLE);

CREATE STREAM exp5_10s INTO sink_10s AS
SELECT _WSTART, COUNT(*), AVG(val), PERCENTILE(val, 99)
FROM sensor_raw
INTERVAL(10s);

CREATE STREAM exp5_1m INTO sink_1m AS
SELECT _WSTART, COUNT(*), AVG(val), PERCENTILE(val, 99)
FROM sensor_raw
INTERVAL(1m);
