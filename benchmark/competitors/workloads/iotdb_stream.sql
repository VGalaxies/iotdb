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

-- IoTDB StreamNode (Ours) multi-granularity slice-reuse + evidence VARIATION workload

CREATE DATABASE IF NOT EXISTS bench_exp5;
USE bench_exp5;

CREATE TABLE sensor_raw (
    time TIMESTAMP TIME,
    device_id STRING TAG,
    val DOUBLE FIELD
);

CREATE TABLE sink_10s (
    time TIMESTAMP TIME,
    cnt INT64 FIELD,
    avg_v DOUBLE FIELD,
    p50 DOUBLE FIELD
);

CREATE TABLE sink_1m (
    time TIMESTAMP TIME,
    cnt INT64 FIELD,
    avg_v DOUBLE FIELD,
    p99 DOUBLE FIELD
);

CREATE TABLE sink_regime (
    time TIMESTAMP TIME,
    device_id STRING TAG,
    prev_v DOUBLE FIELD,
    curr_v DOUBLE FIELD,
    cnt INT64 FIELD,
    avg_v DOUBLE FIELD
);

CREATE STREAM exp5_10s
FROM sensor_raw
TUMBLE(10s, 0)
INTO sink_10s(time, cnt, avg_v, p50)
SELECT ${START_TIME}, count(*), avg(val), approx_percentile(val, 0.50) FROM ${ROWS}
WITH (
    enable_slice_reuse = true,
    reuse_sketch_type = 'kll',
    reuse_kll_k = 200,
    align_partition_watermark = true
);

CREATE STREAM exp5_1m
FROM sensor_raw
TUMBLE(1m, 0)
INTO sink_1m(time, cnt, avg_v, p99)
SELECT ${START_TIME}, count(*), avg(val), approx_percentile(val, 0.99) FROM ${ROWS}
WITH (
    enable_slice_reuse = true,
    reuse_sketch_type = 'kll',
    reuse_kll_k = 200,
    watermark = 1s
);

CREATE STREAM exp5_var
FROM sensor_raw PARTITION BY device_id
VARIATION(col => val, delta => 2.5)
INTO sink_regime(time, device_id, prev_v, curr_v, cnt, avg_v)
SELECT ${START_TIME}, ${1}, ${PREV_VALUE}, ${CURRENT_VALUE}, ${ROW_NUM}, avg(val) FROM ${ROWS}
WITH (
    variation_detector = 'evidence',
    variation_theta = 3.0,
    variation_epsilon = 2.0,
    variation_min_samples = 3,
    variation_adaptive_baseline = true
);

START STREAM exp5_10s;
START STREAM exp5_1m;
START STREAM exp5_var;
