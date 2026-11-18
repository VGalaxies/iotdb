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

-- TimescaleDB Continuous Aggregate Competitor Workload (PG16)

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS sensor_raw (
    time        TIMESTAMPTZ NOT NULL,
    device_id   TEXT        NOT NULL,
    val         DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('sensor_raw', 'time', if_not_exists => TRUE);

CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_10s
WITH (timescaledb.continuous) AS
SELECT time_bucket('10 seconds', time) AS bucket,
       COUNT(*) AS cnt,
       AVG(val) AS avg_v,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY val) AS p99
FROM sensor_raw
GROUP BY bucket;

CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', time) AS bucket,
       COUNT(*) AS cnt,
       AVG(val) AS avg_v,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY val) AS p99
FROM sensor_raw
GROUP BY bucket;

SELECT add_continuous_aggregate_policy('cagg_10s',
    start_offset => INTERVAL '5 minutes',
    end_offset => INTERVAL '10 seconds',
    schedule_interval => INTERVAL '10 seconds');

SELECT add_continuous_aggregate_policy('cagg_1m',
    start_offset => INTERVAL '15 minutes',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');
