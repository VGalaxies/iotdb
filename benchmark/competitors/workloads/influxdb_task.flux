//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

// InfluxDB 2.7 Scheduled Flux Window Task Competitor Workload

option task = {name: "exp5_window_agg_10s_and_1m", every: 10s, offset: 2s}

base = from(bucket: "tsbench")
    |> range(start: -2m)
    |> filter(fn: (r) => r._measurement == "sensor_raw" and r._field == "val")

agg10s = base
    |> aggregateWindow(every: 10s, fn: mean, createEmpty: false)
    |> set(key: "_measurement", value: "sink_10s")
    |> to(bucket: "tsbench", org: "bench_org")

agg1m = base
    |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
    |> set(key: "_measurement", value: "sink_1m")
    |> to(bucket: "tsbench", org: "bench_org")
