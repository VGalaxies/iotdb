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

"""Deterministic benchmark dataset generator for StreamNode regime & window experiments."""

import csv
import json
from pathlib import Path
import numpy as np


OUT_DIR = Path(__file__).resolve().parent


def generate_pipeline_highfreq(seed: int = 42) -> dict:
    rng = np.random.default_rng(seed)
    n_points = 20_000
    step_ms = 100
    base_ts = 1_700_000_000_000

    # 25 ground-truth regime shifts (~every 750-850 samples, 26 regimes total)
    changepoints = sorted(rng.choice(np.arange(400, n_points - 400, 150), size=25, replace=False).tolist())
    # Ensure minimum separation >= 350 samples between ground-truth transitions
    filtered_cps = []
    for cp in changepoints:
        if not filtered_cps or cp - filtered_cps[-1] >= 350:
            filtered_cps.append(int(cp))
    while len(filtered_cps) < 25:
        candidate = int(rng.integers(400, n_points - 400))
        if all(abs(candidate - c) >= 350 for c in filtered_cps):
            filtered_cps.append(candidate)
    filtered_cps = sorted(filtered_cps)

    # Regime levels separated by alternating + / - jumps of 6..18 units (noise sigma ~ 0.6)
    regime_levels = [50.0]
    for idx in range(len(filtered_cps)):
        direction = 1.0 if idx % 2 == 0 else -1.0
        jump = float(rng.uniform(6.0, 16.0)) * direction
        regime_levels.append(regime_levels[-1] + jump)

    cp_set = set(filtered_cps)
    values = np.zeros(n_points, dtype=np.float64)
    regime_ids = np.zeros(n_points, dtype=np.int32)
    cur_regime = 0
    for i in range(n_points):
        if i in cp_set:
            cur_regime += 1
        regime_ids[i] = cur_regime
        values[i] = regime_levels[cur_regime] + rng.normal(0.0, 0.55)

    # Inject 80 transient 1-point and 2-point spikes well away from transitions
    spike_flags = np.zeros(n_points, dtype=np.int32)
    spikes_placed = 0
    while spikes_placed < 80:
        idx = int(rng.integers(50, n_points - 50))
        if any(abs(idx - c) < 25 for c in filtered_cps):
            continue
        if spike_flags[idx - 2 : idx + 3].any():
            continue
        width = 2 if (spikes_placed % 4 == 0) else 1
        amp = float(rng.uniform(7.5, 14.0)) * (1.0 if spikes_placed % 2 == 0 else -1.0)
        for w in range(width):
            values[idx + w] += amp
            spike_flags[idx + w] = 1
        spikes_placed += 1

    csv_path = OUT_DIR / "pipeline_highfreq.csv"
    cp_timestamps = []
    spike_timestamps = []
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp_ms",
            "device_id",
            "partition_id",
            "arrival_ts_ms",
            "value",
            "is_true_changepoint",
            "is_injected_spike",
            "regime_id",
        ])
        for i in range(n_points):
            ts = base_ts + i * step_ms
            part_id = i % 4
            # Simulated staggered partition skew up to 2.5s on partition 3
            skew = int(rng.integers(0, 300)) + (1800 if part_id == 3 else 0)
            is_cp = 1 if i in cp_set else 0
            is_sp = int(spike_flags[i])
            if is_cp:
                cp_timestamps.append(ts)
            if is_sp:
                spike_timestamps.append(ts)
            writer.writerow([
                ts,
                f"pipe_dev_{part_id}",
                part_id,
                ts + skew,
                f"{values[i]:.6f}",
                is_cp,
                is_sp,
                int(regime_ids[i]),
            ])

    return {
        "dataset": "pipeline_highfreq",
        "path": str(csv_path),
        "rows": n_points,
        "changepoint_indices": filtered_cps,
        "changepoint_timestamps": cp_timestamps,
        "injected_spike_count": int(spike_flags.sum()),
        "spike_timestamps": spike_timestamps,
    }


def generate_cisdi_regime_jitter(seed: int = 108) -> dict:
    rng = np.random.default_rng(seed)
    n_points = 15_000
    base_ts = 1_700_100_000_000

    # 20 regime transitions across heterogeneous industrial rolling/annealing plateaus
    candidates = np.linspace(450, n_points - 450, 20, dtype=int).tolist()
    filtered_cps = [int(c + rng.integers(-60, 61)) for c in candidates]
    filtered_cps = sorted(filtered_cps)
    cp_set = set(filtered_cps)

    # Distinct plateau noise sigmas (0.35 quiet -> 2.3 high-jitter -> 0.50 -> 1.8)
    sigma_schedule = [0.40, 2.20, 0.50, 1.90, 0.35, 2.35, 0.60, 1.60]
    levels = [420.0]
    sigmas = [sigma_schedule[0]]
    for idx in range(len(filtered_cps)):
        sig = sigma_schedule[(idx + 1) % len(sigma_schedule)]
        sigmas.append(sig)
        # Clear process state shift >= max(8.0, 4.5 * max_sigma)
        step_mag = float(rng.uniform(9.5, 22.0))
        direction = 1.0 if idx % 2 == 0 else -1.0
        levels.append(levels[-1] + direction * step_mag)

    # Generate timestamps with 25 intermittent telemetry gaps (> 5000 ms) inside plateaus
    timestamps = np.zeros(n_points, dtype=np.int64)
    gap_indices = set(
        idx for idx in rng.choice(np.arange(100, n_points - 100), size=40, replace=False)
        if all(abs(int(idx) - c) > 30 for c in filtered_cps)
    )
    t = base_ts
    for i in range(n_points):
        if i in gap_indices:
            t += int(rng.integers(5500, 12000))
        else:
            t += 200  # 5 Hz industrial PLC polling
        timestamps[i] = t

    values = np.zeros(n_points, dtype=np.float64)
    regime_ids = np.zeros(n_points, dtype=np.int32)
    cur_regime = 0
    ar_noise = 0.0
    for i in range(n_points):
        if i in cp_set:
            cur_regime += 1
        regime_ids[i] = cur_regime
        sig = sigmas[cur_regime]
        ar_noise = 0.25 * ar_noise + rng.normal(0.0, sig * 0.92)
        values[i] = levels[cur_regime] + ar_noise

    # Inject 50 1-2 point sensor glitches + 6 short 2-point alternating vibration bursts
    spike_flags = np.zeros(n_points, dtype=np.int32)
    placed = 0
    while placed < 50:
        idx = int(rng.integers(40, n_points - 40))
        if any(abs(idx - c) < 25 for c in filtered_cps) or spike_flags[idx - 2 : idx + 3].any():
            continue
        width = 2 if (placed % 5 == 0) else 1
        sig = sigmas[regime_ids[idx]]
        amp = float(rng.uniform(10.0, 20.0)) * (1.0 if placed % 2 == 0 else -1.0)
        for w in range(width):
            values[idx + w] += amp + 2.0 * sig
            spike_flags[idx + w] = 1
        placed += 1

    csv_path = OUT_DIR / "cisdi_regime_jitter.csv"
    cp_timestamps = []
    spike_timestamps = []
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp_ms",
            "device_id",
            "value",
            "is_true_changepoint",
            "is_injected_spike",
            "regime_id",
            "regime_sigma",
        ])
        for i in range(n_points):
            is_cp = 1 if i in cp_set else 0
            is_sp = int(spike_flags[i])
            if is_cp:
                cp_timestamps.append(int(timestamps[i]))
            if is_sp:
                spike_timestamps.append(int(timestamps[i]))
            writer.writerow([
                int(timestamps[i]),
                "cisdi_roll_stand_01",
                f"{values[i]:.6f}",
                is_cp,
                is_sp,
                int(regime_ids[i]),
                f"{sigmas[regime_ids[i]]:.3f}",
            ])

    return {
        "dataset": "cisdi_regime_jitter",
        "path": str(csv_path),
        "rows": n_points,
        "changepoint_indices": filtered_cps,
        "changepoint_timestamps": cp_timestamps,
        "injected_spike_count": int(spike_flags.sum()),
        "spike_timestamps": spike_timestamps,
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    summary = {
        "pipeline_highfreq": generate_pipeline_highfreq(),
        "cisdi_regime_jitter": generate_cisdi_regime_jitter(),
    }
    summary_path = OUT_DIR / "ground_truth_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Generated datasets in {OUT_DIR}:")
    for k, v in summary.items():
        print(
            f"  - {k}: rows={v['rows']}, true_changepoints={len(v['changepoint_indices'])}, "
            f"injected_spike_points={v['injected_spike_count']}"
        )


if __name__ == "__main__":
    main()
