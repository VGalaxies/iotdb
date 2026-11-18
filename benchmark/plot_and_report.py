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

"""Generate 300-DPI publication figures and RESULTS_REPORT.md for all cluster & micro benchmarks."""

import json
import platform
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


BENCH_ROOT = Path(__file__).resolve().parent
RESULTS_DIR = BENCH_ROOT / "results"
FIGURES_DIR = BENCH_ROOT / "figures"

plt.rcParams.update({
    "font.size": 10,
    "axes.titlesize": 11,
    "axes.labelsize": 10,
    "legend.fontsize": 8.5,
    "figure.dpi": 150,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "axes.spines.top": False,
    "axes.spines.right": False,
})

COLORS = {
    "ours": "#1f77b4",
    "unaligned": "#d62728",
    "buffering": "#ff7f0e",
    "raw_delta": "#d62728",
    "duration": "#ff7f0e",
    "adwin": "#2ca02c",
    "optwin": "#9467bd",
    "pelt": "#7f7f7f",
    "tdigest": "#ff7f0e",
    "noreuse": "#d62728",
}


def plot_exp1(micro_df: pd.DataFrame, cluster_df: pd.DataFrame | None) -> None:
    if cluster_df is not None and not cluster_df.empty:
        fig, axes = plt.subplots(1, 3, figsize=(15.5, 4.3))
        ax_skew, ax_mem, ax_cls = axes
    else:
        fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.2))
        ax_skew, ax_mem = axes
        ax_cls = None

    skew_df = micro_df[micro_df["scenario"] == "skew_sweep"].copy()
    method_meta = [
        ("SinglePartitionUnaligned", "Unaligned Single-Partition", COLORS["unaligned"], "o--"),
        ("ClientSideRowBuffering", "Client Row-Buffering", COLORS["buffering"], "s-."),
        ("Ours_PartitionProgressTracker", "Ours (PartitionProgressTracker)", COLORS["ours"], "^-"),
    ]
    for key, label, color, fmt in method_meta:
        sub = skew_df[skew_df["method"] == key].sort_values("skew_ms")
        ax_skew.plot(
            sub["skew_ms"] / 1000.0,
            sub["completeness_recall"] * 100.0,
            fmt,
            color=color,
            linewidth=2.0,
            markersize=6,
            label=label,
        )
        ax_mem.plot(
            sub["skew_ms"] / 1000.0,
            sub["buffered_rows_peak"],
            fmt,
            color=color,
            linewidth=2.0,
            markersize=6,
            label=label,
        )

    ax_skew.set_title("Exp-1a: Completeness vs. Skew")
    ax_skew.set_xlabel("Partition Skew (s)")
    ax_skew.set_ylabel("Row Completeness (%)")
    ax_skew.set_ylim(0, 108)
    ax_skew.legend(loc="lower left")

    ax_mem.set_title("Exp-1b: Alignment Buffering Footprint")
    ax_mem.set_xlabel("Partition Skew (s)")
    ax_mem.set_ylabel("Peak Buffered Rows")
    ax_mem.legend(loc="upper left")

    if ax_cls is not None:
        order = ["Unaligned_Default", "Ours_Aligned_Watermark"]
        sub_c = cluster_df.set_index("group").loc[order]
        x = np.arange(len(order))
        bars = ax_cls.bar(
            x,
            sub_c["completeness_recall_mean"] * 100.0,
            yerr=sub_c["completeness_recall_std"] * 100.0,
            capsize=5,
            color=[COLORS["unaligned"], COLORS["ours"]],
            width=0.5,
        )
        ax_cls.set_title("Exp-1c: Live 3-JVM 4-Region Alignment (R=3)")
        ax_cls.set_xticks(x)
        ax_cls.set_xticklabels(["Unaligned\nDefault", "Ours Aligned\nWatermark"])
        ax_cls.set_ylabel("Measured Completeness (%)")
        ax_cls.set_ylim(0, 112)
        for b in bars:
            h = b.get_height()
            ax_cls.text(b.get_x() + b.get_width() / 2.0, h + 1.5, f"{h:.1f}%", ha="center", va="bottom", fontsize=8.5)

    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "exp1_distributed_skew.png", dpi=300)
    plt.close(fig)


def plot_exp2_methods(micro_df: pd.DataFrame, cluster_df: pd.DataFrame | None) -> None:
    if cluster_df is not None and not cluster_df.empty:
        fig, axes = plt.subplots(1, 2, figsize=(12.6, 4.6))
        sub_live = cluster_df.set_index("method")
        order_live = [
            ("Raw_Delta", "Raw Delta"),
            ("Duration_Rule", "Duration Rule"),
            ("ADWIN", "ADWIN"),
            ("OPTWIN", "OPTWIN"),
            ("Ours_Evidence_Backtrack", "Ours (Evidence+Backtrack)"),
        ]
        x = np.arange(len(order_live))
        w = 0.25
        axes[0].bar(x - w, [float(sub_live.loc[m, "precision_mean"]) for m, _ in order_live], w, label="Precision", color="#6baed6")
        axes[0].bar(x, [float(sub_live.loc[m, "recall_mean"]) for m, _ in order_live], w, label="Recall", color="#74c476")
        f1_bars = axes[0].bar(
            x + w,
            [float(sub_live.loc[m, "f1_mean"]) for m, _ in order_live],
            w,
            yerr=[float(sub_live.loc[m, "f1_std"]) for m, _ in order_live],
            capsize=3,
            label="F1 Score",
            color="#1f77b4",
        )
        f1_bars[-1].set_color("#08306b")
        axes[0].set_title("Exp-2a: Live 3-JVM SQL VARIATION (CISDI Jitter, R=3)")
        axes[0].set_xticks(x)
        axes[0].set_xticklabels([lbl for _, lbl in order_live], rotation=22, ha="right")
        axes[0].set_ylabel("Score")
        axes[0].set_ylim(0, 1.15)
        axes[0].legend(loc="upper left", ncol=3)

        # Panel B: Pipeline High-Freq (6 methods including offline PELT oracle)
        sub_pipe = micro_df[micro_df["dataset"] == "pipeline_highfreq"].set_index("method")
        order_pipe = order_live + [("Offline_PELT_Oracle", "Offline PELT (Oracle)")]
        xp = np.arange(len(order_pipe))
        axes[1].bar(xp - w, [float(sub_pipe.loc[m, "precision"]) for m, _ in order_pipe], w, label="Precision", color="#6baed6")
        axes[1].bar(xp, [float(sub_pipe.loc[m, "recall"]) for m, _ in order_pipe], w, label="Recall", color="#74c476")
        f1p = axes[1].bar(xp + w, [float(sub_pipe.loc[m, "f1"]) for m, _ in order_pipe], w, label="F1 Score", color="#1f77b4")
        f1p[4].set_color("#08306b")
        axes[1].set_title("Exp-2b: Pipeline High-Freq (10Hz + Spikes, vs. PELT)")
        axes[1].set_xticks(xp)
        axes[1].set_xticklabels([lbl for _, lbl in order_pipe], rotation=22, ha="right")
        axes[1].set_ylabel("Score")
        axes[1].set_ylim(0, 1.15)
        axes[1].legend(loc="upper left", ncol=3)
    else:
        fig, axes = plt.subplots(1, 2, figsize=(12.2, 4.5))
        datasets = ["pipeline_highfreq", "cisdi_regime_jitter"]
        titles = ["Pipeline High-Freq", "CISDI Industrial Jitter"]
        order = [
            ("Raw_Delta", "Raw Delta"),
            ("Duration_Rule", "Duration Rule"),
            ("ADWIN", "ADWIN"),
            ("OPTWIN", "OPTWIN"),
            ("Ours_Evidence_Backtrack", "Ours (Evidence+Backtrack)"),
            ("Offline_PELT_Oracle", "Offline PELT"),
        ]
        x = np.arange(len(order))
        w = 0.26
        for ax, ds, title in zip(axes, datasets, titles):
            sub = micro_df[micro_df["dataset"] == ds].set_index("method")
            ax.bar(x - w, [float(sub.loc[m, "precision"]) for m, _ in order], w, label="Precision", color="#6baed6")
            ax.bar(x, [float(sub.loc[m, "recall"]) for m, _ in order], w, label="Recall", color="#74c476")
            ax.bar(x + w, [float(sub.loc[m, "f1"]) for m, _ in order], w, label="F1 Score", color="#1f77b4")
            ax.set_title(title)
            ax.set_xticks(x)
            ax.set_xticklabels([lbl for _, lbl in order], rotation=22, ha="right")
            ax.set_ylabel("Score")
            ax.set_ylim(0, 1.12)
            ax.legend(loc="upper left", ncol=3)

    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "exp2_regime_detection_comparison.png", dpi=300)
    plt.close(fig)


def plot_exp2_ablation(df: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.2))
    variants = [
        ("Full_Ours", "Full Ours"),
        ("wo_TimeDecay", "w/o Time Decay"),
        ("wo_AdaptiveBaseline", "w/o Adaptive MAD/IQR"),
        ("wo_OnsetBacktracking", "w/o Onset Backtrack"),
    ]
    x = np.arange(len(variants))
    width = 0.35

    sub_pipe = df[df["dataset"] == "pipeline_highfreq"].set_index("method")
    sub_cisdi = df[df["dataset"] == "cisdi_regime_jitter"].set_index("method")

    axes[0].bar(x - width / 2, [float(sub_pipe.loc[v, "f1"]) for v, _ in variants], width, label="Pipeline High-Freq", color="#1f77b4")
    axes[0].bar(x + width / 2, [float(sub_cisdi.loc[v, "f1"]) for v, _ in variants], width, label="CISDI Jitter", color="#ff7f0e")
    axes[0].set_title("Exp-2c Ablation: Change-Point F1 Score")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels([lbl for _, lbl in variants], rotation=18, ha="right")
    axes[0].set_ylabel("F1 Score")
    axes[0].set_ylim(0, 1.12)
    axes[0].legend()

    axes[1].bar(x - width / 2, [float(sub_pipe.loc[v, "mae_samples"]) for v, _ in variants], width, label="Pipeline High-Freq", color="#1f77b4")
    axes[1].bar(x + width / 2, [float(sub_cisdi.loc[v, "mae_samples"]) for v, _ in variants], width, label="CISDI Jitter", color="#ff7f0e")
    axes[1].set_title("Exp-2d Ablation: Boundary Localization MAE (samples)")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels([lbl for _, lbl in variants], rotation=18, ha="right")
    axes[1].set_ylabel("Mean Error (samples)")
    axes[1].legend()

    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "exp2_ablation_study.png", dpi=300)
    plt.close(fig)


def plot_exp3(reuse_df: pd.DataFrame, sketch_df: pd.DataFrame, cluster_df: pd.DataFrame | None) -> None:
    cols = 3 if (cluster_df is not None and not cluster_df.empty) else 2
    fig, axes = plt.subplots(1, cols, figsize=(15.5 if cols == 3 else 11.8, 4.3))

    fan_df = reuse_df[reuse_df["workload"] == "fanout_sweep"].copy()
    for method, label, color, fmt in [
        ("No_Reuse_Independent_Raw_Scan", "No Reuse (Independent Raw)", COLORS["noreuse"], "o--"),
        ("Slice_Reuse_TDigest", "Slice Reuse + TDigest", COLORS["tdigest"], "s-."),
        ("Ours_Slice_Reuse_KLL", "Ours (Slice Reuse + KLL + DAG)", COLORS["ours"], "^-"),
    ]:
        sub = fan_df[fan_df["method"] == method].sort_values("fanout")
        axes[0].plot(
            sub["fanout"],
            sub["speedup_vs_noreuse"],
            fmt,
            color=color,
            linewidth=2.0,
            markersize=6,
            label=label,
        )

    axes[0].set_title("Exp-3a: Micro Speedup vs. Fan-out")
    axes[0].set_xlabel("Concurrent Coarse Window Consumers")
    axes[0].set_ylabel("Speedup vs. No-Reuse (x)")
    axes[0].legend(loc="upper left")

    dist_sub = sketch_df[sketch_df["distribution"] == "LogNormal_HeavyTail"].copy().set_index("sketch_method")
    order = ["TDigest_Default", "KLL_k128", "KLL_k200", "KLL_k400"]
    labels = ["TDigest", "KLL(128)", "KLL(200)", "KLL(400)"]
    x = np.arange(len(order))
    width = 0.26
    axes[1].bar(x - width, [float(dist_sub.loc[m, "p50_norm_rank_err"]) * 100 for m in order], width, label="P50 Err (%)", color="#6baed6")
    axes[1].bar(x, [float(dist_sub.loc[m, "p90_norm_rank_err"]) * 100 for m in order], width, label="P90 Err (%)", color="#74c476")
    axes[1].bar(x + width, [float(dist_sub.loc[m, "p99_norm_rank_err"]) * 100 for m in order], width, label="P99 Err (%)", color="#1f77b4")
    axes[1].set_title("Exp-3b: LogNormal Multi-Slice Rank Err (%)")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(labels)
    axes[1].set_ylabel("Normalized Rank Error (%)")
    axes[1].legend()

    if cols == 3:
        m_order = ["No_Reuse_Independent", "Slice_Reuse_TDigest", "Ours_Slice_Reuse_KLL"]
        m_labels = ["No Reuse\n(5 Streams)", "Reuse +\nTDigest", "Ours Reuse +\nKLL (k=200)"]
        sub_c = cluster_df.set_index("method").loc[m_order]
        xc = np.arange(len(m_order))
        use_cpu = float(sub_c["sn_cpu_ms_mean"].max()) > 0.0
        vals = sub_c["sn_cpu_ms_mean"] if use_cpu else sub_c["e2e_drain_ms_mean"]
        errs = sub_c["sn_cpu_ms_std"] if use_cpu else sub_c["e2e_drain_ms_std"]
        bars = axes[2].bar(
            xc,
            vals,
            yerr=errs,
            capsize=5,
            color=[COLORS["noreuse"], COLORS["tdigest"], COLORS["ours"]],
            width=0.52,
        )
        axes[2].set_title(
            "Exp-3c: Live 3-JVM StreamNode CPU (R=3)"
            if use_cpu
            else "Exp-3c: Live 3-JVM E2E Drain Latency (R=3)"
        )
        axes[2].set_xticks(xc)
        axes[2].set_xticklabels(m_labels)
        axes[2].set_ylabel("StreamNode CPU Time (ms)" if use_cpu else "E2E Drain Latency (ms)")
        for b in bars:
            h = b.get_height()
            axes[2].text(b.get_x() + b.get_width() / 2.0, h * 1.02, f"{h:.1f}ms", ha="center", va="bottom", fontsize=8.5)

    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "exp3_slice_reuse_and_kll.png", dpi=300)
    plt.close(fig)


def plot_exp4(df: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(15.5, 4.4))
    modes = [
        ("Baseline_Raw_Write", "Raw Write\n(No Trigger)"),
        ("Client_SQL_Polling", "Client SQL\nPolling"),
        ("Sync_WritePath_Trigger", "Sync Write\nTrigger JAR"),
        ("Ours_StreamNode_Async_Reuse", "Ours StreamNode\n(Async + Reuse)"),
    ]
    sub = df.set_index("mode")
    x = np.arange(len(modes))

    yerr = (
        sub["write_throughput_std"] / 1000.0
        if "write_throughput_std" in sub.columns
        else None
    )
    bars = axes[0].bar(
        x,
        [float(sub.loc[m, "write_throughput_pts_sec"]) / 1000.0 for m, _ in modes],
        yerr=[float(yerr.loc[m]) for m, _ in modes] if yerr is not None else None,
        capsize=4,
        color=["#7f7f7f", "#2ca02c", "#d62728", "#1f77b4"],
        width=0.55,
    )
    axes[0].set_title("Exp-4a: Live 3-JVM Client Write Throughput (R=3)")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels([lbl for _, lbl in modes])
    axes[0].set_ylabel("Write Throughput (k pts/s)")
    for b in bars:
        h = b.get_height()
        axes[0].text(b.get_x() + b.get_width() / 2.0, h * 1.02, f"{h:.1f}k", ha="center", va="bottom", fontsize=8.5)

    w = 0.25
    axes[1].bar(x - w, [float(sub.loc[m, "write_p50_ms"]) for m, _ in modes], w, label="Write P50", color="#6baed6")
    axes[1].bar(x, [float(sub.loc[m, "write_p95_ms"]) for m, _ in modes], w, label="Write P95", color="#fd8d3c")
    axes[1].bar(x + w, [float(sub.loc[m, "write_p99_ms"]) for m, _ in modes], w, label="Write P99", color="#d62728")
    axes[1].set_title("Exp-4b: Client Write Latency Tail (P50/P95/P99)")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels([lbl for _, lbl in modes])
    axes[1].set_ylabel("Latency (ms)")
    axes[1].legend()

    if "cn_rss_mb" in sub.columns:
        cn = np.array([float(sub.loc[m, "cn_rss_mb"]) for m, _ in modes])
        dn = np.array([float(sub.loc[m, "dn_rss_mb"]) for m, _ in modes])
        sn = np.array([float(sub.loc[m, "sn_rss_mb"]) for m, _ in modes])
        axes[2].bar(x, cn, width=0.5, label="ConfigNode (-Xmx1536M)", color="#9ecae1")
        axes[2].bar(x, dn, width=0.5, bottom=cn, label="DataNode (-Xmx2304M)", color="#4292c6")
        axes[2].bar(x, sn, width=0.5, bottom=cn + dn, label="StreamNode (-Xmx3379M)", color="#084594")
        axes[2].set_title("Exp-4c: 3-Node Memory Footprint (RSS MB)")
        axes[2].set_xticks(x)
        axes[2].set_xticklabels([lbl for _, lbl in modes])
        axes[2].set_ylabel("Resident Set Size (MB)")
        axes[2].legend(loc="upper left", fontsize=8)
    else:
        axes[2].bar(x, [float(sub.loc[m, "cluster_rss_mb"]) for m, _ in modes], color="#1f77b4", width=0.5)
        axes[2].set_title("Exp-4c: Cluster RSS (MB)")
        axes[2].set_xticks(x)
        axes[2].set_xticklabels([lbl for _, lbl in modes])
        axes[2].set_ylabel("RSS (MB)")

    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "exp4_iotdb_architecture_e2e.png", dpi=300)
    plt.close(fig)


def df_to_md(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
    ]
    for _, row in df.iterrows():
        formatted = []
        for c in cols:
            v = row[c]
            if isinstance(v, (float, np.floating)):
                formatted.append(f"{v:.4f}" if abs(v) < 10 else f"{v:.2f}")
            else:
                formatted.append(str(v))
        lines.append("| " + " | ".join(formatted) + " |")
    return "\n".join(lines)


def write_report(
    exp1_cls: pd.DataFrame,
    exp2_cls: pd.DataFrame,
    exp3_cls: pd.DataFrame,
    exp4_cls: pd.DataFrame,
    exp1_micro: pd.DataFrame,
    exp2_micro: pd.DataFrame,
    exp2_abl: pd.DataFrame,
    exp3_micro: pd.DataFrame,
    exp3_sk: pd.DataFrame,
    docker_status: dict,
) -> None:
    report = f"""<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Experimental Evaluation & Competitor Comparison Report

## Cluster Deployment & Fairness Methodology

- **Host Platform**: {platform.platform()} ({platform.machine()}), JDK 17
- **Cluster Topology**: Three independent Java OS processes launched cleanly per test unit:
  - **ConfigNode**: `-Xms1536M -Xmx1536M -Xmn256M`
  - **DataNode**: `-Xms2304M -Xmx2304M -Xmn768M`
  - **StreamNode**: `-Xms3379M -Xmx3379M -Xmn820M`
- **Isolation & State Hygiene**: Prior to **every** individual test unit (`42` total standalone cluster lifecycles = `Exp-1 (6) + Exp-2 (15) + Exp-3 (9) + Exp-4 (12)`), all old JVMs are stopped and verified unbound across cluster ports, and all `data`, `logs`, `ext`, and `iotdb-subscription` directories on ConfigNode, DataNode, StreamNode, and client scratch are completely wiped to eliminate WAL or subscription-cursor carryover.
- **Alternating Serial Execution**: Each experiment executes `R = 3` rounds serially on the idle machine, rotating group order cyclically (`(step + round) % G`) across rounds to eliminate thermal/order bias. All per-trial measurements are stored in `benchmark/results/cluster_raw_trials.csv`.

---

## Exp-1: Multi-Region Distributed Alignment & Skew Immunity

![Exp-1 Distributed Skew](figures/exp1_distributed_skew.png)

### Live 3-JVM 4-DataRegion Alignment (`R = 3` Alternating Rounds)

{df_to_md(exp1_cls)}

### Skew Sensitivity, Idle Heartbeat & Epoch-Fencing Micro-Benchmark

{df_to_md(exp1_micro)}

---

## Exp-2: Regime-Aware `VARIATION` Boundary Detection & 4-Way Ablation

![Exp-2 Detection Comparison](figures/exp2_regime_detection_comparison.png)

![Exp-2 Ablation Study](figures/exp2_ablation_study.png)

### Live 3-JVM SQL `VARIATION` Benchmark on `cisdi_regime_jitter.csv` (`R = 3` Alternating Rounds)

{df_to_md(exp2_cls)}

### Engine Micro-Benchmark (6 Methods × 2 Datasets, incl. Offline PELT Oracle)

{df_to_md(exp2_micro)}

### 4-Way Ablation Study

{df_to_md(exp2_abl)}

---

## Exp-3: Lifecycle-Constrained Base-Slice Reuse & KLL Quantile Sketch

![Exp-3 Slice Reuse and KLL](figures/exp3_slice_reuse_and_kll.png)

### Live 3-JVM 5-Stream Workload (`s_10s -> s_1m(P50/P90/P99) -> s_5m(P99)`, `R = 3` Rounds)

{df_to_md(exp3_cls)}

### Fan-out (`1..16`) & Multi-Hop DAG (`10s -> 1m -> 5m -> 1h`) Micro-Benchmark

{df_to_md(exp3_micro)}

### Quantile Sketch Rank Error & Serialized Footprint

{df_to_md(exp3_sk)}

---

## Exp-4: End-to-End Trigger Architecture Comparison (`R = 3` Alternating Rounds)

![Exp-4 Architecture E2E](figures/exp4_iotdb_architecture_e2e.png)

{df_to_md(exp4_cls)}

---

## Exp-5: Uniform-Quota Competitor Stack Setup (`4 vCPU / 8 GiB RAM`)

| System | Native DB Engine Separation | Distributed Partition Watermark Alignment | Adaptive Regime `VARIATION` Trigger | Multi-Granularity Slice Reuse + KLL Merge | Late-Data Recompute + Lossless Fallback |
| --- | --- | --- | --- | --- | --- |
| **Apache IoTDB + StreamNode (Ours)** | Yes (DataNode Storage + StreamNode Compute) | Yes (`PartitionProgressTracker` + Epoch Fencing) | Yes (Time-Weighted Evidence + Median/MAD + Backtrack) | Yes (`10s -> 1m -> 5m -> 1h` DAG + Single KLL Sketch) | Yes (`stateVersion` + Raw-Scan Fallback) |
| **TDengine 3.x (`CREATE STREAM`)** | No (Embedded in vnode) | Watermark / Partitioned | Fixed `STATE_WINDOW` | Independent `INTERVAL` streams | Partial (`FILL`/recalc) |
| **TimescaleDB (`Continuous Aggregates`)** | No (PostgreSQL extension) | Refresh-interval scheduled | No native regime trigger | Hierarchical CAGG (exact per-bucket refresh) | Re-materialization policy |
| **InfluxDB 2.7 (`Flux Task`)** | No (TSM + Task engine) | Periodic `every` polling | Scripted custom Flux | Independent `aggregateWindow` tasks | Re-query window range |
| **Apache Flink + IoTDB** | External cluster (2 extra RPC hops) | Watermark per subtask | Custom `KeyedProcessFunction` | Manual state wiring (`DataStream`) | Checkpoint / Savepoint replay |

- **Docker Container Runner Status**: `{docker_status.get("status", "UNKNOWN")}` — {docker_status.get("reason", "N/A")}

---

## Reproducibility Commands

```bash
# 1. Regenerate deterministic datasets
benchmark/.venv/bin/python benchmark/datasets/generate_datasets.py

# 2. Run Exp-1..3 engine & reuse micro-benchmarks
mvn exec:java -nsu -pl iotdb-core/streamnode -Dexec.classpathScope=test \\
  -Dexec.mainClass=org.apache.iotdb.streamnode.benchmark.ThesisMicroBenchmarkRunner

# 3. Run 42-trial isolated 3-JVM cluster benchmark suite (Exp-1..4, alternating rounds)
cd integration-test && java \\
  --add-opens=java.base/java.lang=ALL-UNNAMED \\
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \\
  --add-opens=java.base/java.util=ALL-UNNAMED \\
  --add-opens=java.base/java.nio=ALL-UNNAMED \\
  --add-opens=java.base/java.io=ALL-UNNAMED \\
  --add-opens=java.base/java.net=ALL-UNNAMED \\
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \\
  -cp "target/test-classes:target/classes:$(cat target/e2e_cp.txt)" \\
  org.apache.iotdb.streamnode.benchmark.ThesisE2EBenchmarkRunner

# 4. Generate 300-DPI PNG figures & this report
benchmark/.venv/bin/python benchmark/plot_and_report.py
```
"""
    (BENCH_ROOT / "RESULTS_REPORT.md").write_text(report, encoding="utf-8")


def main() -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    exp1_micro = pd.read_csv(RESULTS_DIR / "exp1_distributed_skew.csv")
    exp2_micro = pd.read_csv(RESULTS_DIR / "exp2_variation_detection.csv")
    exp2_abl = pd.read_csv(RESULTS_DIR / "exp2_ablation.csv")
    exp3_micro = pd.read_csv(RESULTS_DIR / "exp3_slice_reuse_and_kll.csv")
    exp3_sk = pd.read_csv(RESULTS_DIR / "exp3_sketch_accuracy.csv")

    exp1_cls = pd.read_csv(RESULTS_DIR / "exp1_cluster_alignment.csv") if (RESULTS_DIR / "exp1_cluster_alignment.csv").exists() else pd.DataFrame()
    exp2_cls = pd.read_csv(RESULTS_DIR / "exp2_cluster_variation.csv") if (RESULTS_DIR / "exp2_cluster_variation.csv").exists() else pd.DataFrame()
    exp3_cls = pd.read_csv(RESULTS_DIR / "exp3_cluster_slice_reuse.csv") if (RESULTS_DIR / "exp3_cluster_slice_reuse.csv").exists() else pd.DataFrame()
    exp4_cls = pd.read_csv(RESULTS_DIR / "exp4_e2e_modes.csv")

    docker_status_path = RESULTS_DIR / "exp5_docker_status.json"
    docker_status = (
        json.loads(docker_status_path.read_text(encoding="utf-8"))
        if docker_status_path.exists()
        else {"status": "NOT_RUN", "reason": "exp5_docker_status.json missing"}
    )

    plot_exp1(exp1_micro, exp1_cls)
    plot_exp2_methods(exp2_micro, exp2_cls)
    plot_exp2_ablation(exp2_abl)
    plot_exp3(exp3_micro, exp3_sk, exp3_cls)
    plot_exp4(exp4_cls)
    write_report(exp1_cls, exp2_cls, exp3_cls, exp4_cls, exp1_micro, exp2_micro, exp2_abl, exp3_micro, exp3_sk, docker_status)
    print(f"Generated figures in {FIGURES_DIR} and report at {BENCH_ROOT / 'RESULTS_REPORT.md'}")


if __name__ == "__main__":
    main()
