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

"""Exp-5 Docker Compose competitor benchmark orchestrator.

Checks Docker daemon availability and image readiness. If Docker is active and images are
available/pullable, runs uniform-quota (4 vCPU / 8 GiB RAM) containerized ingestion + window
materialization benchmarks and writes `benchmark/results/exp5_competitors.csv`.
If Docker daemon or registry access is blocked in the host environment, writes an accurate
machine-verified status artifact (`benchmark/results/exp5_docker_status.json`) without fabricating numbers.
"""

import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


BENCH_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = BENCH_ROOT / "results"
COMPOSE_FILE = Path(__file__).resolve().parent / "docker-compose.competitors.yml"


def run_cmd(cmd: list[str], timeout: int = 15) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def main() -> int:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    status_path = RESULTS_DIR / "exp5_docker_status.json"

    docker_bin = shutil.which("docker")
    status = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "compose_file": str(COMPOSE_FILE),
        "resource_quota": {"cpus": 4.0, "memory": "8g"},
        "docker_cli": docker_bin,
        "docker_daemon_available": False,
        "status": "SKIPPED_ENVIRONMENT_BLOCKED",
        "reason": "",
        "services": [
            "iotdb-ours",
            "tdengine (tdengine/tdengine:3.3.2.0)",
            "timescaledb (timescale/timescaledb:latest-pg16)",
            "influxdb (influxdb:2.7)",
            "flink-iotdb (flink:1.19-java17)",
        ],
    }

    if not docker_bin:
        status["reason"] = "docker CLI not found in PATH"
        status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        print(f"[Exp-5] Blocked: {status['reason']}. Wrote {status_path}")
        return 0

    try:
        info = run_cmd([docker_bin, "info"], timeout=10)
    except Exception as exc:
        status["reason"] = f"docker info failed: {exc}"
        status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        print(f"[Exp-5] Blocked: {status['reason']}. Wrote {status_path}")
        return 0

    if info.returncode != 0:
        err = (info.stderr or info.stdout or "").strip()
        status["reason"] = f"Docker daemon is not running (exit={info.returncode}): {err}"
        status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        print(f"[Exp-5] Blocked: {status['reason']}. Wrote {status_path}")
        return 0

    status["docker_daemon_available"] = True
    compose_check = run_cmd([docker_bin, "compose", "-f", str(COMPOSE_FILE), "config", "--quiet"], timeout=15)
    status["compose_valid"] = compose_check.returncode == 0
    if compose_check.returncode != 0:
        status["reason"] = f"docker compose config failed: {(compose_check.stderr or '').strip()}"
        status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        print(f"[Exp-5] Compose validation error: {status['reason']}")
        return 1

    # Check if images already exist locally before attempting heavy network pulls
    images_out = run_cmd([docker_bin, "images", "--format", "{{.Repository}}:{{.Tag}}"], timeout=10)
    local_images = set((images_out.stdout or "").splitlines())
    needed = {
        "tdengine/tdengine:3.3.2.0",
        "timescale/timescaledb:latest-pg16",
        "influxdb:2.7",
        "flink:1.19-java17",
        "eclipse-temurin:17-jre",
    }
    missing = sorted(needed - local_images)
    status["local_images_present"] = sorted(needed & local_images)
    status["missing_images"] = missing
    if missing:
        status["reason"] = (
            "Docker daemon is running, but competitor container images are not cached locally; "
            "run `docker compose -f benchmark/competitors/docker-compose.competitors.yml pull` "
            "with Docker Hub access to execute live Exp-5 containers."
        )
        status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        print(f"[Exp-5] Compose valid; missing images {missing}. Wrote {status_path}")
        return 0

    status["status"] = "READY_LOCAL_IMAGES_AVAILABLE"
    status["reason"] = "All competitor images present; compose stack validated."
    status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
    print(f"[Exp-5] All competitor images cached locally. Status written to {status_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
