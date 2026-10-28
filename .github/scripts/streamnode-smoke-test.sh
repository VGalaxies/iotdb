#!/bin/bash
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

# Real start/stop smoke test for the StreamNode distribution scripts.
# Runs on Linux: builds the distribution, brings up an all-in-one
# (standalone) ConfigNode+DataNode as the seed ConfigNode, then really
# starts/stops a StreamNode and checks the process + listening port.
#
# Usage:
#   SKIP_BUILD=1    # ignore build
#   bash .github/scripts/streamnode-smoke-test.sh                # build + smoke
#
# Cases covered (spec SN-01/02/06/10/12/13/18):
#   SN-02  conf delivery: streamnode-env.sh / logback / iotdb-system.properties
#   SN-01  start-streamnode.sh -> one StreamNode JVM appears
#   SN-06  internal port 10820 + metric port 9093 listening
#   SN-18  remote JMX port 32001 listening
#   SN-10  stop-streamnode.sh -> the StreamNode stops
#   SN-12  start-standalone.sh (no args) -> 1C1D, CN:DN memory ratio 0.3:0.5
#   SN-13  --with_streamnode -> 1C1D1S (not implemented)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DIST_TARGET="$REPO_ROOT/distribution/target"

PASS=0
FAIL=0
NOT_IMPL=0

pass() { echo "[PASS] $1"; PASS=$((PASS + 1)); }
fail() { echo "[FAIL] $1"; FAIL=$((FAIL + 1)); }
not_impl() { echo "[NOT-IMPLEMENTED] $1"; NOT_IMPL=$((NOT_IMPL + 1)); }

# Locate a distribution home (the base-directory layer that contains sbin/),
# matched by its assembly id: streamnode-bin or all-bin. The assembly nests the
# base directory under a same-named dir, so find both layers and keep the one
# that actually has sbin/.
distribution_home() {
  local id="$1" d
  while IFS= read -r d; do
    [ -d "$d/sbin" ] && { echo "$d"; return 0; }
  done < <(find "$DIST_TARGET" -type d -name "apache-iotdb-*-${id}" 2>/dev/null | sort)
  return 1
}

# Poll until a java process whose command line matches $1 appears.
wait_for_process() {
  local pat="$1" t="${2:-120}" i
  for ((i = 0; i < t; i++)); do
    ps -ef | grep -v grep | grep -q -- "$pat" && return 0
    sleep 1
  done
  return 1
}

# Poll until no java process matches $1 anymore.
wait_for_gone() {
  local pat="$1" t="${2:-60}" i
  for ((i = 0; i < t; i++)); do
    ps -ef | grep -v grep | grep -q -- "$pat" || return 0
    sleep 1
  done
  return 1
}

# True if the given TCP port is LISTENing.
port_open() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -i:"$port" -sTCP:LISTEN >/dev/null 2>&1
  elif command -v ss >/dev/null 2>&1; then
    ss -ltn 2>/dev/null | grep -q ":$port "
  else
    (echo >/dev/tcp/127.0.0.1/"$port") 2>/dev/null
  fi
}

wait_for_port() {
  local port="$1" t="${2:-120}" i
  for ((i = 0; i < t; i++)); do
    port_open "$port" && return 0
    sleep 1
  done
  return 1
}

# System memory in MB, mirroring iotdb-common.sh calculate_memory_sizes:
# free -m, then clamped by the cgroup limit if present.
system_memory_mb() {
  local mem cg cgmb
  mem=$(free -m | sed -n '2p' | awk '{print $2}')
  if [ -f /sys/fs/cgroup/memory.max ]; then
    cg=$(cat /sys/fs/cgroup/memory.max 2>/dev/null)
    if [ -n "$cg" ] && [ "$cg" != "max" ]; then
      cgmb=$((cg / 1024 / 1024))
      [ "$cgmb" -lt "$mem" ] && mem=$cgmb
    fi
  elif [ -f /sys/fs/cgroup/memory/memory.limit_in_bytes ]; then
    cg=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null)
    cgmb=$((cg / 1024 / 1024))
    [ "$cgmb" -lt "$mem" ] && mem=$cgmb
  fi
  echo "$mem"
}

# on-heap (-Xmx) + off-heap (-XX:MaxDirectMemorySize) total MB of a java process.
# on+off equals the configured memory_size_in_mb, independent of the heap ratio.
node_total_mb() {
  local pat="$1" cmd xmx maxd
  cmd=$(ps -ef | grep -v grep | grep -- "$pat" | head -1)
  [ -z "$cmd" ] && { echo 0; return 1; }
  xmx=$(printf '%s' "$cmd" | grep -oE -e '-Xmx[0-9]+M' | head -1 | grep -oE '[0-9]+')
  maxd=$(printf '%s' "$cmd" | grep -oE -e '-XX:MaxDirectMemorySize=[0-9]+M' | head -1 | grep -oE '[0-9]+')
  echo $(( ${xmx:-0} + ${maxd:-0} ))
}

echo "==== StreamNode smoke test ===="

# --- 0. build (SKIP_BUILD=1 to reuse an existing build) ---
if [ "${SKIP_BUILD:-0}" != "1" ]; then
  echo "[smoke] building distribution (this is slow)..."
  # streamnode.xml is only registered under the with-streamnode profile, so
  # without -P with-streamnode the streamnode-bin assembly is not produced.
  (cd "$REPO_ROOT" && mvn -pl distribution -am -DskipTests clean package -P with-streamnode) || {
    fail "build distribution"
    exit 1
  }
fi

SN_HOME=$(distribution_home streamnode-bin) || { fail "locate streamnode-bin"; exit 1; }
ALL_HOME=$(distribution_home all-bin) || { fail "locate all-bin"; exit 1; }
echo "[smoke] streamnode home = $SN_HOME"
echo "[smoke] standalone home = $ALL_HOME"

# --- SN-02: delivered config files + overridable knobs ---
echo "== SN-02 =="
sn02_ok=1
for f in streamnode-env.sh logback-streamnode.xml iotdb-system.properties; do
  if [ ! -f "$SN_HOME/conf/$f" ]; then
    echo "[smoke]   missing conf: $f"
    sn02_ok=0
  fi
done
# env (streamnode-env.sh) covers memory / JVM / JMX
grep -q 'MEMORY_SIZE' "$SN_HOME/conf/streamnode-env.sh" 2>/dev/null || sn02_ok=0
grep -q 'STREAMNODE_JMX_OPTS' "$SN_HOME/conf/streamnode-env.sh" 2>/dev/null || sn02_ok=0
grep -q 'JMX_PORT' "$SN_HOME/conf/streamnode-env.sh" 2>/dev/null || sn02_ok=0
# system.properties overridable keys: RPC / consensus / metric / parallelism
for key in sn_internal_port sn_seed_config_node sn_metric_prometheus_reporter_port; do
  grep -qh "^${key}=" "$SN_HOME/conf/iotdb-system.properties" "$SN_HOME/conf/iotdb-system.properties.template" 2>/dev/null || sn02_ok=0
done
if [ "$sn02_ok" -eq 1 ]; then
  pass "SN-02 conf delivery + overridable knobs"
else
  fail "SN-02 conf delivery + overridable knobs"
fi

# --- SN-12: start standalone (no args) -> 1C1D, also seeds the ConfigNode ---
echo "== SN-12 =="
"$ALL_HOME/sbin/start-standalone.sh"
sleep 3
if wait_for_process "org.apache.iotdb.confignode.service.ConfigNode" 180 \
  && wait_for_process "org.apache.iotdb.db.service.DataNode" 180; then
  pass "SN-12 standalone starts only 1C1D"
else
  fail "SN-12 standalone 1C1D"
fi
# SN-12 memory ratio: CN = 30% (3/10, cap 8192MB), DN = 50% (1/2).
sys_mb=$(system_memory_mb)
cn_expect=$((sys_mb / 10 * 3))
[ "$cn_expect" -gt 8192 ] && cn_expect=8192
dn_expect=$((sys_mb / 2))
cn_actual=$(node_total_mb "org.apache.iotdb.confignode.service.ConfigNode")
dn_actual=$(node_total_mb "org.apache.iotdb.db.service.DataNode")
if [ "$cn_actual" -eq "$cn_expect" ] && [ "$dn_actual" -eq "$dn_expect" ]; then
  pass "SN-12 memory ratio 0.3/0.5 (CN=${cn_actual}MB DN=${dn_actual}MB)"
else
  fail "SN-12 memory ratio 0.3/0.5 (expect CN=${cn_expect}MB DN=${dn_expect}MB, got CN=${cn_actual}MB DN=${dn_actual}MB)"
fi

# --- wait for the ConfigNode internal port so the StreamNode can register ---
if wait_for_port 10710 120; then
  echo "[smoke] ConfigNode internal port 10710 ready"
else
  fail "ConfigNode internal port 10710"
fi

# --- SN-01: start the StreamNode ---
echo "== SN-01 =="
# SN-06 needs the Prometheus reporter for 9093; SN-18 needs remote JMX for 32001.
# The shipped conf may omit sn_metric_reporter_list, and JMX_LOCAL defaults to
# "true" (which never binds 32001). The streamnode assembly also ships no
# jmx.password/jmx.access, so copy the same credentials confignode/datanode use.
grep -q '^sn_metric_reporter_list=' "$SN_HOME/conf/iotdb-system.properties" 2>/dev/null \
  || echo "sn_metric_reporter_list=PROMETHEUS" >> "$SN_HOME/conf/iotdb-system.properties"
cp "$REPO_ROOT/iotdb-core/confignode/src/assembly/resources/conf/jmx.password" "$SN_HOME/conf/jmx.password"
cp "$REPO_ROOT/iotdb-core/confignode/src/assembly/resources/conf/jmx.access" "$SN_HOME/conf/jmx.access"
sed -i 's/^JMX_LOCAL="true"/JMX_LOCAL="false"/' "$SN_HOME/conf/streamnode-env.sh"
"$SN_HOME/sbin/start-streamnode.sh" -d
if wait_for_process "org.apache.iotdb.streamnode.StreamNode" 120; then
  pass "SN-01 StreamNode JVM appears"
else
  fail "SN-01 StreamNode JVM appears"
fi

# --- SN-06: check internal port 10820 and metric port 9093 ---
echo "== SN-06 =="
if wait_for_port 10820 60; then
  pass "SN-06 internal port 10820 listening"
else
  fail "SN-06 internal port 10820 listening"
fi
if wait_for_port 9093 60; then
  pass "SN-06 metric port 9093 listening"
else
  fail "SN-06 metric port 9093 listening"
fi

# --- SN-18: check JMX port 32001 ---
echo "== SN-18 =="
if wait_for_port 32001 60; then
  pass "SN-18 JMX port 32001 listening"
else
  fail "SN-18 JMX port 32001 listening"
fi

# --- SN-10: stop the StreamNode ---
echo "== SN-10 =="
"$SN_HOME/sbin/stop-streamnode.sh"
if wait_for_gone "org.apache.iotdb.streamnode.StreamNode" 60; then
  pass "SN-10 StreamNode stopped"
else
  fail "SN-10 StreamNode stopped"
fi

# --- SN-12 teardown: stop the standalone ---
"$ALL_HOME/sbin/stop-standalone.sh"
if wait_for_gone "org.apache.iotdb.confignode.service.ConfigNode" 60 \
  && wait_for_gone "org.apache.iotdb.db.service.DataNode" 60; then
  pass "SN-12 standalone stopped"
else
  fail "SN-12 standalone stopped"
fi

echo "==== summary: PASS=$PASS FAIL=$FAIL NOT-IMPLEMENTED=$NOT_IMPL ===="
[ "$FAIL" -eq 0 ]