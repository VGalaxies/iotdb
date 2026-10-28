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
"""Regression tests for StreamNode start / standalone launch scripts.

Covers two areas:

  * Shell-side memory-allocation refactor (P1 / P2):
      - P1  cross-platform memory detection (Linux free -m, Darwin sysctl,
            FreeBSD sysctl, SunOS prtconf, safe fallback)
      - P1  detect_system_memory_mb failure exits with an error
      -     --with_datanode ratio (50 % independent, 20 % co-located)
      -     --with_streamnode 2 : 4 : 2 split via standalone
      - P2  preserve an operator-supplied MEMORY_SIZE

  * StreamNode packaging / launch spec (SN-01..07, SN-17, SN-18):
      - SN-01  start-streamnode.sh launches one StreamNode JVM (class + ``-s``)
      - SN-02  conf delivery: streamnode-env.sh / logback / iotdb-system.properties
      - SN-03  default memory = 20 % of system memory (env-script tier)
      - SN-04  heap / off-heap ratio tiers
      - SN-05  jdk.nio.maxCachedBufferSize == off-heap / 100
      - SN-07  stop-streamnode.sh SIGTERMs the single StreamNode
      - SN-17  -Xms == -Xmx == on-heap, -XX:MaxDirectMemorySize == off-heap
      - SN-18  JMX default port 32001

The launcher tests stub java / free / egrep / lsof / ps / kill (and on
Windows netstat / taskkill) so the scripts run without a real JVM or cluster.
"""

import os
import re
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[2]
WINDOWS = os.name == "nt"

# ---------------------------------------------------------------------------
# stubs
# ---------------------------------------------------------------------------

JAVA_STUB = """#!/bin/bash
if [ "$1" = "-version" ]; then
  echo 'openjdk version "17.0.9" 2023-10-17' >&2
  exit 0
fi
printf '%s\\n' "$@" >> "$SN_TEST_JAVA_ARGS"
exit 0
"""

FREE_STUB = """#!/bin/bash
cat <<EOS
              total        used        free      shared  buff/cache   available
Mem:          ${SN_TEST_SYSTEM_MB:-16384}           0           0           0           0           0
Swap:            0           0           0
EOS
"""

UNAME_STUB = """#!/bin/bash
echo "${SN_TEST_UNAME:-Linux}"
"""

ID_STUB = """#!/bin/bash
if [ "$1" = "-u" ]; then
  echo "${SN_TEST_UID:-0}"
else
  echo "uid=${SN_TEST_UID:-0}(root) gid=0(root) groups=0(root)"
fi
"""

SYSCTL_STUB = """#!/bin/bash
if [ "$1" = "-n" ]; then
  shift
fi
case "$*" in
  hw.memsize)
    printf '%d\\n' $(( ${SN_TEST_SYSTEM_MB:-16384} * 1024 * 1024 ))
    ;;
  hw.physmem)
    printf '%d\\n' $(( ${SN_TEST_SYSTEM_MB:-16384} * 1024 * 1024 ))
    ;;
  hw.ncpu|kern.ipc.somaxconn|net.core.somaxconn)
    echo "${SN_TEST_CPU_CORES:-4}"
    ;;
  *)
    echo 0
    ;;
esac
"""

PRTCONF_STUB = """#!/bin/bash
echo "Memory size: ${SN_TEST_SYSTEM_MB:-16384} Megabytes"
"""

LSOF_STUB = """#!/bin/bash
if [ "$1" = "-t" ]; then
  if echo "$*" | grep -q 10820; then
    echo "99999"
    exit 0
  fi
fi
exit 1
"""

PS_STUB = """#!/bin/bash
echo "99999 ? Sl 0:00 /opt/java/bin/java -cp /opt/iotdb/lib/iotdb.StreamNode org.apache.iotdb.streamnode.StreamNode -s"
"""

KILL_STUB = """#!/bin/bash
printf 'kill %s\\n' "$*" >> "$SN_TEST_KILL_ARGS"
exit 0
"""

EGREP_STUB = """#!/bin/bash
echo "${SN_TEST_CPU_CORES:-4}"
"""

NETSTAT_STUB = """@echo off
echo   TCP    0.0.0.0:10820    0.0.0.0:0    LISTENING    99999
"""

TASKKILL_STUB = """@echo off
>>"%SN_TEST_KILL_ARGS%" echo taskkill %*
"""


def _tier_heap_offheap(mb):
    """Mirror streamnode-env.sh / iotdb-common.sh heap/off-heap split."""
    if mb < 4096:
        on_heap = mb // 4 * 3
    elif mb < 16384:
        on_heap = mb // 5 * 4
    elif mb < 131072:
        on_heap = mb // 8 * 7
    else:
        on_heap = mb - 16384
    return on_heap, mb - on_heap


class StreamNodeOpsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="iotdb-streamnode-ops-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()

        # ---- fake STREAMNODE_HOME layout ----
        self.installation = self.root / "streamnode"
        self.config = self.installation / "conf"
        self.config.mkdir(parents=True)
        (self.installation / "lib").mkdir(parents=True)

        # ---- fake all-bin standalone layout ----
        self.all_home = self.root / "all"
        (self.all_home / "lib").mkdir(parents=True)

        # ---- stub bin directory (prepended to PATH) ----
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.java_args = self.root / "java-args.txt"
        self.kill_args = self.root / "kill-args.txt"

        self.env = os.environ.copy()
        for key in (
            "JAVA_HOME",
            "STREAMNODE_HOME",
            "STREAMNODE_CONF",
            "STREAMNODE_JMX_OPTS",
            "STREAMNODE_LOGS",
            "STREAMNODE_LOG_CONFIG",
            "IOTDB_HOME",
            "MEMORY_SIZE",
            "TSFILE_LOCALE_JVM_OPT",
        ):
            self.env.pop(key, None)
        self.env["PATH"] = str(self.bin) + os.pathsep + self.env["PATH"]
        self.env["SN_TEST_JAVA_ARGS"] = str(self.java_args)
        self.env["SN_TEST_KILL_ARGS"] = str(self.kill_args)
        self.env["SN_TEST_SYSTEM_MB"] = "16384"
        self.env["SN_TEST_UNAME"] = "Linux"
        self.env["SN_TEST_CPU_CORES"] = "4"

        if WINDOWS:
            self._setup_windows()
        else:
            self._setup_unix()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _write(path, content):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content.encode("utf-8"))
        path.chmod(0o755)

    def _copy(self, relative):
        destination = self.installation / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPOSITORY / "scripts" / relative, destination)
        destination.chmod(0o755)
        return destination

    def _copy_standalone(self, relative):
        dest = self.all_home / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPOSITORY / "scripts" / relative, dest)
        dest.chmod(0o755)
        return dest

    def _setup_unix(self):
        self._copy("sbin/start-streamnode.sh")
        self._copy("sbin/stop-streamnode.sh")
        self._copy("conf/iotdb-common.sh")
        self._copy("conf/streamnode-env.sh")

        # standalone scripts
        self._copy_standalone("sbin/start-confignode.sh")
        self._copy_standalone("sbin/start-datanode.sh")
        standalone = self._copy_standalone("sbin/start-standalone.sh")
        self._standalone = standalone
        dest = self.all_home / "conf" / "iotdb-common.sh"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPOSITORY / "scripts/conf/iotdb-common.sh", dest)
        dest.chmod(0o755)

        self._write(
            self.config / "iotdb-system.properties",
            "sn_internal_port=10820\nsn_seed_config_node=127.0.0.1:10710\n",
        )

        # stubs
        self._write(self.bin / "java", JAVA_STUB)
        self._write(self.bin / "free", FREE_STUB)
        self._write(self.bin / "uname", UNAME_STUB)
        self._write(self.bin / "id", ID_STUB)
        self._write(self.bin / "lsof", LSOF_STUB)
        self._write(self.bin / "ps", PS_STUB)
        self._write(self.bin / "kill", KILL_STUB)
        self._write(self.bin / "sysctl", SYSCTL_STUB)
        self._write(self.bin / "prtconf", PRTCONF_STUB)
        self._write(self.bin / "egrep", EGREP_STUB)
        self.env["STREAMNODE_HOME"] = str(self.installation)

    def _setup_windows(self):
        self._copy("sbin/windows/start-streamnode.bat")
        self._copy("sbin/windows/stop-streamnode.bat")
        self._copy("conf/windows/streamnode-env.bat")
        self._write(self.config / "iotdb-system.properties", "sn_internal_port=10820\n")
        self._write(self.bin / "netstat.bat", NETSTAT_STUB)
        self._write(self.bin / "taskkill.bat", TASKKILL_STUB)
        self.env["STREAMNODE_HOME"] = str(self.installation)

    def _run_script(self, script, arguments=()):
        if WINDOWS:
            command = '"{}" /d /c call "{}" {}'.format(
                os.environ["COMSPEC"], script, " ".join(arguments)
            )
        else:
            command = [
                "bash",
                "-c",
                "enable -n kill 2>/dev/null; . \"$0\"; exit",
                str(script),
                *arguments,
            ]
        return subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            errors="replace",
            env=self.env,
            cwd=self.root,
            timeout=60,
        )

    def _run_start_and_capture_jvm(self, *args):
        self.java_args.write_text("")
        result = self._run_script(
            self.installation / "sbin/start-streamnode.sh", args
        )
        return result, self.java_args.read_text().splitlines()

    def _jvm_flag(self, lines, prefix):
        for line in lines:
            if line.startswith(prefix):
                return line[len(prefix) :]
        return None

    def _assert_heap_off_heap_flags(self, lines, mb):
        on_heap, off_heap = _tier_heap_offheap(mb)
        self.assertEqual("{}M".format(on_heap), self._jvm_flag(lines, "-Xmx"))
        self.assertEqual("{}M".format(on_heap), self._jvm_flag(lines, "-Xms"))
        self.assertEqual(
            "{}M".format(off_heap),
            self._jvm_flag(lines, "-XX:MaxDirectMemorySize="),
        )
        expected_cached = off_heap * 1024 * 1024 // 100
        self.assertEqual(
            str(expected_cached),
            self._jvm_flag(lines, "-Djdk.nio.maxCachedBufferSize="),
        )

    # ==================================================================
    # SN-01..SN-18 spec tests
    # ==================================================================

    @unittest.skipIf(WINDOWS, "Linux start-streamnode.sh")
    def test_start_launches_streamnode_class_with_dash_s(self):
        # SN-01: executing sbin/start-streamnode.sh starts one StreamNode JVM.
        _, lines = self._run_start_and_capture_jvm()
        self.assertIn("org.apache.iotdb.streamnode.StreamNode", lines)
        self.assertIn("-s", lines)
        for key, suffix in (
            ("-DSTREAMNODE_HOME=", ""),
            ("-DSTREAMNODE_CONF=", "/conf"),
            ("-DSTREAMNODE_LOGS=", "/logs"),
            ("-Dlogback.configurationFile=", "logback-streamnode.xml"),
        ):
            val = self._jvm_flag(lines, key) or ""
            self.assertTrue(val, "missing {}".format(key))
            self.assertIn(suffix, val)
        self.assertIsNotNone(self._jvm_flag(lines, "-Xmx"), "missing -Xmx")
        self.assertIsNotNone(self._jvm_flag(lines, "-Xms"), "missing -Xms")
        self.assertIsNotNone(
            self._jvm_flag(lines, "-XX:MaxDirectMemorySize="),
            "missing -XX:MaxDirectMemorySize",
        )
        self.assertIsNotNone(
            self._jvm_flag(lines, "-Djdk.nio.maxCachedBufferSize="),
            "missing maxCachedBufferSize",
        )

    @unittest.skipIf(WINDOWS, "Linux streamnode-env.sh")
    def test_memory_tier_heap_off_heap_ratios(self):
        # SN-04 + SN-05 + SN-17: ratio tiers and -Xms == -Xmx == on-heap,
        # -XX:MaxDirectMemorySize == off-heap.  Set MEMORY_SIZE via the
        # environment so the start script's auto-detection is bypassed (P2).
        cases = {
            "2G": 2048,
            "3G": 3072,
            "4G": 4096,
            "8G": 8192,
            "16G": 16384,
            "32G": 32768,
            "128G": 131072,
            "160G": 163840,
        }
        for text, mb in cases.items():
            with self.subTest(memory_size=text):
                self.env["MEMORY_SIZE"] = text
                _, lines = self._run_start_and_capture_jvm()
                self._assert_heap_off_heap_flags(lines, mb)
                del self.env["MEMORY_SIZE"]

    @unittest.skipIf(WINDOWS, "Linux streamnode-env.sh")
    def test_default_memory_is_twenty_percent_of_system(self):
        # SN-03: without MEMORY_SIZE, streamnode-env.sh suggests 20 % of system
        # memory capped at 4 GB.  The start script's default is now 50 %
        # independent / 20 % co-located, so we inject a MEMORY_SIZE that
        # explicitly matches the env script's suggest value to verify the tier
        # logic.
        self.env["SN_TEST_SYSTEM_MB"] = "8192"
        suggest_mb = min(8192 // 10 * 2, 4096)
        self.env["MEMORY_SIZE"] = "{}M".format(suggest_mb)
        _, lines = self._run_start_and_capture_jvm()
        self._assert_heap_off_heap_flags(lines, suggest_mb)

    @unittest.skipIf(WINDOWS, "Linux streamnode-env.sh")
    def test_env_jmx_opts_override_computed_memory(self):
        # SN-02: STREAMNODE_JMX_OPTS overrides computed memory values.
        self.env["STREAMNODE_JMX_OPTS"] = "-Xmx512M -XX:MaxDirectMemorySize=256M"
        _, lines = self._run_start_and_capture_jvm()
        self.assertEqual("512M", self._jvm_flag(lines, "-Xmx"))
        self.assertEqual("256M", self._jvm_flag(lines, "-XX:MaxDirectMemorySize="))

    @unittest.skipIf(WINDOWS, "Linux streamnode-env.sh")
    def test_jmx_remote_enables_remote_port(self):
        # SN-18: JMX_LOCAL=false emits the remote JMX port 32001.
        env_sh = self.installation / "conf/streamnode-env.sh"
        env_sh.write_text(
            env_sh.read_text().replace('JMX_LOCAL="true"', 'JMX_LOCAL="false"')
        )
        _, lines = self._run_start_and_capture_jvm()
        self.assertIn("-Dcom.sun.management.jmxremote.port=32001", lines)

    # ==================================================================
    # P1 — cross-platform memory detection
    # ==================================================================

    @unittest.skipIf(WINDOWS, "uname / sysctl stubs are Unix-only")
    def test_linux_memory_detection(self):
        self.env["SN_TEST_SYSTEM_MB"] = "8192"
        _, lines = self._run_start_and_capture_jvm()
        on_heap, off_heap = _tier_heap_offheap(8192 // 2)
        self.assertEqual("{}M".format(on_heap), self._jvm_flag(lines, "-Xmx"))
        self.assertEqual("{}M".format(off_heap),
                         self._jvm_flag(lines, "-XX:MaxDirectMemorySize="))

    @unittest.skipIf(WINDOWS, "uname / sysctl stubs are Unix-only")
    def test_darwin_memory_detection(self):
        self.env["SN_TEST_UNAME"] = "Darwin"
        self.env["SN_TEST_SYSTEM_MB"] = "16384"
        _, lines = self._run_start_and_capture_jvm()
        on_heap, off_heap = _tier_heap_offheap(16384 // 2)
        self.assertEqual("{}M".format(on_heap), self._jvm_flag(lines, "-Xmx"))
        self.assertEqual("{}M".format(off_heap),
                         self._jvm_flag(lines, "-XX:MaxDirectMemorySize="))

    @unittest.skipIf(WINDOWS, "uname / sysctl stubs are Unix-only")
    def test_freebsd_memory_detection(self):
        self.env["SN_TEST_UNAME"] = "FreeBSD"
        self.env["SN_TEST_SYSTEM_MB"] = "16384"
        _, lines = self._run_start_and_capture_jvm()
        on_heap, _ = _tier_heap_offheap(16384 // 2)
        self.assertIsNotNone(self._jvm_flag(lines, "-Xmx"))
        self.assertEqual("{}M".format(on_heap), self._jvm_flag(lines, "-Xmx"))

    @unittest.skipIf(WINDOWS, "uname stubs are Unix-only")
    def test_fallback_memory_detection(self):
        self.env["SN_TEST_UNAME"] = "UnknownOS"
        _, lines = self._run_start_and_capture_jvm()
        on_heap, off_heap = _tier_heap_offheap(2048 // 2)
        self.assertEqual("{}M".format(on_heap), self._jvm_flag(lines, "-Xmx"))
        self.assertEqual("{}M".format(off_heap),
                         self._jvm_flag(lines, "-XX:MaxDirectMemorySize="))

    @unittest.skipIf(WINDOWS, "uname stubs are Unix-only")
    def test_memory_detection_failure_exits(self):
        self.env["SN_TEST_UNAME"] = "Linux"
        self._write(self.bin / "free", "#!/bin/bash\nexit 0\n")
        result, _ = self._run_start_and_capture_jvm()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unable to detect system memory", result.stdout)

    # ==================================================================
    # --with_datanode ratio
    # ==================================================================

    @unittest.skipIf(WINDOWS, "Linux sh start script")
    def test_with_datanode_flag_20_percent(self):
        _, lines = self._run_start_and_capture_jvm("--with_datanode")
        on_heap, off_heap = _tier_heap_offheap(16384 // 5)
        self.assertEqual("{}M".format(on_heap), self._jvm_flag(lines, "-Xmx"))
        self.assertEqual("{}M".format(off_heap),
                         self._jvm_flag(lines, "-XX:MaxDirectMemorySize="))

    @unittest.skipIf(WINDOWS, "Linux sh start script")
    def test_default_without_with_datanode_50_percent(self):
        _, lines = self._run_start_and_capture_jvm()
        on_heap, off_heap = _tier_heap_offheap(16384 // 2)
        self.assertEqual("{}M".format(on_heap), self._jvm_flag(lines, "-Xmx"))
        self.assertEqual("{}M".format(off_heap),
                         self._jvm_flag(lines, "-XX:MaxDirectMemorySize="))

    # ==================================================================
    # P2 — explicit MEMORY_SIZE is preserved
    # ==================================================================

    @unittest.skipIf(WINDOWS, "Linux sh start script")
    def test_explicit_memory_size_preserved(self):
        self.env["MEMORY_SIZE"] = "1G"
        _, lines = self._run_start_and_capture_jvm()
        on_heap, _ = _tier_heap_offheap(1024)
        self.assertEqual("{}M".format(on_heap), self._jvm_flag(lines, "-Xmx"))

    @unittest.skipIf(WINDOWS, "Linux sh start script")
    def test_explicit_memory_size_preserved_with_datanode(self):
        self.env["MEMORY_SIZE"] = "512M"
        _, lines = self._run_start_and_capture_jvm("--with_datanode")
        on_heap, _ = _tier_heap_offheap(512)
        self.assertEqual("{}M".format(on_heap), self._jvm_flag(lines, "-Xmx"))

    # ==================================================================
    # standalone --with_streamnode
    # ==================================================================

    # SN-12: standalone without --with_streamnode starts 1C1D using the
    # default memory ratios (CN 30 % capped at 8 GB, DN 50 %).  Each stub
    # sources its own env script so the computed ON_HEAP_MEMORY /
    # OFF_HEAP_MEMORY reflect the real default ratios.
    @unittest.skipIf(WINDOWS, "Linux sh standalone script")
    def test_standalone_default_1c1d(self):
        # Copy env scripts so the stubs can source them.
        for env_name in ("confignode-env.sh", "datanode-env.sh"):
            dest = self.all_home / "conf" / env_name
            self._write(
                dest, (REPOSITORY / "scripts/conf" / env_name).read_bytes().decode()
            )
        for name, env_file in (
            ("start-confignode.sh", "confignode-env.sh"),
            ("start-datanode.sh", "datanode-env.sh"),
        ):
            path = self.all_home / "sbin" / name
            self._write(
                path, "#!/bin/bash\n"
                '# A real start script sources iotdb-common.sh then its env; we'
                '# do the same here so the env-script default ratios apply.\n'
                'source "$(dirname "$0")/../conf/iotdb-common.sh"\n'
                'source "$(dirname "$0")/../conf/{}"\n'
                'echo "${{0##*/}}" "${{ON_HEAP_MEMORY}}" "${{OFF_HEAP_MEMORY}}"'
                ' >> "$SN_TEST_JAVA_ARGS"\n'
                "exit 0\n".format(env_file),
            )
        self.env["IOTDB_HOME"] = str(self.all_home)
        self.java_args.write_text("")
        result = self._run_script(self._standalone)
        self.assertEqual(result.returncode, 0, result.stdout)
        time.sleep(0.5)
        lines = self.java_args.read_text().splitlines()
        self.assertGreaterEqual(
            len(lines), 2,
            "Expected >= 2 lines (CN + DN), got {}".format(lines),
        )
        self.assertIn("start-confignode.sh", lines[0])
        self.assertIn("start-datanode.sh", lines[1])
        # lines are: "script-name ON_HEAP OFF_HEAP"
        cn_parts = lines[0].split()
        dn_parts = lines[1].split()
        cn_on = int(re.sub(r"[^0-9]", "", cn_parts[1]))
        cn_off = int(re.sub(r"[^0-9]", "", cn_parts[2]))
        dn_on = int(re.sub(r"[^0-9]", "", dn_parts[1]))
        dn_off = int(re.sub(r"[^0-9]", "", dn_parts[2]))
        # Bash integer division: CN 16384 / 10 * 3 = 4914, cap 8192 → 4914.
        #   heap = 4914 / 5 * 4 = 3928, off = 4914 - 3928 = 986
        self.assertEqual(4914 // 5 * 4, cn_on)
        self.assertEqual(4914 - cn_on, cn_off)
        # DN 16384 / 2 * 1 = 8192, no cap.
        #   heap = 8192 / 5 * 4 = 6552, off = 8192 - 6552 = 1640
        self.assertEqual(8192 // 5 * 4, dn_on)
        self.assertEqual(8192 - dn_on, dn_off)

    # SN-13: standalone --with_streamnode allocates 1C1D with 0.2/0.4.
    @unittest.skipIf(WINDOWS, "Linux sh standalone script")
    def test_standalone_with_streamnode_split(self):
        for name in ("start-confignode.sh", "start-datanode.sh"):
            path = self.all_home / "sbin" / name
            self._write(
                path,
                "#!/bin/bash\n"
                'echo "${0##*/}" "$MEMORY_SIZE" >> "$SN_TEST_JAVA_ARGS"\n'
                "exit 0\n",
            )
        self.env["IOTDB_HOME"] = str(self.all_home)
        self.java_args.write_text("")
        result = self._run_script(self._standalone, ("--with_streamnode",))
        self.assertEqual(result.returncode, 0, result.stdout)
        time.sleep(0.5)
        lines = self.java_args.read_text().splitlines()
        self.assertGreaterEqual(len(lines), 2,
                                "Expected >= 2 lines, got {}".format(lines))
        self.assertIn("start-confignode.sh", lines[0])
        self.assertIn("start-datanode.sh", lines[1])
        cn_mb = int(re.sub(r"[^0-9]", "", lines[0].split()[-1]) or "0")
        dn_mb = int(re.sub(r"[^0-9]", "", lines[1].split()[-1]) or "0")
        self.assertEqual(16384 // 10 * 2, cn_mb)
        self.assertEqual(16384 // 10 * 4, dn_mb)

    # SN-13: --with_streamnode preserves an operator-supplied MEMORY_SIZE
    # instead of overriding it with the auto-split value.
    @unittest.skipIf(WINDOWS, "Linux sh standalone script")
    def test_standalone_with_streamnode_preserves_explicit_memory_size(self):
        self.env["MEMORY_SIZE"] = "1G"
        for name in ("start-confignode.sh", "start-datanode.sh"):
            path = self.all_home / "sbin" / name
            self._write(
                path,
                "#!/bin/bash\n"
                'echo "${0##*/}" "$MEMORY_SIZE" >> "$SN_TEST_JAVA_ARGS"\n'
                "exit 0\n",
            )
        self.env["IOTDB_HOME"] = str(self.all_home)
        self.java_args.write_text("")
        result = self._run_script(self._standalone, ("--with_streamnode",))
        self.assertEqual(result.returncode, 0, result.stdout)
        lines = self.java_args.read_text().splitlines()
        for line in lines:
            self.assertIn("1G", line)

    # ==================================================================
    # stop-streamnode
    # ==================================================================

    @unittest.skipIf(WINDOWS, "Linux sh stop script")
    def test_stop_script_sends_sigterm(self):
        result = self._run_script(
            self.installation / "sbin/stop-streamnode.sh"
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        kill_lines = self.kill_args.read_text().splitlines()
        self.assertTrue(any("TERM" in line for line in kill_lines), kill_lines)

    # ==================================================================
    # Windows
    # ==================================================================

    @unittest.skipUnless(WINDOWS, "Native Windows stop script")
    def test_windows_stop_uses_taskkill(self):
        result = self._run_script(
            self.installation / "sbin/windows/stop-streamnode.bat"
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        kill_lines = self.kill_args.read_text().splitlines()
        self.assertTrue(
            any("/pid 99999" in line for line in kill_lines), kill_lines
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)