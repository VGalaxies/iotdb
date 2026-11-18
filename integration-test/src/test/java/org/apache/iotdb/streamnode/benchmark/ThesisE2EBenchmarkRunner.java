/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.streamnode.benchmark;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.TableSessionBuilder;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Isolated 3-JVM small-spec cluster benchmark harness (no {@code @Test} methods; invoked directly).
 *
 * <p>Deploys three independent Java OS processes per test unit:
 *
 * <ul>
 *   <li>ConfigNode: {@code -Xms1536M -Xmx1536M -Xmn256M}
 *   <li>DataNode: {@code -Xms2304M -Xmx2304M -Xmn768M}
 *   <li>StreamNode: {@code -Xms3379M -Xmx3379M -Xmn820M}
 * </ul>
 *
 * <p>Before every test unit, shuts down all processes, wipes {@code data}, {@code logs}, and {@code
 * iotdb-subscription} across all three nodes + client scratch dir, waits for all cluster ports to
 * release, and executes alternating cyclic rounds serially on the idle machine.
 */
public final class ThesisE2EBenchmarkRunner {

  private static final int ROUNDS = 3;

  private static final int CN_INTERNAL_PORT = 10710;
  private static final int CN_CONSENSUS_PORT = 10720;
  private static final int DN_RPC_PORT = 6667;
  private static final int DN_INTERNAL_PORT = 10730;
  private static final int DN_MPP_PORT = 10740;
  private static final int DN_SCHEMA_PORT = 10750;
  private static final int DN_DATA_PORT = 10760;
  private static final int SN_INTERNAL_PORT = 10820;
  private static final int[] CLUSTER_PORTS = {
    CN_INTERNAL_PORT,
    CN_CONSENSUS_PORT,
    DN_RPC_PORT,
    DN_INTERNAL_PORT,
    DN_MPP_PORT,
    DN_SCHEMA_PORT,
    DN_DATA_PORT,
    SN_INTERNAL_PORT
  };

  private static final String[] ADD_OPENS_ARGS = {
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.nio.channels=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "-Dfile.encoding=UTF-8",
    "-Dsun.jnu.encoding=UTF-8",
    "-Duser.language=en",
    "-Duser.country=US",
    "-Duser.timezone=Asia/Shanghai"
  };

  private ThesisE2EBenchmarkRunner() {}

  private static final class IsolatedCluster implements AutoCloseable {
    final Path allBinHome;
    final Path streamBinHome;
    final Path runtimeRoot;
    final Path cnHome;
    final Path dnHome;
    final Path snHome;
    final Path clientHome;
    final String javaBin;

    Process cnProc;
    Process dnProc;
    Process snProc;
    boolean verifiedPsFlags = false;

    IsolatedCluster(Path repoRoot) {
      this.allBinHome =
          repoRoot.resolve(
              "distribution/target/apache-iotdb-2.0.11-SNAPSHOT-all-bin/apache-iotdb-2.0.11-SNAPSHOT-all-bin");
      this.streamBinHome =
          repoRoot.resolve(
              "distribution/target/apache-iotdb-2.0.11-SNAPSHOT-streamnode-bin/apache-iotdb-2.0.11-SNAPSHOT-streamnode-bin");
      if (!Files.isDirectory(allBinHome.resolve("lib"))
          || !Files.isDirectory(streamBinHome.resolve("lib"))) {
        throw new IllegalStateException(
            "Packaged distribution not found. Run mvn install -nsu -pl distribution -am -DskipTests -P with-streamnode");
      }
      this.runtimeRoot = repoRoot.resolve("benchmark/.runtime");
      this.cnHome = runtimeRoot.resolve("confignode");
      this.dnHome = runtimeRoot.resolve("datanode");
      this.snHome = runtimeRoot.resolve("streamnode");
      this.clientHome = runtimeRoot.resolve("client");
      String jh = System.getProperty("java.home");
      this.javaBin = Paths.get(jh, "bin", "java").toString();
    }

    void resetAndStart(int dataRegionGroupNum) throws Exception {
      stopAllProcesses();
      wipeAndPrepareDirectories(dataRegionGroupNum);

      // 1. Start ConfigNode (-Xms1536M -Xmx1536M -Xmn256M)
      cnProc = launchConfigNode();
      waitForTcpPort(CN_INTERNAL_PORT, 30_000L, "ConfigNode internal port");

      // 2. Start DataNode (-Xms2304M -Xmx2304M -Xmn768M) & StreamNode (-Xms3379M -Xmx3379M
      // -Xmn820M)
      dnProc = launchDataNode();
      waitForTcpPort(DN_RPC_PORT, 45_000L, "DataNode RPC port");
      snProc = launchStreamNode();
      waitForTcpPort(SN_INTERNAL_PORT, 45_000L, "StreamNode internal port");

      awaitClusterReady(60_000L);

      if (!verifiedPsFlags) {
        verifyJvmFlagsAndPids();
        verifiedPsFlags = true;
      }
    }

    private void verifyJvmFlagsAndPids() throws IOException, InterruptedException {
      long cnPid = cnProc.pid();
      long dnPid = dnProc.pid();
      long snPid = snProc.pid();
      if (cnPid == dnPid || dnPid == snPid || cnPid == snPid) {
        throw new IllegalStateException("Expected 3 distinct PIDs for CN/DN/SN");
      }
      String psOut = runCapture("ps", "-ww", "-fp", cnPid + "," + dnPid + "," + snPid);
      if (!psOut.contains("-Xmx1536M")
          || !psOut.contains("-Xmn256M")
          || !psOut.contains("-Xmx2304M")
          || !psOut.contains("-Xmn768M")
          || !psOut.contains("-Xmx3379M")
          || !psOut.contains("-Xmn820M")) {
        throw new IllegalStateException("JVM heap flags mismatch in ps output:\n" + psOut);
      }
      System.out.printf(
          Locale.US,
          "[ClusterSpecVerified] Independent PIDs: ConfigNode=%d (-Xmx1536M/-Xmn256M), "
              + "DataNode=%d (-Xmx2304M/-Xmn768M), StreamNode=%d (-Xmx3379M/-Xmn820M)%n",
          cnPid,
          dnPid,
          snPid);
    }

    private void wipeAndPrepareDirectories(int dataRegionGroupNum) throws IOException {
      deleteRecursively(cnHome.resolve("data"));
      deleteRecursively(cnHome.resolve("logs"));
      deleteRecursively(cnHome.resolve("iotdb-subscription"));

      deleteRecursively(dnHome.resolve("data"));
      deleteRecursively(dnHome.resolve("logs"));
      deleteRecursively(dnHome.resolve("ext"));
      deleteRecursively(dnHome.resolve("iotdb-subscription"));

      deleteRecursively(snHome.resolve("data"));
      deleteRecursively(snHome.resolve("logs"));
      deleteRecursively(snHome.resolve("iotdb-subscription"));

      deleteRecursively(clientHome);
      deleteRecursively(Paths.get(System.getProperty("user.dir"), "iotdb-subscription"));

      Files.createDirectories(cnHome.resolve("data"));
      Files.createDirectories(cnHome.resolve("logs"));
      Files.createDirectories(dnHome.resolve("data"));
      Files.createDirectories(dnHome.resolve("logs"));
      Files.createDirectories(snHome.resolve("data"));
      Files.createDirectories(snHome.resolve("logs"));
      Files.createDirectories(clientHome);

      copyDir(allBinHome.resolve("conf"), cnHome.resolve("conf"));
      copyDir(allBinHome.resolve("conf"), dnHome.resolve("conf"));
      copyDir(streamBinHome.resolve("conf"), snHome.resolve("conf"));

      StringBuilder props = new StringBuilder();
      props.append("\n# Isolated Benchmark Settings\n");
      props.append("cluster_name=defaultCluster\n");
      props.append("cn_seed_config_node=127.0.0.1:").append(CN_INTERNAL_PORT).append('\n');
      props.append("dn_seed_config_node=127.0.0.1:").append(CN_INTERNAL_PORT).append('\n');
      props.append("sn_seed_config_node=127.0.0.1:").append(CN_INTERNAL_PORT).append('\n');
      props.append("cn_internal_address=127.0.0.1\n");
      props.append("cn_internal_port=").append(CN_INTERNAL_PORT).append('\n');
      props.append("cn_consensus_port=").append(CN_CONSENSUS_PORT).append('\n');
      props.append("dn_rpc_address=127.0.0.1\n");
      props.append("dn_rpc_port=").append(DN_RPC_PORT).append('\n');
      props.append("dn_internal_address=127.0.0.1\n");
      props.append("dn_internal_port=").append(DN_INTERNAL_PORT).append('\n');
      props.append("dn_mpp_data_exchange_port=").append(DN_MPP_PORT).append('\n');
      props.append("dn_schema_region_consensus_port=").append(DN_SCHEMA_PORT).append('\n');
      props.append("dn_data_region_consensus_port=").append(DN_DATA_PORT).append('\n');
      props.append("sn_internal_address=127.0.0.1\n");
      props.append("sn_internal_port=").append(SN_INTERNAL_PORT).append('\n');
      props.append("sn_cluster_ingress_node_urls=127.0.0.1:").append(DN_RPC_PORT).append('\n');
      props.append("schema_replication_factor=1\n");
      props.append("data_replication_factor=1\n");
      props.append("subscription_enabled=true\n");
      props.append("subscription_consensus_fallback_polling_enabled=false\n");
      props.append(
          "data_region_consensus_protocol_class=org.apache.iotdb.consensus.iot.IoTConsensus\n");
      props.append("data_region_group_extension_policy=CUSTOM\n");
      props
          .append("default_data_region_group_num_per_database=")
          .append(dataRegionGroupNum)
          .append('\n');
      props.append("sn_write_back_session_max_retry_count=10\n");
      props.append("sn_write_back_session_retry_interval_ms=100\n");
      props.append("sn_write_back_session_connection_timeout_ms=1000\n");
      props.append("sn_write_back_max_retry_attempts=5\n");
      props.append("sn_write_back_retry_backoff_base_ms=200\n");

      Files.writeString(
          cnHome.resolve("conf/iotdb-system.properties"), props.toString(), StandardCharsets.UTF_8);
      Files.writeString(
          dnHome.resolve("conf/iotdb-system.properties"), props.toString(), StandardCharsets.UTF_8);
      String snProps =
          props.toString()
              + "sn_system_dir="
              + snHome.resolve("data/streamnode").toAbsolutePath()
              + "\n";
      Files.writeString(
          snHome.resolve("conf/iotdb-system.properties"), snProps, StandardCharsets.UTF_8);
    }

    private Process launchConfigNode() throws IOException {
      List<String> cmd = new ArrayList<>();
      cmd.add(javaBin);
      cmd.addAll(Arrays.asList(ADD_OPENS_ARGS));
      cmd.add("-Xms1536M");
      cmd.add("-Xmx1536M");
      cmd.add("-Xmn256M");
      cmd.add("-XX:MaxDirectMemorySize=512M");
      cmd.add("-Dlogback.configurationFile=" + cnHome.resolve("conf/logback-confignode.xml"));
      cmd.add("-DCONFIGNODE_HOME=" + cnHome);
      cmd.add("-DCONFIGNODE_DATA_HOME=" + cnHome.resolve("data"));
      cmd.add("-DTSFILE_HOME=" + cnHome);
      cmd.add("-DCONFIGNODE_CONF=" + cnHome.resolve("conf"));
      cmd.add("-DTSFILE_CONF=" + cnHome.resolve("conf"));
      cmd.add("-DCONFIGNODE_LOGS=" + cnHome.resolve("logs"));
      cmd.add("-Dname=bench.iotdb.ConfigNode");
      cmd.add("-Diotdb-foreground=yes");
      cmd.add("-cp");
      cmd.add(allBinHome.resolve("lib/*").toString());
      cmd.add("org.apache.iotdb.confignode.service.ConfigNode");
      cmd.add("-s");
      return startNodeProcess(cmd, cnHome, cnHome.resolve("logs/stdout.log"));
    }

    private Process launchDataNode() throws IOException {
      List<String> cmd = new ArrayList<>();
      cmd.add(javaBin);
      cmd.addAll(Arrays.asList(ADD_OPENS_ARGS));
      cmd.add("-Xms2304M");
      cmd.add("-Xmx2304M");
      cmd.add("-Xmn768M");
      cmd.add("-XX:MaxDirectMemorySize=768M");
      cmd.add("-DOFF_HEAP_MEMORY=768M");
      cmd.add("-Dlogback.configurationFile=" + dnHome.resolve("conf/logback-datanode.xml"));
      cmd.add("-DIOTDB_HOME=" + dnHome);
      cmd.add("-DIOTDB_DATA_HOME=" + dnHome.resolve("data"));
      cmd.add("-DTSFILE_HOME=" + dnHome);
      cmd.add("-DIOTDB_CONF=" + dnHome.resolve("conf"));
      cmd.add("-DTSFILE_CONF=" + dnHome.resolve("conf"));
      cmd.add("-DIOTDB_LOG_DIR=" + dnHome.resolve("logs"));
      cmd.add("-Dname=bench.iotdb.DataNode");
      cmd.add("-Diotdb-foreground=yes");
      cmd.add("-cp");
      cmd.add(allBinHome.resolve("lib/*").toString());
      cmd.add("org.apache.iotdb.db.service.DataNode");
      cmd.add("-s");
      return startNodeProcess(cmd, dnHome, dnHome.resolve("logs/stdout.log"));
    }

    private Process launchStreamNode() throws IOException {
      List<String> cmd = new ArrayList<>();
      cmd.add(javaBin);
      cmd.addAll(Arrays.asList(ADD_OPENS_ARGS));
      cmd.add("-Xms3379M");
      cmd.add("-Xmx3379M");
      cmd.add("-Xmn820M");
      cmd.add("-XX:MaxDirectMemorySize=845M");
      cmd.add("-Dlogback.configurationFile=" + snHome.resolve("conf/logback-streamnode.xml"));
      cmd.add("-DSTREAMNODE_HOME=" + snHome);
      cmd.add("-DTSFILE_HOME=" + snHome);
      cmd.add("-DSTREAMNODE_CONF=" + snHome.resolve("conf"));
      cmd.add("-DTSFILE_CONF=" + snHome.resolve("conf"));
      cmd.add("-DSTREAMNODE_LOGS=" + snHome.resolve("logs"));
      cmd.add("-Dname=bench.iotdb.StreamNode");
      cmd.add("-Diotdb-foreground=yes");
      cmd.add("-cp");
      cmd.add(streamBinHome.resolve("lib/*").toString());
      cmd.add("org.apache.iotdb.streamnode.StreamNode");
      cmd.add("-s");
      return startNodeProcess(cmd, snHome, snHome.resolve("logs/stdout.log"));
    }

    private static Process startNodeProcess(List<String> cmd, Path cwd, Path stdoutLog)
        throws IOException {
      ProcessBuilder pb = new ProcessBuilder(cmd);
      pb.directory(cwd.toFile());
      pb.redirectErrorStream(true);
      pb.redirectOutput(stdoutLog.toFile());
      return pb.start();
    }

    private void awaitClusterReady(long timeoutMs) throws Exception {
      long deadline = System.currentTimeMillis() + timeoutMs;
      while (System.currentTimeMillis() < deadline) {
        if (!cnProc.isAlive() || !dnProc.isAlive() || !snProc.isAlive()) {
          throw new IllegalStateException(
              "A cluster JVM exited early: cn="
                  + cnProc.isAlive()
                  + ", dn="
                  + dnProc.isAlive()
                  + ", sn="
                  + snProc.isAlive());
        }
        boolean nodesRunning = false;
        try (ISession sess = openTreeSession()) {
          SessionDataSet ds = sess.executeQueryStatement("SHOW CLUSTER");
          int runningCount = 0;
          boolean sawCn = false;
          boolean sawDn = false;
          while (ds.hasNext()) {
            RowRecord rec = ds.next();
            String line = rec.getFields().toString();
            if (line.contains("ConfigNode") && line.contains("Running")) {
              sawCn = true;
              runningCount++;
            }
            if (line.contains("DataNode") && line.contains("Running")) {
              sawDn = true;
              runningCount++;
            }
            if (line.contains("StreamNode") && line.contains("Running")) {
              runningCount++;
            }
          }
          ds.close();
          nodesRunning = sawCn && sawDn && runningCount >= 3;
        } catch (Exception ignored) {
          // cluster still initializing
        }
        if (nodesRunning
            && logContains(
                snHome.resolve("logs/log_streamnode_all.log"), "StreamNode started successfully")) {
          return;
        }
        Thread.sleep(250);
      }
      throw new IllegalStateException(
          "Cluster did not reach ready state within " + timeoutMs + "ms");
    }

    NodeStats captureNodeStats() {
      double cnRss = processRssMb(cnProc.pid());
      double dnRss = processRssMb(dnProc.pid());
      double snRss = processRssMb(snProc.pid());
      double dnCpu = processCpuMs(dnProc);
      double snCpu = processCpuMs(snProc);
      return new NodeStats(cnRss, dnRss, snRss, dnCpu, snCpu);
    }

    ISession openTreeSession() throws IoTDBConnectionException {
      ISession sess =
          new Session.Builder()
              .host("127.0.0.1")
              .port(DN_RPC_PORT)
              .username("root")
              .password("root")
              .build();
      sess.open(false);
      return sess;
    }

    ITableSession openTableSession(String db) throws IoTDBConnectionException {
      TableSessionBuilder builder =
          new TableSessionBuilder()
              .nodeUrls(Collections.singletonList("127.0.0.1:" + DN_RPC_PORT))
              .username("root")
              .password("root");
      if (db != null) {
        builder.database(db);
      }
      return builder.build();
    }

    void stopAllProcesses() throws InterruptedException {
      terminateProcess(snProc);
      terminateProcess(dnProc);
      terminateProcess(cnProc);
      snProc = null;
      dnProc = null;
      cnProc = null;

      // Also sweep any orphaned child JVMs tied to runtimeRoot
      String marker = runtimeRoot.toAbsolutePath().toString();
      ProcessHandle.allProcesses()
          .forEach(
              ph -> {
                if (ph.pid() == ProcessHandle.current().pid()) {
                  return;
                }
                Optional<String> cmdLine = ph.info().commandLine();
                if (cmdLine.isPresent() && cmdLine.get().contains(marker)) {
                  ph.destroyForcibly();
                }
              });

      waitForPortsClosed(CLUSTER_PORTS, 20_000L);
    }

    @Override
    public void close() throws Exception {
      stopAllProcesses();
      deleteRecursively(runtimeRoot);
    }
  }

  private static final class NodeStats {
    final double cnRssMb;
    final double dnRssMb;
    final double snRssMb;
    final double dnCpuMs;
    final double snCpuMs;

    NodeStats(double cnRssMb, double dnRssMb, double snRssMb, double dnCpuMs, double snCpuMs) {
      this.cnRssMb = cnRssMb;
      this.dnRssMb = dnRssMb;
      this.snRssMb = snRssMb;
      this.dnCpuMs = dnCpuMs;
      this.snCpuMs = snCpuMs;
    }
  }

  private static final class TrialRecord {
    final Map<String, Double> metrics = new LinkedHashMap<>();
  }

  public static void main(String[] args) throws Exception {
    Path repoRoot = resolveRepoRoot();
    Path datasetsDir = repoRoot.resolve("benchmark/datasets");
    Path resultsDir = repoRoot.resolve("benchmark/results");
    Files.createDirectories(resultsDir);
    Path triggerJar = repoRoot.resolve("benchmark/triggers/WindowAggregationSyncTrigger.jar");
    if (!Files.isRegularFile(triggerJar)) {
      throw new IllegalStateException("Trigger JAR not found at " + triggerJar);
    }

    List<String> rawTrialLines = new ArrayList<>();
    rawTrialLines.add(
        "experiment,round,group,run_order_idx,completeness_recall,premature_close_rate,"
            + "precision,recall,f1,mae_ms,false_split_rate,write_throughput_pts_sec,"
            + "write_p50_ms,write_p95_ms,write_p99_ms,trigger_e2e_p50_ms,trigger_e2e_p99_ms,"
            + "e2e_drain_ms,p99_rel_error,cn_rss_mb,dn_rss_mb,sn_rss_mb,dn_cpu_ms,sn_cpu_ms");

    try (IsolatedCluster cluster = new IsolatedCluster(repoRoot)) {
      runClusterExp1(cluster, resultsDir, rawTrialLines);
      runClusterExp2(cluster, datasetsDir, resultsDir, rawTrialLines);
      runClusterExp3(cluster, resultsDir, rawTrialLines);
      runClusterExp4(cluster, triggerJar, resultsDir, rawTrialLines);
    } finally {
      Files.write(
          resultsDir.resolve("cluster_raw_trials.csv"), rawTrialLines, StandardCharsets.UTF_8);
    }

    System.out.println("Completed all 42 isolated 3-JVM cluster benchmark runs cleanly.");
    System.exit(0);
  }

  // --------------------------------------------------------------------------------------------
  // Cluster Exp-1: Multi-Region Partition Watermark Alignment (2 groups x 3 alternating rounds)
  // --------------------------------------------------------------------------------------------

  private static void runClusterExp1(
      IsolatedCluster cluster, Path resultsDir, List<String> rawLines) throws Exception {
    List<String> groups = Arrays.asList("Unaligned_Default", "Ours_Aligned_Watermark");
    Map<String, List<TrialRecord>> byGroup = initGroupMap(groups);

    for (int round = 0; round < ROUNDS; round++) {
      for (int step = 0; step < groups.size(); step++) {
        String group = groups.get((step + round) % groups.size());
        boolean aligned = "Ours_Aligned_Watermark".equals(group);
        System.out.printf(
            Locale.US, "[Exp-1] Round %d/%d, Order %d: %s%n", round + 1, ROUNDS, step, group);
        cluster.resetAndStart(4);

        TrialRecord tr = executeExp1Unit(cluster, aligned);
        byGroup.get(group).add(tr);
        rawLines.add(formatRawTrialLine("Exp1_Alignment", round, group, step, tr));
      }
    }

    List<String> out = new ArrayList<>();
    out.add(
        "group,rounds,completeness_recall_mean,completeness_recall_std,premature_close_rate_mean,"
            + "write_throughput_pts_sec_mean,e2e_drain_ms_mean,cn_rss_mb_mean,dn_rss_mb_mean,"
            + "sn_rss_mb_mean,sn_cpu_ms_mean");
    for (String g : groups) {
      List<TrialRecord> list = byGroup.get(g);
      out.add(
          String.format(
              Locale.US,
              "%s,%d,%.4f,%.4f,%.4f,%.1f,%.2f,%.1f,%.1f,%.1f,%.1f",
              g,
              list.size(),
              meanOf(list, "completeness_recall"),
              stdOf(list, "completeness_recall"),
              meanOf(list, "premature_close_rate"),
              meanOf(list, "write_throughput_pts_sec"),
              meanOf(list, "e2e_drain_ms"),
              meanOf(list, "cn_rss_mb"),
              meanOf(list, "dn_rss_mb"),
              meanOf(list, "sn_rss_mb"),
              meanOf(list, "sn_cpu_ms")));
    }
    Files.write(resultsDir.resolve("exp1_cluster_alignment.csv"), out, StandardCharsets.UTF_8);
  }

  private static TrialRecord executeExp1Unit(IsolatedCluster cluster, boolean aligned)
      throws Exception {
    String db = "exp1_align_db";
    try (ITableSession sess = cluster.openTableSession(null)) {
      sess.executeNonQueryStatement("CREATE DATABASE " + db);
      sess.executeNonQueryStatement("USE " + db);
      sess.executeNonQueryStatement(
          "CREATE TABLE align_src(time TIMESTAMP TIME, device_id STRING TAG, val DOUBLE FIELD)");
      sess.executeNonQueryStatement(
          "CREATE TABLE align_sink(time TIMESTAMP TIME, cnt INT64 FIELD, total DOUBLE FIELD)");
      String props =
          aligned
              ? "WITH (align_partition_watermark = true, partition_stale_timeout_ms = 25s)"
              : "WITH (align_partition_watermark = false)";
      sess.executeNonQueryStatement(
          "CREATE STREAM s_align FROM align_src TUMBLE(10s, 0) "
              + "INTO align_sink(time, cnt, total) "
              + "SELECT ${START_TIME}, count(*), sum(val) FROM ${ROWS} "
              + props);
      sess.executeNonQueryStatement("START STREAM s_align");
      awaitStreamRunning(sess, "s_align");
    }

    int fastDevs = 6; // d0..d5 lead by +2 windows (+20s)
    int slowDevs = 2; // d6..d7 lag by -2 windows (-20s)
    int rowsPerDevPerWin = 100;
    int windows = 20; // 20 x 800 = 16,000 target points
    long base = alignedFutureMinute() + 60_000L;

    List<Double> batchLats = new ArrayList<>();
    long wallStart = System.nanoTime();
    try (ITableSession sess = cluster.openTableSession(db)) {
      // Seed all 8 devices across 4 DataRegions before window 0 begins so all region partitions
      // exist
      Tablet seedTab = newTableTablet("align_src", (fastDevs + slowDevs) * 5);
      int idx = 0;
      for (int d = 0; d < fastDevs + slowDevs; d++) {
        for (int r = 0; r < 5; r++) {
          seedTab.addTimestamp(idx, base - 10_000L + r * 1_000L);
          seedTab.addValue("device_id", idx, "d" + d);
          seedTab.addValue("val", idx, 1.0);
          idx++;
        }
      }
      sess.insert(seedTab);
      Thread.sleep(600);

      Tablet fastTab = newTableTablet("align_src", fastDevs * rowsPerDevPerWin);
      Tablet slowTab = newTableTablet("align_src", slowDevs * rowsPerDevPerWin);
      for (int t = 0; t < windows + 2; t++) {
        if (t < windows) {
          fastTab.reset();
          int ri = 0;
          long wStart = base + t * 10_000L;
          for (int d = 0; d < fastDevs; d++) {
            for (int r = 0; r < rowsPerDevPerWin; r++) {
              fastTab.addTimestamp(ri, wStart + r * 50L);
              fastTab.addValue("device_id", ri, "d" + d);
              fastTab.addValue("val", ri, 1.0);
              ri++;
            }
          }
          long t0 = System.nanoTime();
          sess.insert(fastTab);
          batchLats.add((System.nanoTime() - t0) / 1e6);
        }
        if (t >= 2 && t - 2 < windows) {
          int sw = t - 2;
          slowTab.reset();
          int ri = 0;
          long wStart = base + sw * 10_000L;
          for (int d = fastDevs; d < fastDevs + slowDevs; d++) {
            for (int r = 0; r < rowsPerDevPerWin; r++) {
              slowTab.addTimestamp(ri, wStart + r * 50L);
              slowTab.addValue("device_id", ri, "d" + d);
              slowTab.addValue("val", ri, 1.0);
              ri++;
            }
          }
          long t0 = System.nanoTime();
          sess.insert(slowTab);
          batchLats.add((System.nanoTime() - t0) / 1e6);
        }
        Thread.sleep(40);
      }

      // Final pusher past all windows across all devices
      Tablet pusher = newTableTablet("align_src", fastDevs + slowDevs);
      long pushTs = base + windows * 10_000L + 5_000L;
      for (int d = 0; d < fastDevs + slowDevs; d++) {
        pusher.addTimestamp(d, pushTs);
        pusher.addValue("device_id", d, "d" + d);
        pusher.addValue("val", d, 1.0);
      }
      sess.insert(pusher);
    }

    double writeSec = Math.max(1e-6, (System.nanoTime() - wallStart) / 1e9);
    int totalPts = windows * (fastDevs + slowDevs) * rowsPerDevPerWin;

    long drainStart = System.nanoTime();
    try (ITableSession sess = cluster.openTableSession(db)) {
      awaitSinkCount(sess, db + ".align_sink", windows, 35_000L);
      Thread.sleep(1_200L); // let any straggling writes settle
    }
    double drainMs = (System.nanoTime() - drainStart) / 1e6;

    long includedRows = 0L;
    int prematureWindows = 0;
    long expectedPerWindow = (long) (fastDevs + slowDevs) * rowsPerDevPerWin;
    try (ITableSession sess = cluster.openTableSession(db)) {
      SessionDataSet ds =
          sess.executeQueryStatement(
              "SELECT time, cnt FROM "
                  + db
                  + ".align_sink WHERE time >= "
                  + base
                  + " AND time < "
                  + (base + windows * 10_000L)
                  + " ORDER BY time");
      Map<Long, Long> winCounts = new LinkedHashMap<>();
      while (ds.hasNext()) {
        RowRecord r = ds.next();
        long ts = r.getFields().get(0).getLongV();
        long cnt = r.getFields().get(1).getLongV();
        winCounts.put(ts, cnt);
      }
      ds.close();
      for (int w = 0; w < windows; w++) {
        long wTs = base + w * 10_000L;
        long cnt = winCounts.getOrDefault(wTs, 0L);
        includedRows += Math.min(expectedPerWindow, cnt);
        if (cnt < expectedPerWindow) {
          prematureWindows++;
        }
      }
    }

    NodeStats ns = cluster.captureNodeStats();
    TrialRecord tr = new TrialRecord();
    tr.metrics.put("completeness_recall", (double) includedRows / totalPts);
    tr.metrics.put("premature_close_rate", (double) prematureWindows / windows);
    tr.metrics.put("write_throughput_pts_sec", totalPts / writeSec);
    double[] lats = toSortedArray(batchLats, 0.0);
    tr.metrics.put("write_p50_ms", percentile(lats, 0.50));
    tr.metrics.put("write_p95_ms", percentile(lats, 0.95));
    tr.metrics.put("write_p99_ms", percentile(lats, 0.99));
    tr.metrics.put("e2e_drain_ms", drainMs);
    tr.metrics.put("cn_rss_mb", ns.cnRssMb);
    tr.metrics.put("dn_rss_mb", ns.dnRssMb);
    tr.metrics.put("sn_rss_mb", ns.snRssMb);
    tr.metrics.put("dn_cpu_ms", ns.dnCpuMs);
    tr.metrics.put("sn_cpu_ms", ns.snCpuMs);
    return tr;
  }

  // --------------------------------------------------------------------------------------------
  // Cluster Exp-2: Live SQL VARIATION Detection (5 detectors x 3 alternating rounds)
  // --------------------------------------------------------------------------------------------

  private static final class TracePoint {
    final long ts;
    final double val;
    final boolean isCp;
    final boolean isSpike;

    TracePoint(long ts, double val, boolean isCp, boolean isSpike) {
      this.ts = ts;
      this.val = val;
      this.isCp = isCp;
      this.isSpike = isSpike;
    }
  }

  private static List<TracePoint> loadCisdiTrace(Path csvPath) throws IOException {
    List<TracePoint> pts = new ArrayList<>();
    try (BufferedReader br = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
      String header = br.readLine();
      String[] cols = header.split(",");
      int tsIdx = Arrays.asList(cols).indexOf("timestamp_ms");
      int valIdx = Arrays.asList(cols).indexOf("value");
      int cpIdx = Arrays.asList(cols).indexOf("is_true_changepoint");
      int spIdx = Arrays.asList(cols).indexOf("is_injected_spike");
      String line;
      while ((line = br.readLine()) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }
        String[] p = line.split(",");
        pts.add(
            new TracePoint(
                Long.parseLong(p[tsIdx]),
                Double.parseDouble(p[valIdx]),
                Integer.parseInt(p[cpIdx]) == 1,
                Integer.parseInt(p[spIdx]) == 1));
      }
    }
    return pts;
  }

  private static void runClusterExp2(
      IsolatedCluster cluster, Path datasetsDir, Path resultsDir, List<String> rawLines)
      throws Exception {
    List<TracePoint> trace = loadCisdiTrace(datasetsDir.resolve("cisdi_regime_jitter.csv"));
    List<String> groups =
        Arrays.asList("Raw_Delta", "Duration_Rule", "ADWIN", "OPTWIN", "Ours_Evidence_Backtrack");
    Map<String, List<TrialRecord>> byGroup = initGroupMap(groups);

    for (int round = 0; round < ROUNDS; round++) {
      for (int step = 0; step < groups.size(); step++) {
        String group = groups.get((step + round) % groups.size());
        System.out.printf(
            Locale.US, "[Exp-2] Round %d/%d, Order %d: %s%n", round + 1, ROUNDS, step, group);
        cluster.resetAndStart(1);

        TrialRecord tr = executeExp2Unit(cluster, group, trace);
        byGroup.get(group).add(tr);
        rawLines.add(formatRawTrialLine("Exp2_Variation", round, group, step, tr));
      }
    }

    List<String> out = new ArrayList<>();
    out.add(
        "method,rounds,precision_mean,recall_mean,f1_mean,f1_std,mae_ms_mean,"
            + "false_split_rate_mean,write_throughput_pts_sec_mean,e2e_drain_ms_mean,"
            + "cn_rss_mb_mean,dn_rss_mb_mean,sn_rss_mb_mean,sn_cpu_ms_mean");
    for (String g : groups) {
      List<TrialRecord> list = byGroup.get(g);
      out.add(
          String.format(
              Locale.US,
              "%s,%d,%.4f,%.4f,%.4f,%.4f,%.2f,%.4f,%.1f,%.2f,%.1f,%.1f,%.1f,%.1f",
              g,
              list.size(),
              meanOf(list, "precision"),
              meanOf(list, "recall"),
              meanOf(list, "f1"),
              stdOf(list, "f1"),
              meanOf(list, "mae_ms"),
              meanOf(list, "false_split_rate"),
              meanOf(list, "write_throughput_pts_sec"),
              meanOf(list, "e2e_drain_ms"),
              meanOf(list, "cn_rss_mb"),
              meanOf(list, "dn_rss_mb"),
              meanOf(list, "sn_rss_mb"),
              meanOf(list, "sn_cpu_ms")));
    }
    Files.write(resultsDir.resolve("exp2_cluster_variation.csv"), out, StandardCharsets.UTF_8);
  }

  private static TrialRecord executeExp2Unit(
      IsolatedCluster cluster, String group, List<TracePoint> trace) throws Exception {
    String db = "exp2_var_db";
    String withProps;
    switch (group) {
      case "Raw_Delta":
        withProps = "WITH (variation_detector = 'delta')";
        break;
      case "Duration_Rule":
        withProps = "WITH (variation_detector = 'duration', variation_min_samples = 3)";
        break;
      case "ADWIN":
        withProps = "WITH (variation_detector = 'adwin', variation_min_samples = 5)";
        break;
      case "OPTWIN":
        withProps = "WITH (variation_detector = 'optwin', variation_min_samples = 5)";
        break;
      default:
        withProps =
            "WITH (variation_detector = 'evidence', variation_theta = 2.5, "
                + "variation_epsilon = 2.0, variation_min_samples = 3, "
                + "variation_adaptive_baseline = true)";
        break;
    }

    try (ITableSession sess = cluster.openTableSession(null)) {
      sess.executeNonQueryStatement("CREATE DATABASE " + db);
      sess.executeNonQueryStatement("USE " + db);
      sess.executeNonQueryStatement(
          "CREATE TABLE var_src(time TIMESTAMP TIME, device_id STRING TAG, val DOUBLE FIELD)");
      sess.executeNonQueryStatement(
          "CREATE TABLE var_sink(time TIMESTAMP TIME, device_id STRING TAG, prev_v DOUBLE FIELD, "
              + "curr_v DOUBLE FIELD, cnt INT64 FIELD, avg_v DOUBLE FIELD)");
      sess.executeNonQueryStatement(
          "CREATE STREAM s_var FROM var_src PARTITION BY device_id VARIATION(col => val, delta => 2.5) "
              + "INTO var_sink(time, device_id, prev_v, curr_v, cnt, avg_v) "
              + "SELECT ${START_TIME}, ${1}, ${PREV_VALUE}, ${CURRENT_VALUE}, ${ROW_NUM}, avg(val) "
              + "FROM ${ROWS} "
              + withProps);
      sess.executeNonQueryStatement("START STREAM s_var");
      awaitStreamRunning(sess, "s_var");
    }

    long base = alignedFutureMinute();
    long offset = base - trace.get(0).ts;
    List<Long> gtTimestamps = new ArrayList<>();
    List<Long> spikeTimestamps = new ArrayList<>();
    for (TracePoint tp : trace) {
      long shifted = tp.ts + offset;
      if (tp.isCp) {
        gtTimestamps.add(shifted);
      }
      if (tp.isSpike) {
        spikeTimestamps.add(shifted);
      }
    }

    int batchSize = 200;
    List<Double> batchLats = new ArrayList<>();
    long wallStart = System.nanoTime();
    try (ITableSession sess = cluster.openTableSession(db)) {
      Tablet tablet = newTableTablet("var_src", batchSize);
      for (int start = 0; start < trace.size(); start += batchSize) {
        int end = Math.min(trace.size(), start + batchSize);
        tablet.reset();
        for (int i = start; i < end; i++) {
          int row = i - start;
          tablet.addTimestamp(row, trace.get(i).ts + offset);
          tablet.addValue("device_id", row, "cisdi_01");
          tablet.addValue("val", row, trace.get(i).val);
        }
        long t0 = System.nanoTime();
        sess.insert(tablet);
        batchLats.add((System.nanoTime() - t0) / 1e6);
      }
      // Synthetic tail regime shift so the last plateau also closes deterministically
      long lastTs = trace.get(trace.size() - 1).ts + offset;
      double lastVal = trace.get(trace.size() - 1).val;
      Tablet tailTab = newTableTablet("var_src", 12);
      for (int r = 0; r < 12; r++) {
        tailTab.addTimestamp(r, lastTs + (r + 1) * 200L);
        tailTab.addValue("device_id", r, "cisdi_01");
        tailTab.addValue("val", r, lastVal + 60.0);
      }
      sess.insert(tailTab);
    }
    double writeSec = Math.max(1e-6, (System.nanoTime() - wallStart) / 1e9);

    long drainStart = System.nanoTime();
    List<Long> detectedBoundaryTs = new ArrayList<>();
    try (ITableSession sess = cluster.openTableSession(db)) {
      awaitSinkStableCount(sess, db + ".var_sink", 1, 20_000L, 1_500L);
      SessionDataSet ds =
          sess.executeQueryStatement("SELECT time FROM " + db + ".var_sink ORDER BY time");
      List<Long> startTimes = new ArrayList<>();
      while (ds.hasNext()) {
        startTimes.add(ds.next().getFields().get(0).getLongV());
      }
      ds.close();
      // Window 0 starts at trace[0]; windows 1..K start at detected change-point onset timestamps
      // Exclude synthetic closing tail timestamp beyond lastTs
      long lastTraceTs = trace.get(trace.size() - 1).ts + offset;
      for (int i = 1; i < startTimes.size(); i++) {
        long t = startTimes.get(i);
        if (t <= lastTraceTs) {
          detectedBoundaryTs.add(t);
        }
      }
    }
    double drainMs = (System.nanoTime() - drainStart) / 1e6;

    // 1-to-1 greedy matching within +-3000 ms (15 samples * 200ms)
    long tolMs = 3_000L;
    boolean[] gtMatched = new boolean[gtTimestamps.size()];
    int tpCount = 0;
    int fpCount = 0;
    double maeSumMs = 0.0;
    int spikeFalseSplits = 0;
    for (long detTs : detectedBoundaryTs) {
      int bestGt = -1;
      long bestDist = Long.MAX_VALUE;
      for (int g = 0; g < gtTimestamps.size(); g++) {
        if (gtMatched[g]) {
          continue;
        }
        long dist = Math.abs(detTs - gtTimestamps.get(g));
        if (dist <= tolMs && dist < bestDist) {
          bestDist = dist;
          bestGt = g;
        }
      }
      if (bestGt >= 0) {
        gtMatched[bestGt] = true;
        tpCount++;
        maeSumMs += bestDist;
      } else {
        fpCount++;
        for (long spTs : spikeTimestamps) {
          if (Math.abs(detTs - spTs) <= 600L) {
            spikeFalseSplits++;
            break;
          }
        }
      }
    }

    double prec = (tpCount + fpCount) > 0 ? (double) tpCount / (tpCount + fpCount) : 0.0;
    double rec = !gtTimestamps.isEmpty() ? (double) tpCount / gtTimestamps.size() : 1.0;
    double f1 = (prec + rec) > 0 ? (2.0 * prec * rec) / (prec + rec) : 0.0;
    double maeMs = tpCount > 0 ? maeSumMs / tpCount : -1.0;
    double falseSplitRate =
        !spikeTimestamps.isEmpty()
            ? Math.min(1.0, (double) spikeFalseSplits / spikeTimestamps.size())
            : 0.0;

    NodeStats ns = cluster.captureNodeStats();
    TrialRecord tr = new TrialRecord();
    tr.metrics.put("precision", prec);
    tr.metrics.put("recall", rec);
    tr.metrics.put("f1", f1);
    tr.metrics.put("mae_ms", maeMs);
    tr.metrics.put("false_split_rate", falseSplitRate);
    tr.metrics.put("write_throughput_pts_sec", trace.size() / writeSec);
    double[] lats = toSortedArray(batchLats, 0.0);
    tr.metrics.put("write_p50_ms", percentile(lats, 0.50));
    tr.metrics.put("write_p95_ms", percentile(lats, 0.95));
    tr.metrics.put("write_p99_ms", percentile(lats, 0.99));
    tr.metrics.put("e2e_drain_ms", drainMs);
    tr.metrics.put("cn_rss_mb", ns.cnRssMb);
    tr.metrics.put("dn_rss_mb", ns.dnRssMb);
    tr.metrics.put("sn_rss_mb", ns.snRssMb);
    tr.metrics.put("dn_cpu_ms", ns.dnCpuMs);
    tr.metrics.put("sn_cpu_ms", ns.snCpuMs);
    return tr;
  }

  // --------------------------------------------------------------------------------------------
  // Cluster Exp-3: Live Multi-Stream Slice Reuse & KLL (3 modes x 3 alternating rounds)
  // --------------------------------------------------------------------------------------------

  private static void runClusterExp3(
      IsolatedCluster cluster, Path resultsDir, List<String> rawLines) throws Exception {
    List<String> groups =
        Arrays.asList("No_Reuse_Independent", "Slice_Reuse_TDigest", "Ours_Slice_Reuse_KLL");
    Map<String, List<TrialRecord>> byGroup = initGroupMap(groups);

    for (int round = 0; round < ROUNDS; round++) {
      for (int step = 0; step < groups.size(); step++) {
        String group = groups.get((step + round) % groups.size());
        System.out.printf(
            Locale.US, "[Exp-3] Round %d/%d, Order %d: %s%n", round + 1, ROUNDS, step, group);
        cluster.resetAndStart(1);

        TrialRecord tr = executeExp3Unit(cluster, group);
        byGroup.get(group).add(tr);
        rawLines.add(formatRawTrialLine("Exp3_SliceReuse", round, group, step, tr));
      }
    }

    List<String> out = new ArrayList<>();
    out.add(
        "method,rounds,write_throughput_pts_sec_mean,e2e_drain_ms_mean,e2e_drain_ms_std,"
            + "p99_rel_error_mean,cn_rss_mb_mean,dn_rss_mb_mean,sn_rss_mb_mean,"
            + "sn_cpu_ms_mean,sn_cpu_ms_std");
    for (String g : groups) {
      List<TrialRecord> list = byGroup.get(g);
      out.add(
          String.format(
              Locale.US,
              "%s,%d,%.1f,%.2f,%.2f,%.5f,%.1f,%.1f,%.1f,%.1f,%.1f",
              g,
              list.size(),
              meanOf(list, "write_throughput_pts_sec"),
              meanOf(list, "e2e_drain_ms"),
              stdOf(list, "e2e_drain_ms"),
              meanOf(list, "p99_rel_error"),
              meanOf(list, "cn_rss_mb"),
              meanOf(list, "dn_rss_mb"),
              meanOf(list, "sn_rss_mb"),
              meanOf(list, "sn_cpu_ms"),
              stdOf(list, "sn_cpu_ms")));
    }
    Files.write(resultsDir.resolve("exp3_cluster_slice_reuse.csv"), out, StandardCharsets.UTF_8);
  }

  private static TrialRecord executeExp3Unit(IsolatedCluster cluster, String group)
      throws Exception {
    String db = "exp3_reuse_db";
    String baseProps;
    String props1m;
    String props5m;
    if ("No_Reuse_Independent".equals(group)) {
      baseProps = "WITH (enable_slice_reuse = false)";
      props1m = "WITH (enable_slice_reuse = false, watermark = 10s)";
      props5m = "WITH (enable_slice_reuse = false, watermark = 20s)";
    } else if ("Slice_Reuse_TDigest".equals(group)) {
      baseProps =
          "WITH (enable_slice_reuse = true, reuse_sketch_type = 'tdigest', slice_retention_ms = 10m)";
      props1m =
          "WITH (enable_slice_reuse = true, reuse_sketch_type = 'tdigest', slice_retention_ms = 10m, watermark = 10s)";
      props5m = "WITH (enable_slice_reuse = true, reuse_sketch_type = 'tdigest', watermark = 20s)";
    } else {
      baseProps =
          "WITH (enable_slice_reuse = true, reuse_sketch_type = 'kll', reuse_kll_k = 200, slice_retention_ms = 10m)";
      props1m =
          "WITH (enable_slice_reuse = true, reuse_sketch_type = 'kll', reuse_kll_k = 200, slice_retention_ms = 10m, watermark = 10s)";
      props5m =
          "WITH (enable_slice_reuse = true, reuse_sketch_type = 'kll', reuse_kll_k = 200, watermark = 20s)";
    }

    try (ITableSession sess = cluster.openTableSession(null)) {
      sess.executeNonQueryStatement("CREATE DATABASE " + db);
      sess.executeNonQueryStatement("USE " + db);
      sess.executeNonQueryStatement(
          "CREATE TABLE src(time TIMESTAMP TIME, device_id STRING TAG, val DOUBLE FIELD)");
      String sinkSchema =
          "(time TIMESTAMP TIME, cnt INT64 FIELD, total DOUBLE FIELD, q DOUBLE FIELD)";
      sess.executeNonQueryStatement("CREATE TABLE sink_10s" + sinkSchema);
      sess.executeNonQueryStatement("CREATE TABLE sink_1m_p50" + sinkSchema);
      sess.executeNonQueryStatement("CREATE TABLE sink_1m_p90" + sinkSchema);
      sess.executeNonQueryStatement("CREATE TABLE sink_1m_p99" + sinkSchema);
      sess.executeNonQueryStatement("CREATE TABLE sink_5m_p99" + sinkSchema);

      sess.executeNonQueryStatement(
          "CREATE STREAM s_10s FROM src TUMBLE(10s, 0) INTO sink_10s(time, cnt, total, q) "
              + "SELECT ${START_TIME}, count(*), sum(val), approx_percentile(val, 0.50) FROM ${ROWS} "
              + baseProps);
      sess.executeNonQueryStatement(
          "CREATE STREAM s_1m_p50 FROM src TUMBLE(1m, 0) INTO sink_1m_p50(time, cnt, total, q) "
              + "SELECT ${START_TIME}, count(*), sum(val), approx_percentile(val, 0.50) FROM ${ROWS} "
              + props1m);
      sess.executeNonQueryStatement(
          "CREATE STREAM s_1m_p90 FROM src TUMBLE(1m, 0) INTO sink_1m_p90(time, cnt, total, q) "
              + "SELECT ${START_TIME}, count(*), sum(val), approx_percentile(val, 0.90) FROM ${ROWS} "
              + props1m);
      sess.executeNonQueryStatement(
          "CREATE STREAM s_1m_p99 FROM src TUMBLE(1m, 0) INTO sink_1m_p99(time, cnt, total, q) "
              + "SELECT ${START_TIME}, count(*), sum(val), approx_percentile(val, 0.99) FROM ${ROWS} "
              + props1m);
      sess.executeNonQueryStatement(
          "CREATE STREAM s_5m_p99 FROM src TUMBLE(5m, 0) INTO sink_5m_p99(time, cnt, total, q) "
              + "SELECT ${START_TIME}, count(*), sum(val), approx_percentile(val, 0.99) FROM ${ROWS} "
              + props5m);

      for (String sName : Arrays.asList("s_10s", "s_1m_p50", "s_1m_p90", "s_1m_p99", "s_5m_p99")) {
        sess.executeNonQueryStatement("START STREAM " + sName);
      }
      for (String sName : Arrays.asList("s_10s", "s_1m_p50", "s_1m_p90", "s_1m_p99", "s_5m_p99")) {
        awaitStreamRunning(sess, sName);
      }
    }

    // 600 batches x 100 rows (100ms step across 5 devices -> 1200s = 20m: 120x10s, 20x1m, 4x5m)
    long base = alignedFutureMinute();
    int batches = 600;
    int rowsPerBatch = 100;
    double[] firstMinuteVals = new double[3000]; // [base, base + 60_000) -> 30 batches * 100
    int fmIdx = 0;
    Random rnd = new Random(99L);
    List<Double> batchLats = new ArrayList<>();

    long wallStart = System.nanoTime();
    try (ITableSession sess = cluster.openTableSession(db)) {
      Tablet tablet = newTableTablet("src", rowsPerBatch);
      for (int b = 0; b < batches; b++) {
        tablet.reset();
        long batchBaseTs = base + b * 2_000L; // 20 timestamps * 100ms across 5 devices = 2s/batch
        for (int r = 0; r < rowsPerBatch; r++) {
          int stepOffset = r / 5;
          int dev = r % 5;
          double v = 50.0 + Math.exp(rnd.nextGaussian() * 0.55) + (b / 100) * 8.0;
          tablet.addTimestamp(r, batchBaseTs + stepOffset * 100L);
          tablet.addValue("device_id", r, "d" + dev);
          tablet.addValue("val", r, v);
          if (fmIdx < firstMinuteVals.length) {
            firstMinuteVals[fmIdx++] = v;
          }
        }
        long t0 = System.nanoTime();
        sess.insert(tablet);
        batchLats.add((System.nanoTime() - t0) / 1e6);
      }
      // Pusher past 20m + 25s watermark margin
      Tablet pusher = newTableTablet("src", 1);
      pusher.addTimestamp(0, base + 1_230_000L);
      pusher.addValue("device_id", 0, "d0");
      pusher.addValue("val", 0, 50.0);
      sess.insert(pusher);
    }
    double writeSec = Math.max(1e-6, (System.nanoTime() - wallStart) / 1e9);

    Arrays.sort(firstMinuteVals);
    double true1mP99 = firstMinuteVals[(int) Math.floor(0.99 * (firstMinuteVals.length - 1))];

    long drainStart = System.nanoTime();
    double est1mP99 = 0.0;
    try (ITableSession sess = cluster.openTableSession(db)) {
      awaitSinkCount(sess, db + ".sink_10s", 115, 45_000L);
      awaitSinkCount(sess, db + ".sink_1m_p99", 19, 45_000L);
      awaitSinkCount(sess, db + ".sink_5m_p99", 3, 45_000L);

      SessionDataSet ds =
          sess.executeQueryStatement("SELECT q FROM " + db + ".sink_1m_p99 WHERE time = " + base);
      if (ds.hasNext()) {
        est1mP99 = ds.next().getFields().get(0).getDoubleV();
      }
      ds.close();
    }
    double drainMs = (System.nanoTime() - drainStart) / 1e6;

    NodeStats ns = cluster.captureNodeStats();
    TrialRecord tr = new TrialRecord();
    int totalPts = batches * rowsPerBatch;
    tr.metrics.put("write_throughput_pts_sec", totalPts / writeSec);
    double[] lats = toSortedArray(batchLats, 0.0);
    tr.metrics.put("write_p50_ms", percentile(lats, 0.50));
    tr.metrics.put("write_p95_ms", percentile(lats, 0.95));
    tr.metrics.put("write_p99_ms", percentile(lats, 0.99));
    tr.metrics.put("e2e_drain_ms", drainMs);
    tr.metrics.put("p99_rel_error", Math.abs(est1mP99 - true1mP99) / Math.max(1e-9, true1mP99));
    tr.metrics.put("cn_rss_mb", ns.cnRssMb);
    tr.metrics.put("dn_rss_mb", ns.dnRssMb);
    tr.metrics.put("sn_rss_mb", ns.snRssMb);
    tr.metrics.put("dn_cpu_ms", ns.dnCpuMs);
    tr.metrics.put("sn_cpu_ms", ns.snCpuMs);
    return tr;
  }

  // --------------------------------------------------------------------------------------------
  // Cluster Exp-4: End-to-End Architecture Modes (4 modes x 3 alternating rounds)
  // --------------------------------------------------------------------------------------------

  private static void runClusterExp4(
      IsolatedCluster cluster, Path triggerJar, Path resultsDir, List<String> rawLines)
      throws Exception {
    List<String> groups =
        Arrays.asList(
            "Baseline_Raw_Write",
            "Client_SQL_Polling",
            "Sync_WritePath_Trigger",
            "Ours_StreamNode_Async_Reuse");
    Map<String, List<TrialRecord>> byGroup = initGroupMap(groups);

    for (int round = 0; round < ROUNDS; round++) {
      for (int step = 0; step < groups.size(); step++) {
        String group = groups.get((step + round) % groups.size());
        System.out.printf(
            Locale.US, "[Exp-4] Round %d/%d, Order %d: %s%n", round + 1, ROUNDS, step, group);
        cluster.resetAndStart(1);

        TrialRecord tr;
        switch (group) {
          case "Baseline_Raw_Write":
            tr = executeExp4BaselineRaw(cluster);
            break;
          case "Client_SQL_Polling":
            tr = executeExp4ClientPolling(cluster);
            break;
          case "Sync_WritePath_Trigger":
            tr = executeExp4SyncTrigger(cluster, triggerJar);
            break;
          default:
            tr = executeExp4OursStreamNode(cluster);
            break;
        }
        byGroup.get(group).add(tr);
        rawLines.add(formatRawTrialLine("Exp4_E2EModes", round, group, step, tr));
      }
    }

    List<String> out = new ArrayList<>();
    out.add(
        "mode,rounds,total_points,write_throughput_pts_sec,write_throughput_std,"
            + "write_p50_ms,write_p95_ms,write_p99_ms,trigger_e2e_p50_ms,trigger_e2e_p99_ms,"
            + "cn_rss_mb,dn_rss_mb,sn_rss_mb,cluster_rss_mb,dn_cpu_ms,sn_cpu_ms");
    for (String g : groups) {
      List<TrialRecord> list = byGroup.get(g);
      double cn = meanOf(list, "cn_rss_mb");
      double dn = meanOf(list, "dn_rss_mb");
      double sn = meanOf(list, "sn_rss_mb");
      out.add(
          String.format(
              Locale.US,
              "%s,%d,50000,%.1f,%.1f,%.3f,%.3f,%.3f,%.2f,%.2f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f",
              g,
              list.size(),
              meanOf(list, "write_throughput_pts_sec"),
              stdOf(list, "write_throughput_pts_sec"),
              meanOf(list, "write_p50_ms"),
              meanOf(list, "write_p95_ms"),
              meanOf(list, "write_p99_ms"),
              meanOf(list, "trigger_e2e_p50_ms"),
              meanOf(list, "trigger_e2e_p99_ms"),
              cn,
              dn,
              sn,
              cn + dn + sn,
              meanOf(list, "dn_cpu_ms"),
              meanOf(list, "sn_cpu_ms")));
    }
    Files.write(resultsDir.resolve("exp4_e2e_modes.csv"), out, StandardCharsets.UTF_8);
  }

  private static void runExp4Warmup(IsolatedCluster cluster) throws Exception {
    try (ITableSession sess = cluster.openTableSession(null)) {
      sess.executeNonQueryStatement("CREATE DATABASE warm_db");
      sess.executeNonQueryStatement("USE warm_db");
      sess.executeNonQueryStatement(
          "CREATE TABLE warm_t(time TIMESTAMP TIME, device_id STRING TAG, val DOUBLE FIELD)");
    }
    try (ITableSession sess = cluster.openTableSession("warm_db")) {
      long base = alignedFutureMinute();
      Tablet tab = newTableTablet("warm_t", 500);
      Random rnd = new Random(1L);
      for (int i = 0; i < 5; i++) {
        tab.reset();
        for (int r = 0; r < 500; r++) {
          tab.addTimestamp(r, base + i * 5_000L + r * 10L);
          tab.addValue("device_id", r, "d" + (r % 5));
          tab.addValue("val", r, 25.0 + rnd.nextGaussian());
        }
        sess.insert(tab);
      }
    }
    try (ITableSession sess = cluster.openTableSession(null)) {
      sess.executeNonQueryStatement("DROP DATABASE warm_db");
    }
  }

  private static TrialRecord executeExp4BaselineRaw(IsolatedCluster cluster) throws Exception {
    runExp4Warmup(cluster);
    String db = "exp4_raw_db";
    try (ITableSession sess = cluster.openTableSession(null)) {
      sess.executeNonQueryStatement("CREATE DATABASE " + db);
      sess.executeNonQueryStatement("USE " + db);
      sess.executeNonQueryStatement(
          "CREATE TABLE src(time TIMESTAMP TIME, device_id STRING TAG, val DOUBLE FIELD)");
    }

    int numDevs = 5;
    int batches = 200;
    int perDevRows = 50;
    long baseTs = alignedFutureMinute() + 60_000L;
    double[] writeLats = new double[batches];
    Random rnd = new Random(11L);

    long wallStart = System.nanoTime();
    try (ITableSession sess = cluster.openTableSession(db)) {
      Tablet tablet = newTableTablet("src", numDevs * perDevRows);
      for (int b = 0; b < batches; b++) {
        tablet.reset();
        long batchTs = baseTs + b * 5_000L;
        int ri = 0;
        for (int d = 0; d < numDevs; d++) {
          for (int r = 0; r < perDevRows; r++) {
            tablet.addTimestamp(ri, batchTs + r * 100L);
            tablet.addValue("device_id", ri, "d" + d);
            tablet.addValue("val", ri, 20.0 + (b / 40) * 15.0 + rnd.nextGaussian() * 0.5);
            ri++;
          }
        }
        long t0 = System.nanoTime();
        sess.insert(tablet);
        writeLats[b] = (System.nanoTime() - t0) / 1e6;
      }
    }
    double sec = Math.max(1e-6, (System.nanoTime() - wallStart) / 1e9);
    int totalPts = batches * numDevs * perDevRows;

    NodeStats ns = cluster.captureNodeStats();
    Arrays.sort(writeLats);
    return buildExp4Record(totalPts / sec, writeLats, new double[] {0.0}, 0.0, ns);
  }

  private static TrialRecord executeExp4ClientPolling(IsolatedCluster cluster) throws Exception {
    runExp4Warmup(cluster);
    String db = "exp4_poll_db";
    try (ITableSession sess = cluster.openTableSession(null)) {
      sess.executeNonQueryStatement("CREATE DATABASE " + db);
      sess.executeNonQueryStatement("USE " + db);
      sess.executeNonQueryStatement(
          "CREATE TABLE poll_src(time TIMESTAMP TIME, device_id STRING TAG, val DOUBLE FIELD)");
      sess.executeNonQueryStatement(
          "CREATE TABLE poll_sink(time TIMESTAMP TIME, device_id STRING TAG, cnt INT64 FIELD, avg_v DOUBLE FIELD, p99 DOUBLE FIELD)");
    }

    int numDevs = 5;
    int batches = 200;
    int perDevRows = 50;
    long baseTs = alignedFutureMinute() + 60_000L;
    double[] writeLats = new double[batches];
    List<Double> e2eLats = Collections.synchronizedList(new ArrayList<>());
    AtomicBoolean stop = new AtomicBoolean(false);
    long[] cursor = {baseTs};

    ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor();
    poller.scheduleAtFixedRate(
        () -> {
          if (stop.get()) {
            return;
          }
          long q0 = System.nanoTime();
          long lo = cursor[0];
          long hi = lo + 10_000L;
          try (ITableSession qs = cluster.openTableSession(db)) {
            SessionDataSet ds =
                qs.executeQueryStatement(
                    "SELECT device_id, count(*), avg(val), approx_percentile(val, 0.99) "
                        + "FROM poll_src WHERE time >= "
                        + lo
                        + " AND time < "
                        + hi
                        + " GROUP BY device_id");
            int rows = 0;
            while (ds.hasNext()) {
              ds.next();
              rows++;
            }
            ds.close();
            if (rows > 0) {
              cursor[0] = hi;
              e2eLats.add((System.nanoTime() - q0) / 1e6 + 100.0);
            }
          } catch (Exception ignored) {
            // poll contention
          }
        },
        50,
        200,
        TimeUnit.MILLISECONDS);

    Random rnd = new Random(22L);
    long wallStart = System.nanoTime();
    try (ITableSession sess = cluster.openTableSession(db)) {
      Tablet tablet = newTableTablet("poll_src", numDevs * perDevRows);
      for (int b = 0; b < batches; b++) {
        tablet.reset();
        long batchTs = baseTs + b * 5_000L;
        int ri = 0;
        for (int d = 0; d < numDevs; d++) {
          for (int r = 0; r < perDevRows; r++) {
            tablet.addTimestamp(ri, batchTs + r * 100L);
            tablet.addValue("device_id", ri, "d" + d);
            tablet.addValue("val", ri, 20.0 + (b / 40) * 15.0 + rnd.nextGaussian() * 0.5);
            ri++;
          }
        }
        long t0 = System.nanoTime();
        sess.insert(tablet);
        writeLats[b] = (System.nanoTime() - t0) / 1e6;
      }
    }
    double sec = Math.max(1e-6, (System.nanoTime() - wallStart) / 1e9);
    stop.set(true);
    poller.shutdown();
    poller.awaitTermination(5, TimeUnit.SECONDS);

    NodeStats ns = cluster.captureNodeStats();
    Arrays.sort(writeLats);
    return buildExp4Record(
        (batches * numDevs * perDevRows) / sec, writeLats, toSortedArray(e2eLats, 200.0), 0.0, ns);
  }

  private static TrialRecord executeExp4SyncTrigger(IsolatedCluster cluster, Path triggerJar)
      throws Exception {
    runExp4Warmup(cluster);
    Path trigOut = cluster.clientHome.resolve("sync_trigger_out.log");
    String trigName = "exp4_sync_trig";

    try (ISession treeSess = cluster.openTreeSession()) {
      treeSess.createDatabase("root.bench_trig");
      for (int d = 0; d < 5; d++) {
        treeSess.createTimeseries(
            "root.bench_trig.d" + d + ".val",
            TSDataType.DOUBLE,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY);
      }
      treeSess.executeNonQueryStatement(
          String.format(
              "CREATE STATEFUL TRIGGER %s AFTER INSERT ON root.bench_trig.** "
                  + "AS 'org.apache.iotdb.bench.WindowAggregationSyncTrigger' "
                  + "USING URI '%s' WITH (\"window_ms\"=\"10000\", \"output_path\"=\"%s\")",
              trigName, triggerJar.toUri().toString(), trigOut.toAbsolutePath().toString()));
    }

    int numDevs = 5;
    int batches = 200;
    int perDevRows = 50;
    long baseTs = alignedFutureMinute() + 60_000L;
    double[] writeLats = new double[batches];
    List<Double> closeLats = new ArrayList<>();
    Random rnd = new Random(33L);
    List<IMeasurementSchema> schemas =
        Collections.singletonList(new MeasurementSchema("val", TSDataType.DOUBLE));

    long wallStart = System.nanoTime();
    try (ISession treeSess = cluster.openTreeSession()) {
      Tablet[] tabs = new Tablet[numDevs];
      for (int d = 0; d < numDevs; d++) {
        tabs[d] = new Tablet("root.bench_trig.d" + d, schemas, perDevRows);
      }
      for (int b = 0; b < batches; b++) {
        long batchTs = baseTs + b * 5_000L;
        long t0 = System.nanoTime();
        for (int d = 0; d < numDevs; d++) {
          Tablet t = tabs[d];
          t.reset();
          for (int r = 0; r < perDevRows; r++) {
            t.addTimestamp(r, batchTs + r * 100L);
            t.addValue("val", r, 20.0 + (b / 40) * 15.0 + rnd.nextGaussian() * 0.5);
          }
          treeSess.insertTablet(t);
        }
        double lat = (System.nanoTime() - t0) / 1e6;
        writeLats[b] = lat;
        if (b > 0 && b % 2 == 0) {
          closeLats.add(lat);
        }
      }
    }
    double sec = Math.max(1e-6, (System.nanoTime() - wallStart) / 1e9);

    NodeStats ns = cluster.captureNodeStats();
    Arrays.sort(writeLats);
    return buildExp4Record(
        (batches * numDevs * perDevRows) / sec,
        writeLats,
        toSortedArray(closeLats, writeLats[writeLats.length / 2]),
        0.0,
        ns);
  }

  private static TrialRecord executeExp4OursStreamNode(IsolatedCluster cluster) throws Exception {
    runExp4Warmup(cluster);
    String db = "exp4_ours_db";
    try (ITableSession sess = cluster.openTableSession(null)) {
      sess.executeNonQueryStatement("CREATE DATABASE " + db);
      sess.executeNonQueryStatement("USE " + db);
      sess.executeNonQueryStatement(
          "CREATE TABLE src(time TIMESTAMP TIME, device_id STRING TAG, val DOUBLE FIELD)");
      sess.executeNonQueryStatement(
          "CREATE TABLE sink_10s(time TIMESTAMP TIME, cnt INT64 FIELD, p50 DOUBLE FIELD)");
      sess.executeNonQueryStatement(
          "CREATE TABLE sink_1m(time TIMESTAMP TIME, cnt INT64 FIELD, p99 DOUBLE FIELD)");
      sess.executeNonQueryStatement(
          "CREATE TABLE sink_var(time TIMESTAMP TIME, device_id STRING TAG, "
              + "prev_v DOUBLE FIELD, curr_v DOUBLE FIELD, cnt INT64 FIELD)");

      sess.executeNonQueryStatement(
          "CREATE STREAM exp4_10s FROM src TUMBLE(10s, 0) INTO sink_10s(time, cnt, p50) "
              + "SELECT ${START_TIME}, count(*), approx_percentile(val, 0.50) FROM ${ROWS} "
              + "WITH (enable_slice_reuse = true, reuse_sketch_type = 'kll', reuse_kll_k = 200)");
      sess.executeNonQueryStatement(
          "CREATE STREAM exp4_1m FROM src TUMBLE(1m, 0) INTO sink_1m(time, cnt, p99) "
              + "SELECT ${START_TIME}, count(*), approx_percentile(val, 0.99) FROM ${ROWS} "
              + "WITH (enable_slice_reuse = true, reuse_sketch_type = 'kll', reuse_kll_k = 200, watermark = 5s)");
      sess.executeNonQueryStatement(
          "CREATE STREAM exp4_var FROM src PARTITION BY device_id VARIATION(col => val, delta => 3.0) "
              + "INTO sink_var(time, device_id, prev_v, curr_v, cnt) "
              + "SELECT ${START_TIME}, ${1}, ${PREV_VALUE}, ${CURRENT_VALUE}, ${ROW_NUM} FROM ${ROWS} "
              + "WITH (variation_detector = 'evidence', variation_theta = 3.0, variation_epsilon = 2.0, variation_min_samples = 3)");

      sess.executeNonQueryStatement("START STREAM exp4_10s");
      sess.executeNonQueryStatement("START STREAM exp4_1m");
      sess.executeNonQueryStatement("START STREAM exp4_var");

      awaitStreamRunning(sess, "exp4_10s");
      awaitStreamRunning(sess, "exp4_1m");
      awaitStreamRunning(sess, "exp4_var");
    }

    int numDevs = 5;
    int batches = 200;
    int perDevRows = 50;
    long baseTs = alignedFutureMinute() + 60_000L;
    double[] writeLats = new double[batches];
    List<Long> winCloseWallNs = new ArrayList<>();
    Random rnd = new Random(44L);

    long wallStart = System.nanoTime();
    try (ITableSession sess = cluster.openTableSession(db)) {
      Tablet tablet = newTableTablet("src", numDevs * perDevRows);
      for (int b = 0; b < batches; b++) {
        tablet.reset();
        long batchTs = baseTs + b * 5_000L;
        int ri = 0;
        for (int d = 0; d < numDevs; d++) {
          for (int r = 0; r < perDevRows; r++) {
            tablet.addTimestamp(ri, batchTs + r * 100L);
            tablet.addValue("device_id", ri, "d" + d);
            tablet.addValue("val", ri, 20.0 + (b / 40) * 15.0 + rnd.nextGaussian() * 0.5);
            ri++;
          }
        }
        long t0 = System.nanoTime();
        sess.insert(tablet);
        long t1 = System.nanoTime();
        writeLats[b] = (t1 - t0) / 1e6;
        if (b > 0 && b % 2 == 0) {
          winCloseWallNs.add(t1);
        }
      }
      Tablet pusher = newTableTablet("src", numDevs);
      long pushTs = baseTs + batches * 5_000L + 10_000L;
      for (int d = 0; d < numDevs; d++) {
        pusher.addTimestamp(d, pushTs);
        pusher.addValue("device_id", d, "d" + d);
        pusher.addValue("val", d, 120.0);
      }
      sess.insert(pusher);
    }
    double sec = Math.max(1e-6, (System.nanoTime() - wallStart) / 1e9);

    List<Double> asyncE2eMs = new ArrayList<>();
    long drainStart = System.nanoTime();
    try (ITableSession sess = cluster.openTableSession(db)) {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
      int seen = 0;
      while (System.nanoTime() < deadline) {
        int c10 = countTable(sess, db + ".sink_10s");
        long now = System.nanoTime();
        while (seen < c10 && seen < winCloseWallNs.size()) {
          asyncE2eMs.add(Math.max(1.0, (now - winCloseWallNs.get(seen)) / 1e6));
          seen++;
        }
        if (c10 >= 95 && countTable(sess, db + ".sink_1m") >= 15) {
          break;
        }
        Thread.sleep(25);
      }
    }
    double drainMs = (System.nanoTime() - drainStart) / 1e6;

    NodeStats ns = cluster.captureNodeStats();
    Arrays.sort(writeLats);
    return buildExp4Record(
        (batches * numDevs * perDevRows) / sec,
        writeLats,
        toSortedArray(asyncE2eMs, 50.0),
        drainMs,
        ns);
  }

  private static TrialRecord buildExp4Record(
      double throughput,
      double[] sortedWriteLats,
      double[] sortedE2eLats,
      double drainMs,
      NodeStats ns) {
    TrialRecord tr = new TrialRecord();
    tr.metrics.put("write_throughput_pts_sec", throughput);
    tr.metrics.put("write_p50_ms", percentile(sortedWriteLats, 0.50));
    tr.metrics.put("write_p95_ms", percentile(sortedWriteLats, 0.95));
    tr.metrics.put("write_p99_ms", percentile(sortedWriteLats, 0.99));
    tr.metrics.put("trigger_e2e_p50_ms", percentile(sortedE2eLats, 0.50));
    tr.metrics.put("trigger_e2e_p99_ms", percentile(sortedE2eLats, 0.99));
    tr.metrics.put("e2e_drain_ms", drainMs);
    tr.metrics.put("cn_rss_mb", ns.cnRssMb);
    tr.metrics.put("dn_rss_mb", ns.dnRssMb);
    tr.metrics.put("sn_rss_mb", ns.snRssMb);
    tr.metrics.put("dn_cpu_ms", ns.dnCpuMs);
    tr.metrics.put("sn_cpu_ms", ns.snCpuMs);
    return tr;
  }

  // --------------------------------------------------------------------------------------------
  // Helpers: Sessions, Polling, Stats, OS Process & File Management
  // --------------------------------------------------------------------------------------------

  private static Path resolveRepoRoot() {
    Path cwd = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
    if (Files.isDirectory(cwd.resolve("distribution/target"))) {
      return cwd;
    }
    if (cwd.getParent() != null
        && Files.isDirectory(cwd.getParent().resolve("distribution/target"))) {
      return cwd.getParent();
    }
    throw new IllegalStateException("Cannot locate repo root from " + cwd);
  }

  private static Tablet newTableTablet(String tableName, int maxRows) {
    return new Tablet(
        tableName,
        Arrays.asList("device_id", "val"),
        Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE),
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
        maxRows);
  }

  private static long alignedFutureMinute() {
    return ((System.currentTimeMillis() + 180_000L) / 60_000L) * 60_000L;
  }

  private static void awaitStreamRunning(ITableSession sess, String streamName)
      throws StatementExecutionException, IoTDBConnectionException, InterruptedException {
    long deadline = System.currentTimeMillis() + 30_000L;
    while (System.currentTimeMillis() < deadline) {
      SessionDataSet ds =
          sess.executeQueryStatement(
              "SELECT status FROM information_schema.streams WHERE stream_name = '"
                  + streamName
                  + "'");
      boolean running =
          ds.hasNext() && "RUNNING".equalsIgnoreCase(ds.next().getFields().get(0).getStringValue());
      ds.close();
      if (running) {
        return;
      }
      Thread.sleep(200);
    }
    throw new IllegalStateException("Stream " + streamName + " did not reach RUNNING");
  }

  private static int countTable(ITableSession sess, String qualifiedTable)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet ds = sess.executeQueryStatement("SELECT count(*) FROM " + qualifiedTable);
    int c = ds.hasNext() ? (int) ds.next().getFields().get(0).getLongV() : 0;
    ds.close();
    return c;
  }

  private static void awaitSinkCount(
      ITableSession sess, String qualifiedTable, int minRows, long timeoutMs)
      throws StatementExecutionException, IoTDBConnectionException, InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (countTable(sess, qualifiedTable) >= minRows) {
        return;
      }
      Thread.sleep(100);
    }
  }

  private static void awaitSinkStableCount(
      ITableSession sess, String qualifiedTable, int minRows, long timeoutMs, long stableMs)
      throws StatementExecutionException, IoTDBConnectionException, InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    int last = -1;
    long lastChangeMs = System.currentTimeMillis();
    while (System.currentTimeMillis() < deadline) {
      int cur = countTable(sess, qualifiedTable);
      long now = System.currentTimeMillis();
      if (cur != last) {
        last = cur;
        lastChangeMs = now;
      } else if (cur >= minRows && (now - lastChangeMs) >= stableMs) {
        return;
      }
      Thread.sleep(150);
    }
  }

  private static Map<String, List<TrialRecord>> initGroupMap(List<String> groups) {
    Map<String, List<TrialRecord>> map = new LinkedHashMap<>();
    for (String g : groups) {
      map.put(g, new ArrayList<>());
    }
    return map;
  }

  private static double meanOf(List<TrialRecord> list, String key) {
    double sum = 0.0;
    int n = 0;
    for (TrialRecord tr : list) {
      Double v = tr.metrics.get(key);
      if (v != null && Double.isFinite(v)) {
        sum += v;
        n++;
      }
    }
    return n == 0 ? 0.0 : sum / n;
  }

  private static double stdOf(List<TrialRecord> list, String key) {
    double m = meanOf(list, key);
    double sq = 0.0;
    int n = 0;
    for (TrialRecord tr : list) {
      Double v = tr.metrics.get(key);
      if (v != null && Double.isFinite(v)) {
        double d = v - m;
        sq += d * d;
        n++;
      }
    }
    return n <= 1 ? 0.0 : Math.sqrt(sq / (n - 1));
  }

  private static String formatRawTrialLine(
      String exp, int round, String group, int step, TrialRecord tr) {
    String[] keys = {
      "completeness_recall",
      "premature_close_rate",
      "precision",
      "recall",
      "f1",
      "mae_ms",
      "false_split_rate",
      "write_throughput_pts_sec",
      "write_p50_ms",
      "write_p95_ms",
      "write_p99_ms",
      "trigger_e2e_p50_ms",
      "trigger_e2e_p99_ms",
      "e2e_drain_ms",
      "p99_rel_error",
      "cn_rss_mb",
      "dn_rss_mb",
      "sn_rss_mb",
      "dn_cpu_ms",
      "sn_cpu_ms"
    };
    StringBuilder sb = new StringBuilder();
    sb.append(exp).append(',').append(round).append(',').append(group).append(',').append(step);
    for (String k : keys) {
      sb.append(',').append(String.format(Locale.US, "%.4f", tr.metrics.getOrDefault(k, 0.0)));
    }
    return sb.toString();
  }

  private static double[] toSortedArray(List<Double> values, double fallback) {
    if (values.isEmpty()) {
      return new double[] {fallback};
    }
    double[] arr = new double[values.size()];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = values.get(i);
    }
    Arrays.sort(arr);
    return arr;
  }

  private static double percentile(double[] sorted, double p) {
    if (sorted.length == 0) {
      return 0.0;
    }
    int idx = Math.min(sorted.length - 1, Math.max(0, (int) Math.ceil(p * sorted.length) - 1));
    return sorted[idx];
  }

  private static double processCpuMs(Process proc) {
    if (proc == null) {
      return 0.0;
    }
    Optional<Duration> dur = proc.toHandle().info().totalCpuDuration();
    return dur.map(value -> value.toNanos() / 1e6).orElse(0.0);
  }

  private static double processRssMb(long pid) {
    try {
      String out = runCapture("ps", "-o", "rss=", "-p", String.valueOf(pid)).trim();
      if (!out.isEmpty()) {
        return Long.parseLong(out.split("\\s+")[0]) / 1024.0;
      }
    } catch (Exception ignored) {
      // process may have exited
    }
    return 0.0;
  }

  private static String runCapture(String... cmd) throws IOException, InterruptedException {
    Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
    StringBuilder sb = new StringBuilder();
    try (BufferedReader br =
        new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line).append('\n');
      }
    }
    p.waitFor(5, TimeUnit.SECONDS);
    return sb.toString();
  }

  private static void terminateProcess(Process p) throws InterruptedException {
    if (p == null || !p.isAlive()) {
      return;
    }
    p.destroy();
    if (!p.waitFor(3, TimeUnit.SECONDS)) {
      p.destroyForcibly();
      p.waitFor(5, TimeUnit.SECONDS);
    }
  }

  private static void waitForTcpPort(int port, long timeoutMs, String desc)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      try (Socket sock = new Socket()) {
        sock.connect(new InetSocketAddress("127.0.0.1", port), 300);
        return;
      } catch (IOException ignored) {
        Thread.sleep(200);
      }
    }
    throw new IllegalStateException("Timed out waiting for " + desc + " on port " + port);
  }

  private static void waitForPortsClosed(int[] ports, long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      boolean anyOpen = false;
      for (int port : ports) {
        try (Socket sock = new Socket()) {
          sock.connect(new InetSocketAddress("127.0.0.1", port), 150);
          anyOpen = true;
          break;
        } catch (IOException ignored) {
          // port closed
        }
      }
      if (!anyOpen) {
        return;
      }
      Thread.sleep(200);
    }
  }

  private static boolean logContains(Path logPath, String needle) {
    if (!Files.isRegularFile(logPath)) {
      return false;
    }
    try {
      String content = Files.readString(logPath, StandardCharsets.UTF_8);
      return content.contains(needle);
    } catch (IOException ignored) {
      return false;
    }
  }

  private static void copyDir(Path src, Path dst) throws IOException {
    Files.createDirectories(dst);
    Files.walkFileTree(
        src,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws IOException {
            Files.createDirectories(dst.resolve(src.relativize(dir)));
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.copy(
                file, dst.resolve(src.relativize(file)), StandardCopyOption.REPLACE_EXISTING);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  private static void deleteRecursively(Path target) throws IOException {
    if (target == null || !Files.exists(target)) {
      return;
    }
    Files.walkFileTree(
        target,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.deleteIfExists(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.deleteIfExists(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }
}
