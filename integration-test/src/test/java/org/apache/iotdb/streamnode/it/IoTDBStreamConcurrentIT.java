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

package org.apache.iotdb.streamnode.it;

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;

/**
 * Reproduces the ConfigNode deadlock between two connections concurrently executing START STREAM
 * and CREATE STREAM: from a certain round on, the statements never return and afterwards even
 * CREATE DATABASE (an unrelated ConfigNode write) hangs until the cluster is restarted.
 *
 * <p>Lock chain: START STREAM holds the StreamInfo read lock for the whole start ({@code
 * StreamManager#startStream}) while issuing consensus writes for the subscription topic ({@code
 * prepareSubscriptionTopic}); CREATE STREAM's consensus write applies under the SimpleConsensus
 * instance lock and then blocks on the StreamInfo write lock ({@code StreamInfo#addTask}), so the
 * START's own topic writes queue behind that apply and never return - the read lock is never
 * released and every later ConfigNode write queues behind the wedged instance lock.
 *
 * <p>Every statement runs under a hard timeout, so the permanent hang turns into a test failure
 * that names the round and the stuck statements instead of blocking the build.
 */
@Category({StreamClusterIT.class})
public class IoTDBStreamConcurrentIT {

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(200, TimeUnit.MILLISECONDS)
          .pollInterval(200, TimeUnit.MILLISECONDS)
          .atMost(20, TimeUnit.SECONDS);

  /**
   * Number of concurrent CREATE/START rounds. The failure typically appears within a few rounds.
   */
  private static final int ROUNDS = 10;

  private static final long STATEMENT_TIMEOUT_MS = 60_000L;
  private static final long PROBE_TIMEOUT_MS = 30_000L;
  private static final long CLEANUP_TIMEOUT_MS = 15_000L;

  private static final String DATABASE = "test";
  private static final String BASE_STREAM = "conc_base_stream";

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1S cluster environment (1 ConfigNode, 1 DataNode, 1 StreamNode).
    StreamEnvFactory.getEnv(1)
        .getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    StreamEnvFactory.getEnv(1).initClusterEnvironment(1, 1, 30);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    StreamEnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void cleanUp() {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      final List<String> streamNames = new ArrayList<>();
      try (ResultSet rs = statement.executeQuery("SHOW STREAMS")) {
        while (rs.next()) {
          streamNames.add(rs.getString("stream_name"));
        }
      } catch (Exception ignored) {
        // no streams registered (possibly a wedged ConfigNode)
      }
      for (final String name : streamNames) {
        executeQuietly(statement, "DROP STREAM " + name);
      }
      for (int round = 1; round <= ROUNDS; round++) {
        executeQuietly(statement, "DROP DATABASE IF EXISTS " + probeDatabaseName(round));
      }
      executeQuietly(statement, "DROP DATABASE IF EXISTS " + DATABASE);
    } catch (Exception ignored) {
      // best-effort cleanup; if the ConfigNode is deadlocked the nodes are stopped forcibly anyway
    }
  }

  /**
   * Fires START STREAM and CREATE STREAM simultaneously from two connections, round after round.
   * After each round an unrelated CREATE DATABASE probe must still return: once the ConfigNode
   * deadlocks, the probe times out and the test fails naming the round.
   */
  @Test
  public void testConcurrentCreateAndStartStreamMustNotDeadlockConfigNode() throws Exception {
    try (Connection setupConnection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Connection startConnection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Connection createConnection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement setup = setupConnection.createStatement();
        Statement startStatement = startConnection.createStatement();
        Statement createStatement = createConnection.createStatement()) {

      setup.execute("CREATE DATABASE " + DATABASE);
      setup.execute("USE " + DATABASE);
      setup.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      setup.execute(
          "CREATE TABLE sink (time TIMESTAMP TIME, device_id STRING TAG, max_value FLOAT FIELD)");
      // All three connections need the session default database for stream DDL (error 701).
      startStatement.execute("USE " + DATABASE);
      createStatement.execute("USE " + DATABASE);
      createStream(setup, BASE_STREAM);
      AWAIT.until(() -> streamExists(setup, BASE_STREAM));
      // START only walks the topic-create path when the task is not RUNNING; reset after rounds.
      setup.execute("STOP STREAM " + BASE_STREAM);

      final ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        for (int round = 1; round <= ROUNDS; round++) {
          final String createStreamName = "conc_round_" + round;
          final CyclicBarrier barrier = new CyclicBarrier(2);
          final long roundDeadline = System.currentTimeMillis() + STATEMENT_TIMEOUT_MS;
          final Future<?> startFuture =
              executor.submit(
                  () -> {
                    barrier.await(10, TimeUnit.SECONDS);
                    startStatement.execute("START STREAM " + BASE_STREAM);
                    return null;
                  });
          final Future<?> createFuture =
              executor.submit(
                  () -> {
                    barrier.await(10, TimeUnit.SECONDS);
                    createStream(createStatement, createStreamName);
                    return null;
                  });

          try {
            startFuture.get(remaining(roundDeadline), TimeUnit.MILLISECONDS);
            createFuture.get(remaining(roundDeadline), TimeUnit.MILLISECONDS);
          } catch (final TimeoutException e) {
            startFuture.cancel(true);
            createFuture.cancel(true);
            fail(
                "ConfigNode deadlocked at round "
                    + round
                    + ": 'START STREAM "
                    + BASE_STREAM
                    + "' and 'CREATE STREAM "
                    + createStreamName
                    + "' were issued concurrently and at least one did not return within "
                    + STATEMENT_TIMEOUT_MS
                    + " ms");
          }

          // Liveness probe: an unrelated ConfigNode write must still complete.
          // The reported symptom is that after the wedged round even CREATE DATABASE never
          // returns until the cluster is restarted.
          assertConfigNodeAlive(setup, round);

          // Reset so the next round's START takes the full topic-create path again.
          executeWithDeadline(
              startStatement, "STOP STREAM " + BASE_STREAM, roundDeadline, "round " + round);

          executeWithDeadline(
              createStatement, "DROP STREAM " + createStreamName, roundDeadline, "round " + round);
        }
      } finally {
        executor.shutdownNow();
      }
    }
  }

  /**
   * Fires START STREAM, DROP STREAM, and CREATE STREAM simultaneously. START/DROP target the same
   * stopped stream so START owns the per-task monitor while DROP tries to mark the same task as
   * being dropped; CREATE provides another consensus write that can expose lock-order cycles.
   */
  @Test
  public void testConcurrentStartDropAndCreateStreamMustNotDeadlockConfigNode() throws Exception {
    try (Connection setupConnection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Connection startConnection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Connection dropConnection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Connection createConnection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement setup = setupConnection.createStatement();
        Statement startStatement = startConnection.createStatement();
        Statement dropStatement = dropConnection.createStatement();
        Statement createStatement = createConnection.createStatement()) {

      setup.execute("CREATE DATABASE " + DATABASE);
      setup.execute("USE " + DATABASE);
      setup.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      setup.execute(
          "CREATE TABLE sink (time TIMESTAMP TIME, device_id STRING TAG, max_value FLOAT FIELD)");
      startStatement.execute("USE " + DATABASE);
      dropStatement.execute("USE " + DATABASE);
      createStatement.execute("USE " + DATABASE);

      final ExecutorService executor = Executors.newFixedThreadPool(3);
      try {
        for (int round = 1; round <= ROUNDS; round++) {
          final String baseStreamName = "conc_drop_base_" + round;
          final String createStreamName = "conc_drop_round_" + round;
          createStream(setup, baseStreamName);
          AWAIT.until(() -> streamExists(setup, baseStreamName));
          setup.execute("STOP STREAM " + baseStreamName);

          final CyclicBarrier barrier = new CyclicBarrier(3);
          final long roundDeadline = System.currentTimeMillis() + STATEMENT_TIMEOUT_MS;
          final Future<?> startFuture =
              executor.submit(
                  () -> {
                    barrier.await(10, TimeUnit.SECONDS);
                    startStatement.execute("START STREAM " + baseStreamName);
                    return null;
                  });
          final Future<?> dropFuture =
              executor.submit(
                  () -> {
                    barrier.await(10, TimeUnit.SECONDS);
                    dropStatement.execute("DROP STREAM " + baseStreamName);
                    return null;
                  });
          final Future<?> createFuture =
              executor.submit(
                  () -> {
                    barrier.await(10, TimeUnit.SECONDS);
                    createStream(createStatement, createStreamName);
                    return null;
                  });

          waitForConcurrentStatement(startFuture, roundDeadline, "START STREAM " + baseStreamName);
          waitForConcurrentStatement(dropFuture, roundDeadline, "DROP STREAM " + baseStreamName);
          waitForConcurrentStatement(
              createFuture, roundDeadline, "CREATE STREAM " + createStreamName);

          assertConfigNodeAlive(setup, round, "START/DROP/CREATE STREAM");

          executeQuietly(setup, "DROP STREAM " + baseStreamName);
          executeQuietly(setup, "DROP STREAM " + createStreamName);
        }
      } finally {
        executor.shutdownNow();
      }
    }
  }

  /** Fails when an unrelated ConfigNode write does not return in time. */
  private void assertConfigNodeAlive(final Statement statement, final int round) {
    assertConfigNodeAlive(statement, round, "START/CREATE STREAM");
  }

  /** Fails when an unrelated ConfigNode write does not return in time. */
  private void assertConfigNodeAlive(
      final Statement statement, final int round, final String concurrentOperations) {
    final String probeSql = "CREATE DATABASE " + probeDatabaseName(round);
    final long deadline = System.currentTimeMillis() + PROBE_TIMEOUT_MS;
    try {
      executeWithDeadline(statement, probeSql, deadline, "liveness probe of round " + round);
    } catch (final AssertionError e) {
      fail(
          "ConfigNode wedged by the concurrent "
              + concurrentOperations
              + " of round "
              + round
              + ": even '"
              + probeSql
              + "' (an unrelated write) never returned. This is the reported deadlock: "
              + e.getMessage());
    }
  }

  private static void waitForConcurrentStatement(
      final Future<?> future, final long deadlineMillis, final String sql) {
    try {
      future.get(remaining(deadlineMillis), TimeUnit.MILLISECONDS);
    } catch (final TimeoutException e) {
      future.cancel(true);
      fail("'" + sql + "' did not return within " + STATEMENT_TIMEOUT_MS + " ms");
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("'" + sql + "' was interrupted");
    } catch (final java.util.concurrent.ExecutionException ignored) {
      // START and DROP operate on the same stream concurrently. Either statement may legitimately
      // fail depending on which operation wins; the test only asserts that none of them hang.
    }
  }

  private static void createStream(final Statement statement, final String streamName)
      throws SQLException {
    statement.execute(
        String.format(
            "CREATE STREAM %s FROM %s.src_t PARTITION BY device_id TUMBLE(3600000ms) "
                + "INTO %s.sink SELECT ${START_TIME}, ${1}, max(value) FROM ${ROWS}",
            streamName, DATABASE, DATABASE));
  }

  private static boolean streamExists(final Statement statement, final String streamName) {
    try (ResultSet rs = statement.executeQuery("SHOW STREAMS")) {
      while (rs.next()) {
        if (streamName.equals(rs.getString("stream_name"))) {
          return true;
        }
      }
    } catch (final Exception ignored) {
      // fall through to false
    }
    return false;
  }

  private static long remaining(final long deadlineMillis) {
    return Math.max(1L, deadlineMillis - System.currentTimeMillis());
  }

  private static String probeDatabaseName(final int round) {
    return "conc_probe_" + round;
  }

  /** Executes {@code sql} and fails naming {@code label} when it does not return in time. */
  private static void executeWithDeadline(
      final Statement statement, final String sql, final long deadlineMillis, final String label) {
    final long timeoutMs = remaining(deadlineMillis);
    final ExecutorService singleExecutor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> future = singleExecutor.submit(() -> statement.execute(sql));
      try {
        future.get(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (final TimeoutException e) {
        future.cancel(true);
        fail(
            label
                + ": '"
                + sql
                + "' did not return within "
                + timeoutMs
                + " ms (ConfigNode deadlocked?)");
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        fail(label + ": '" + sql + "' was interrupted");
      } catch (final java.util.concurrent.ExecutionException e) {
        fail(label + ": '" + sql + "' failed: " + e.getCause());
      }
    } finally {
      singleExecutor.shutdownNow();
    }
  }

  /** Best-effort statement execution for cleanup; every failure is ignored. */
  private static void executeQuietly(final Statement statement, final String sql) {
    final ExecutorService singleExecutor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> future = singleExecutor.submit(() -> statement.execute(sql));
      future.get(CLEANUP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (final Exception ignored) {
      // cleanup is best-effort
    } finally {
      singleExecutor.shutdownNow();
    }
  }
}
