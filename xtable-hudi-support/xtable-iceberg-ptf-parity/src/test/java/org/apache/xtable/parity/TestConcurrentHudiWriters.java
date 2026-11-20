/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.xtable.parity;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import org.apache.xtable.timeline.IcebergSnapshotInstants;

/**
 * Two writers run Hudi SQL updates against the same copy-on-write table published to Iceberg, under
 * Hudi's optimistic concurrency control. This is the Hudi counterpart of Iceberg's serializable
 * isolation: a writer whose files another writer changed since it started fails with a write
 * conflict, every other write lands, and every write that lands is published to Iceberg.
 */
class TestConcurrentHudiWriters {

  private static final int ROUNDS = 6;
  private static final Pattern COMPLETED_COMMIT =
      Pattern.compile("^(\\d+)_(\\d+)\\.(commit|replacecommit)$");

  private static SparkSession spark;
  private static File warehouse;

  @BeforeAll
  static void startSpark() throws IOException {
    warehouse = Files.createTempDirectory("ptf-concurrent-writers").toFile();
    spark =
        SparkSession.builder()
            .master("local[4]")
            .config("spark.testing", "true")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.warehouse.dir", new File(warehouse, "spark").getAbsolutePath())
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                    + "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  private static final String OCC_PROPERTIES =
      "'hoodie.write.concurrency.mode' 'optimistic_concurrency_control',"
          + " 'hoodie.write.lock.provider'"
          + " 'org.apache.hudi.client.transaction.lock.InProcessLockProvider',"
          + " 'hoodie.clean.failed.writes.policy' 'LAZY'";

  /** A COW table with ids 1 to 4, all in one file group unless partitioned by dep. */
  private static String[] create(String name, String partitionedBy) {
    String suffix = name + "_" + UUID.randomUUID().toString().substring(0, 8);
    String table = "default.h_" + suffix;
    String path = new File(warehouse, "h_" + suffix).toURI().toString();
    spark.conf().set("hoodie.index.type", "SIMPLE");
    spark.conf().set("hoodie.write.updates.as.deletes.and.inserts", "false");
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, v INT, dep STRING) USING hudi %s LOCATION '%s'"
                + " TBLPROPERTIES ('type' 'cow', 'primaryKey' 'id',"
                + " 'hoodie.table.format' 'ICEBERG',"
                + " 'hoodie.table.format.iceberg.format-version' '3', %s)",
            table,
            partitionedBy == null ? "" : "PARTITIONED BY (" + partitionedBy + ")",
            path,
            OCC_PROPERTIES));
    spark.sql(
        "INSERT INTO " + table + " VALUES (1, 0, 'a'), (2, 0, 'b'), (3, 0, 'a'), (4, 0, 'b')");
    return new String[] {table, path};
  }

  /**
   * Runs ROUNDS rounds in which writer w updates id (w + 1) to a value unique to the round, both
   * writers released together. Returns, per id, the value of the last round its update landed in.
   */
  private static Map<Integer, Integer> race(String table, List<String> unexpectedFailures)
      throws Exception {
    Map<Integer, Integer> expected = new TreeMap<>();
    expected.put(1, 0);
    expected.put(2, 0);
    expected.put(3, 0);
    expected.put(4, 0);
    int conflicts = 0;
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      for (int round = 1; round <= ROUNDS; round++) {
        CyclicBarrier start = new CyclicBarrier(2);
        List<Future<Throwable>> writers = new ArrayList<>();
        for (int w = 0; w < 2; w++) {
          int id = w + 1;
          int value = round * 10 + id;
          writers.add(
              pool.submit(
                  () -> {
                    start.await(1, TimeUnit.MINUTES);
                    try {
                      spark.sql(
                          String.format("UPDATE %s SET v = %d WHERE id = %d", table, value, id));
                      return null;
                    } catch (Throwable t) {
                      return t;
                    }
                  }));
        }
        for (int w = 0; w < 2; w++) {
          Throwable failure = writers.get(w).get(10, TimeUnit.MINUTES);
          if (failure == null) {
            expected.put(w + 1, round * 10 + w + 1);
          } else if (isWriteConflict(failure)) {
            conflicts++;
          } else {
            unexpectedFailures.add("round " + round + " writer " + w + ": " + describe(failure));
          }
        }
      }
    } finally {
      pool.shutdownNow();
    }
    System.out.println("TestConcurrentHudiWriters " + table + " write conflicts: " + conflicts);
    return expected;
  }

  private static boolean isWriteConflict(Throwable failure) {
    for (Throwable t = failure; t != null; t = t.getCause()) {
      if (t.getClass().getSimpleName().equals("HoodieWriteConflictException")) {
        return true;
      }
    }
    return false;
  }

  private static String describe(Throwable failure) {
    StringBuilder chain = new StringBuilder();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      if (chain.length() > 0) {
        chain.append(" <- ");
      }
      chain.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage());
    }
    return chain.toString();
  }

  private static List<String> rows(Dataset<Row> df) {
    Dataset<Row> data =
        df.drop(
                Arrays.stream(df.columns())
                    .filter(HoodieRecord.HOODIE_META_COLUMNS::contains)
                    .toArray(String[]::new))
            .select("id", "v");
    return data.collectAsList().stream()
        .map(row -> row.mkString("|"))
        .sorted()
        .collect(Collectors.toList());
  }

  private static List<String> modelRows(Map<Integer, Integer> expected) {
    return expected.entrySet().stream()
        .map(e -> e.getKey() + "|" + e.getValue())
        .sorted()
        .collect(Collectors.toList());
  }

  /** Requested times of the commits Hudi completed, read from the timeline files themselves. */
  private static TreeSet<String> completedCommitsOnDisk(String path) {
    TreeSet<String> requested = new TreeSet<>();
    File hoodie = new File(URI.create(path).getPath(), ".hoodie");
    for (File dir : new File[] {hoodie, new File(hoodie, "timeline")}) {
      String[] names = dir.list();
      if (names == null) {
        continue;
      }
      for (String name : names) {
        Matcher m = COMPLETED_COMMIT.matcher(name);
        if (m.matches()) {
          requested.add(m.group(1));
        }
      }
    }
    return requested;
  }

  /** Requested times of the commits recorded in the current Iceberg snapshot's ancestry. */
  private static TreeSet<String> commitsInIceberg(String path) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setConf(new HadoopStorageConfiguration(spark.sessionState().newHadoopConf()))
            .setBasePath(path)
            .build();
    Table table = new HadoopTables(spark.sessionState().newHadoopConf()).load(path);
    TreeSet<String> recorded = new TreeSet<>();
    for (Snapshot snapshot : IcebergSnapshotInstants.ancestorsOldestFirst(table)) {
      HoodieInstant instant =
          IcebergSnapshotInstants.recordedInstant(snapshot, metaClient.getInstantGenerator());
      if (instant.getAction().equals("commit") || instant.getAction().equals("replacecommit")) {
        recorded.add(instant.requestedTime());
      }
    }
    return recorded;
  }

  private static void verify(String step, String table, String path, Map<Integer, Integer> model) {
    SoftAssertions softly = new SoftAssertions();
    List<String> iceberg = rows(spark.read().format("iceberg").load(path));
    List<String> hudi = rows(spark.read().format("hudi").load(path));
    softly
        .assertThat(hudi)
        .as(step + ": Hudi rows vs the writes that landed")
        .isEqualTo(modelRows(model));
    softly.assertThat(iceberg).as(step + ": Iceberg rows vs Hudi rows").isEqualTo(hudi);
    softly
        .assertThat(commitsInIceberg(path))
        .as(step + ": commits recorded in Iceberg vs commits Hudi completed")
        .isEqualTo(completedCommitsOnDisk(path));
    softly.assertAll();
  }

  private void raceAndVerify(String name, String partitionedBy) throws Exception {
    String[] created = create(name, partitionedBy);
    String table = created[0];
    String path = created[1];
    try {
      List<String> unexpected = Collections.synchronizedList(new ArrayList<>());
      Map<Integer, Integer> model = race(table, unexpected);
      assertThat(unexpected).as("failures other than a Hudi write conflict").isEmpty();
      verify("after the race", table, path, model);
      // The table stays writable by a single writer, which also rolls back the losing writes.
      spark.sql("UPDATE " + table + " SET v = 99 WHERE id = 3");
      model.put(3, 99);
      verify("after a later single write", table, path, model);
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  /** Both writers rewrite the same file group, so overlapping updates conflict. */
  @Test
  void updatesOfOneFileGroup() throws Exception {
    raceAndVerify("one_fg", null);
  }

  /** Writer 1 updates partition a and writer 2 partition b: no overlap, so both always land. */
  @Test
  void updatesOfDisjointPartitions() throws Exception {
    raceAndVerify("two_parts", "dep");
  }
}
