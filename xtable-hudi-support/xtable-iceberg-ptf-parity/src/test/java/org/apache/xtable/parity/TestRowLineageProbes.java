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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Probes Iceberg V3 row lineage (_row_id, _last_updated_sequence_number) of a Hudi table published
 * to Iceberg against a native Iceberg table given the equivalent operations, and against the V3
 * spec rules: an unchanged row moved to another file keeps both values; a modified row keeps its
 * _row_id and takes the commit's sequence number; a new row gets an id not used before and the
 * commit's sequence number. Each step prints a LINEAGE line with both sides for the report.
 */
class TestRowLineageProbes {

  private static SparkSession spark;
  private static File warehouse;
  private final List<String> created = new ArrayList<>();

  private static final class Pair {
    final String iceberg;
    final String hudi;
    final String hudiPath;
    final String nativePath;

    Pair(String iceberg, String hudi, String hudiPath, String nativePath) {
      this.iceberg = iceberg;
      this.hudi = hudi;
      this.hudiPath = hudiPath;
      this.nativePath = nativePath;
    }
  }

  /** One side's state after a step: rows keyed by id, plus snapshot facts. */
  private static final class State {
    final Map<Integer, String> data = new LinkedHashMap<>();
    final Map<Integer, Long> rowId = new LinkedHashMap<>();
    final Map<Integer, Long> seq = new LinkedHashMap<>();
    long snapshotSeq;
    Long firstRowId;
    Long nextRowId;
    String operation;
    int dataFiles;
    int deleteFiles;

    String describe() {
      List<String> rows = new ArrayList<>();
      for (Integer id : data.keySet()) {
        rows.add(id + ":" + data.get(id) + ":" + rowId.get(id) + ":" + seq.get(id));
      }
      return String.format(
          "op=%s seq=%d firstRowId=%s nextRowId=%s files=%d deletes=%d rows=%s",
          operation, snapshotSeq, firstRowId, nextRowId, dataFiles, deleteFiles, rows);
    }

    String rowsOnly() {
      return data.keySet().stream()
          .map(id -> id + ":" + data.get(id) + ":" + rowId.get(id) + ":" + seq.get(id))
          .collect(Collectors.joining(","));
    }
  }

  @BeforeAll
  static void startSpark() throws IOException {
    warehouse = Files.createTempDirectory("ptf-lineage-probes").toFile();
    spark =
        SparkSession.builder()
            .master("local[2]")
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
            .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ice.type", "hadoop")
            .config("spark.sql.catalog.ice.warehouse", new File(warehouse, "ice").getAbsolutePath())
            .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db");
  }

  @AfterEach
  void dropTables() {
    for (String table : created) {
      spark.sql("DROP TABLE IF EXISTS " + table);
    }
    created.clear();
    spark.conf().unset("hoodie.parquet.small.file.limit");
  }

  private Pair create(String name, String type) {
    boolean mor = type.equals("mor");
    String suffix = name + "_" + UUID.randomUUID().toString().substring(0, 8);
    String iceberg = "ice.db.i_" + suffix;
    String hudi = "default.h_" + suffix;
    String hudiPath = new File(warehouse, "h_" + suffix).toURI().toString();
    String mode = mor ? "merge-on-read" : "copy-on-write";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, data STRING) USING iceberg TBLPROPERTIES ('format-version' '3',"
                + " 'write.delete.mode' '%s', 'write.update.mode' '%s', 'write.merge.mode' '%s')",
            iceberg, mode, mode, mode));
    spark.conf().set("hoodie.write.updates.as.deletes.and.inserts", String.valueOf(mor));
    spark.conf().set("hoodie.index.type", "SIMPLE");
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, data STRING) USING hudi LOCATION '%s' TBLPROPERTIES ('type' '%s',"
                + " 'primaryKey' 'id', 'hoodie.table.format' 'ICEBERG',"
                + " 'hoodie.table.format.iceberg.format-version' '3'%s)",
            hudi,
            hudiPath,
            type,
            mor
                ? ", 'hoodie.write.updates.as.deletes.and.inserts' 'true', 'hoodie.index.type' 'SIMPLE'"
                : ""));
    created.add(iceberg);
    created.add(hudi);
    String nativePath = new File(warehouse, "ice/db/i_" + suffix).getAbsolutePath();
    return new Pair(iceberg, hudi, hudiPath, nativePath);
  }

  private static void both(Pair pair, String template) {
    spark.sql(template.replace("{t}", pair.iceberg));
    spark.sql(template.replace("{t}", pair.hudi));
  }

  private static State state(String path) {
    Table table = new HadoopTables(spark.sessionState().newHadoopConf()).load(path);
    table.refresh();
    State state = new State();
    for (Row row :
        spark
            .read()
            .format("iceberg")
            .load(path)
            .select("id", "data", "_row_id", "_last_updated_sequence_number")
            .orderBy("id")
            .collectAsList()) {
      int id = row.getInt(0);
      state.data.put(id, row.getString(1));
      state.rowId.put(id, row.isNullAt(2) ? null : row.getLong(2));
      state.seq.put(id, row.isNullAt(3) ? null : row.getLong(3));
    }
    Snapshot snapshot = table.currentSnapshot();
    state.snapshotSeq = snapshot.sequenceNumber();
    state.firstRowId = snapshot.firstRowId();
    state.operation = snapshot.operation();
    state.nextRowId = ((HasTableOperations) table).operations().current().nextRowId();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        state.dataFiles++;
        state.deleteFiles += task.deletes().size();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return state;
  }

  /** Records both sides of a step, compares them, and checks each against the spec rules. */
  private static final class Probe {
    final String name;
    final Pair pair;
    final SoftAssertions softly;
    State nativePrev;
    State publishedPrev;
    final Set<Long> nativeIssued = new HashSet<>();
    final Set<Long> publishedIssued = new HashSet<>();

    Probe(String name, Pair pair, SoftAssertions softly) {
      this.name = name;
      this.pair = pair;
      this.softly = softly;
    }

    /** spec=true checks the moved/modified/new row rules against the previous step. */
    void step(String step, boolean spec) {
      State n = state(pair.nativePath);
      State p = state(pair.hudiPath);
      System.out.println("LINEAGE " + name + " " + step + " native    " + n.describe());
      System.out.println("LINEAGE " + name + " " + step + " published " + p.describe());
      softly
          .assertThat(p.rowsOnly())
          .as(name + " " + step + ": published id:data:_row_id:_last_updated_seq vs native")
          .isEqualTo(n.rowsOnly());
      if (spec) {
        checkSpec("native", step, nativePrev, n, nativeIssued);
        checkSpec("published", step, publishedPrev, p, publishedIssued);
      }
      checkUnique("native", step, n);
      checkUnique("published", step, p);
      nativeIssued.addAll(n.rowId.values());
      publishedIssued.addAll(p.rowId.values());
      nativePrev = n;
      publishedPrev = p;
    }

    void checkUnique(String side, String step, State s) {
      List<Long> ids = new ArrayList<>(s.rowId.values());
      softly
          .assertThat(ids)
          .as(name + " " + step + " " + side + ": _row_id non-null and unique")
          .doesNotContainNull()
          .doesNotHaveDuplicates();
      softly
          .assertThat(s.seq.values())
          .as(name + " " + step + " " + side + ": _last_updated_sequence_number non-null")
          .doesNotContainNull();
    }

    void checkSpec(String side, String step, State prev, State cur, Set<Long> issued) {
      String tag = name + " " + step + " " + side + " spec: ";
      for (Integer id : cur.data.keySet()) {
        Long rid = cur.rowId.get(id);
        Long seq = cur.seq.get(id);
        if (prev != null && prev.data.containsKey(id)) {
          softly
              .assertThat(rid)
              .as(tag + "row " + id + " keeps its _row_id")
              .isEqualTo(prev.rowId.get(id));
          boolean modified = !String.valueOf(prev.data.get(id)).equals(cur.data.get(id));
          softly
              .assertThat(seq)
              .as(
                  tag
                      + "row "
                      + id
                      + (modified
                          ? " modified: takes the commit sequence number"
                          : " unchanged: keeps its sequence number"))
              .isEqualTo(modified ? cur.snapshotSeq : prev.seq.get(id));
        } else {
          softly
              .assertThat(issued)
              .as(tag + "new row " + id + " gets an id never issued before")
              .doesNotContain(rid);
          softly
              .assertThat(seq)
              .as(tag + "new row " + id + " takes the commit sequence number")
              .isEqualTo(cur.snapshotSeq);
        }
      }
    }
  }

  private static String rootMessage(Throwable t) {
    Throwable c = t;
    while (c.getCause() != null && c.getCause() != c) {
      c = c.getCause();
    }
    String frames =
        Arrays.stream(c.getStackTrace())
            .filter(
                f ->
                    f.getClassName().startsWith("org.apache.xtable.")
                            && !f.getClassName().startsWith("org.apache.xtable.parity")
                        || f.getClassName().startsWith("org.apache.hudi.")
                        || f.getClassName().startsWith("org.apache.iceberg."))
            .limit(6)
            .map(f -> f.getClassName() + "." + f.getMethodName() + ":" + f.getLineNumber())
            .collect(Collectors.joining(" < "));
    return c.getClass().getSimpleName() + ": " + c.getMessage() + " @ " + frames;
  }

  /** (a) An insert, then inserts Hudi packs into the same small file group. */
  @Test
  void smallFileInsertsKeepLineage() {
    Pair pair = create("pack", "cow");
    SoftAssertions softly = new SoftAssertions();
    Probe probe = new Probe("a-pack", pair, softly);
    try {
      both(pair, "INSERT INTO {t} VALUES (1, 'a'), (2, 'b')");
      probe.step("1-insert", true);
      both(pair, "INSERT INTO {t} VALUES (3, 'c')");
      probe.step("2-insert-packed", true);
      softly
          .assertThat(probe.publishedPrev.dataFiles)
          .as("a-pack: Hudi packed the second insert into the first file group")
          .isEqualTo(1);
      both(pair, "INSERT INTO {t} VALUES (4, 'd'), (5, 'e')");
      probe.step("3-insert-packed", true);
    } catch (Exception e) {
      softly.fail("a-pack failed: " + rootMessage(e));
    }
    softly.assertAll();
  }

  /** (b) A merge-on-read update and delete with deletion vectors, then compaction. */
  @Test
  void mergeOnReadUpdateThenCompactionKeepLineage() {
    Pair pair = create("mor", "mor");
    SoftAssertions softly = new SoftAssertions();
    Probe probe = new Probe("b-mor", pair, softly);
    try {
      both(pair, "INSERT INTO {t} VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");
      probe.step("1-insert", true);
      both(pair, "UPDATE {t} SET data = 'x' WHERE id = 1");
      probe.step("2-update", true);
      softly
          .assertThat(probe.publishedPrev.deleteFiles)
          .as("b-mor: the published update wrote a deletion vector")
          .isGreaterThan(0);
      both(pair, "DELETE FROM {t} WHERE id = 2");
      probe.step("3-delete", true);
      spark.sql(
          String.format(
              "CALL ice.system.rewrite_data_files(table => '%s',"
                  + " options => map('rewrite-all', 'true'))",
              pair.iceberg.substring("ice.".length())));
      spark.sql(
          String.format(
              "CALL run_compaction(op => 'run', table => '%s',"
                  + " options => 'hoodie.compact.inline.max.delta.commits=1')",
              pair.hudi));
      probe.step("4-compaction", true);
      softly
          .assertThat(probe.publishedPrev.deleteFiles)
          .as("b-mor: compaction applied the deletion vectors")
          .isEqualTo(0);
    } catch (Exception e) {
      softly.fail("b-mor failed: " + rootMessage(e));
    }
    softly.assertAll();
  }

  /** (c) Copy-on-write clustering of three file groups, against Iceberg's rewrite_data_files. */
  @Test
  void clusteringKeepsLineage() {
    Pair pair = create("cluster", "cow");
    SoftAssertions softly = new SoftAssertions();
    Probe probe = new Probe("c-cluster", pair, softly);
    try {
      spark.conf().set("hoodie.parquet.small.file.limit", "0");
      both(pair, "INSERT INTO {t} VALUES (1, 'a'), (2, 'b')");
      both(pair, "INSERT INTO {t} VALUES (3, 'c')");
      both(pair, "INSERT INTO {t} VALUES (4, 'd')");
      probe.step("1-three-inserts", false);
      both(pair, "UPDATE {t} SET data = 'x' WHERE id = 3");
      probe.step("2-update", true);
      spark.conf().unset("hoodie.parquet.small.file.limit");
      int before = probe.publishedPrev.dataFiles;
      spark.sql(
          String.format(
              "CALL ice.system.rewrite_data_files(table => '%s',"
                  + " options => map('rewrite-all', 'true'))",
              pair.iceberg.substring("ice.".length())));
      spark.sql(String.format("CALL run_clustering(table => '%s')", pair.hudi));
      probe.step("3-clustering", true);
      softly
          .assertThat(probe.publishedPrev.operation)
          .as("c-cluster: clustering published (files before " + before + ")")
          .isEqualTo(probe.nativePrev.operation);
      softly
          .assertThat(probe.publishedPrev.dataFiles)
          .as("c-cluster: clustering reduced the file count from " + before)
          .isLessThan(before);
    } catch (Exception e) {
      softly.fail("c-cluster failed: " + rootMessage(e));
    }
    softly.assertAll();
  }

  /** (d) Rollback of an update: Hudi rollback_to_instant against Iceberg rollback_to_snapshot. */
  @Test
  void rollbackOfAnUpdate() {
    Pair pair = create("rollback", "cow");
    SoftAssertions softly = new SoftAssertions();
    Probe probe = new Probe("d-rollback", pair, softly);
    try {
      both(pair, "INSERT INTO {t} VALUES (1, 'a'), (2, 'b')");
      probe.step("1-insert", true);
      State afterInsertNative = probe.nativePrev;
      State afterInsertPublished = probe.publishedPrev;
      long nativeSnapshot =
          new HadoopTables(spark.sessionState().newHadoopConf())
              .load(pair.nativePath)
              .currentSnapshot()
              .snapshotId();
      both(pair, "UPDATE {t} SET data = 'x' WHERE id = 1");
      probe.step("2-update", true);
      String latest =
          spark
              .sql(String.format("CALL show_commits(table => '%s', limit => 10)", pair.hudi))
              .collectAsList()
              .stream()
              .map(r -> r.getString(0))
              .max(String::compareTo)
              .get();
      spark.sql(
          String.format(
              "CALL ice.system.rollback_to_snapshot('%s', %d)",
              pair.iceberg.substring("ice.".length()), nativeSnapshot));
      spark.sql(
          String.format(
              "CALL rollback_to_instant(table => '%s', instant_time => '%s')", pair.hudi, latest));
      Set<Long> issuedBeforeRollback = new HashSet<>(probe.publishedIssued);
      probe.step("3-rollback", false);
      softly
          .assertThat(probe.nativePrev.rowsOnly())
          .as("d-rollback native: rows and lineage as after the insert")
          .isEqualTo(afterInsertNative.rowsOnly());
      softly
          .assertThat(probe.publishedPrev.rowsOnly())
          .as("d-rollback published: rows and lineage as after the insert")
          .isEqualTo(afterInsertPublished.rowsOnly());
      for (Long id : probe.publishedPrev.rowId.values()) {
        softly
            .assertThat(
                afterInsertPublished.rowId.containsValue(id) || !issuedBeforeRollback.contains(id))
            .as("d-rollback published: restored id " + id + " is either the original or fresh")
            .isTrue();
      }
      both(pair, "INSERT INTO {t} VALUES (3, 'c')");
      probe.step("4-insert-after-rollback", true);
      both(pair, "UPDATE {t} SET data = 'y' WHERE id = 2");
      probe.step("5-update-after-rollback", true);
    } catch (Exception e) {
      softly.fail("d-rollback failed: " + rootMessage(e));
    }
    softly.assertAll();
  }
}
