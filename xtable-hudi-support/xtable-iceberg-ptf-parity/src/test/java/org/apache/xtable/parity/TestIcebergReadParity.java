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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.common.model.HoodieRecord;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.types.Types;

/**
 * Runs the same SQL against a native Iceberg table and a Hudi table published to Iceberg by the
 * pluggable table format, and compares what an Iceberg reader sees of the two: rows, schema,
 * snapshot history, metadata tables and the effect of Iceberg's own maintenance actions.
 */
class TestIcebergReadParity {

  private static SparkSession spark;
  private static File warehouse;

  private final List<String> created = new ArrayList<>();

  /** A native Iceberg table and a Hudi table published to Iceberg, created from the same DDL. */
  private static final class Pair {
    final String iceberg;
    final String hudi;
    final String hudiPath;

    Pair(String iceberg, String hudi, String hudiPath) {
      this.iceberg = iceberg;
      this.hudi = hudi;
      this.hudiPath = hudiPath;
    }
  }

  @BeforeAll
  static void startSpark() throws IOException {
    warehouse = Files.createTempDirectory("ptf-read-parity").toFile();
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
            .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.default_iceberg.type", "hadoop")
            .config(
                "spark.sql.catalog.default_iceberg.warehouse",
                new File(warehouse, "default_iceberg").getAbsolutePath())
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
  }

  private Pair create(String name, String columns, String partitionedBy, String type) {
    boolean mor = type.equals("mor");
    String suffix = name + "_" + UUID.randomUUID().toString().substring(0, 8);
    String iceberg = "ice.db.i_" + suffix;
    String hudi = "default.h_" + suffix;
    String hudiPath = new File(warehouse, "h_" + suffix).toURI().toString();
    String partitioning = partitionedBy == null ? "" : "PARTITIONED BY (" + partitionedBy + ")";
    String mode = mor ? "merge-on-read" : "copy-on-write";
    spark.sql(
        String.format(
            "CREATE TABLE %s (%s) USING iceberg %s TBLPROPERTIES ('format-version' '3',"
                + " 'write.delete.mode' '%s', 'write.update.mode' '%s', 'write.merge.mode' '%s')",
            iceberg, columns, partitioning, mode, mode, mode));
    spark.conf().set("hoodie.write.updates.as.deletes.and.inserts", String.valueOf(mor));
    spark.conf().set("hoodie.index.type", "SIMPLE");
    spark.sql(
        String.format(
            "CREATE TABLE %s (%s) USING hudi %s LOCATION '%s' TBLPROPERTIES ('type' '%s',"
                + " 'hoodie.table.format' 'ICEBERG',"
                + " 'hoodie.table.format.iceberg.format-version' '3'%s)",
            hudi,
            columns,
            partitioning,
            hudiPath,
            type,
            mor
                ? ", 'hoodie.write.updates.as.deletes.and.inserts' 'true', 'hoodie.index.type' 'SIMPLE'"
                : ""));
    created.add(iceberg);
    created.add(hudi);
    return new Pair(iceberg, hudi, hudiPath);
  }

  /** Runs a statement, with {t} standing for the table, against both tables. */
  private static void both(Pair pair, String template) {
    spark.sql(template.replace("{t}", pair.iceberg));
    spark.sql(template.replace("{t}", pair.hudi));
  }

  private static Dataset<Row> icebergReadOfHudi(Pair pair) {
    return spark.read().format("iceberg").load(pair.hudiPath);
  }

  private static Dataset<Row> withoutHudiMetaColumns(Dataset<Row> df) {
    return df.drop(
        Arrays.stream(df.columns())
            .filter(HoodieRecord.HOODIE_META_COLUMNS::contains)
            .toArray(String[]::new));
  }

  /** Rows of a query over {t}, read through Iceberg, for the native and the published table. */
  private static List<List<String>> rowsOfBoth(Pair pair, String query) {
    List<String> nativeRows = rows(spark.sql(query.replace("{t}", pair.iceberg)));
    withoutHudiMetaColumns(icebergReadOfHudi(pair)).createOrReplaceTempView("ptf_read");
    List<String> publishedRows = rows(spark.sql(query.replace("{t}", "ptf_read")));
    return Arrays.asList(nativeRows, publishedRows);
  }

  private static List<String> rows(Dataset<Row> df) {
    for (org.apache.spark.sql.types.StructField field : df.schema().fields()) {
      if (field.dataType() instanceof org.apache.spark.sql.types.BinaryType) {
        df = df.withColumn(field.name(), org.apache.spark.sql.functions.hex(df.col(field.name())));
      }
    }
    return df.collectAsList().stream()
        .map(row -> row.mkString("|"))
        .sorted()
        .collect(Collectors.toList());
  }

  private static void assertSameRows(SoftAssertions softly, Pair pair, String step, String query) {
    List<List<String>> rows = rowsOfBoth(pair, query);
    softly
        .assertThat(rows.get(1))
        .as(step + ": published rows vs native rows")
        .isEqualTo(rows.get(0));
  }

  private static Table nativeTable(Pair pair) {
    String name = pair.iceberg.substring("ice.db.".length());
    return new HadoopTables(spark.sessionState().newHadoopConf())
        .load(new File(warehouse, "ice/db/" + name).getAbsolutePath());
  }

  private static Table publishedTable(Pair pair) {
    return new HadoopTables(spark.sessionState().newHadoopConf()).load(pair.hudiPath);
  }

  private static List<Snapshot> history(Table table) {
    table.refresh();
    List<Snapshot> snapshots = new ArrayList<>();
    for (Snapshot snapshot = table.currentSnapshot();
        snapshot != null;
        snapshot = snapshot.parentId() == null ? null : table.snapshot(snapshot.parentId())) {
      snapshots.add(snapshot);
    }
    Collections.reverse(snapshots);
    return snapshots;
  }

  @Test
  void hudiMetaColumnsAreNotPartOfTheIcebergSchema() {
    Pair pair = create("meta", "id INT, data STRING", null, "cow");
    both(pair, "INSERT INTO {t} VALUES (1, 'a')");
    assertThat(icebergReadOfHudi(pair).columns())
        .as("columns an Iceberg reader sees")
        .containsExactly(spark.table(pair.iceberg).columns());
  }

  @ParameterizedTest
  @ValueSource(strings = {"cow", "mor"})
  void columnOrderWithAPartitionColumnInTheMiddle(String type) {
    Pair pair = create("order", "id INT, dep STRING, data STRING", "dep", type);
    both(pair, "INSERT INTO {t} VALUES (1, 'hr', 'a')");
    assertThat(withoutHudiMetaColumns(icebergReadOfHudi(pair)).columns())
        .containsExactly(spark.table(pair.iceberg).columns());
  }

  @ParameterizedTest
  @ValueSource(strings = {"cow", "mor"})
  void everySparkTypeRoundTrips(String type) {
    Pair pair =
        create(
            "types",
            "id INT, b BOOLEAN, l BIGINT, f FLOAT, d DOUBLE, dec DECIMAL(10, 2),"
                + " wide DECIMAL(38, 10), s STRING, bin BINARY, dt DATE, ts TIMESTAMP,"
                + " ntz TIMESTAMP_NTZ, arr ARRAY<INT>, m MAP<STRING, INT>,"
                + " st STRUCT<a: INT, b: STRING>, p STRING",
            "p",
            type);
    both(
        pair,
        "INSERT INTO {t} VALUES"
            + " (1, true, 10000000000, 1.5, 2.25, 12345678.91, 1234567890123456789012345678.1234567891,"
            + " 'héllo', X'CAFE', DATE '2024-02-29', TIMESTAMP '2024-02-29 12:34:56.123456',"
            + " TIMESTAMP_NTZ '2024-02-29 12:34:56.123456', array(1, null, 3), map('k', 1),"
            + " named_struct('a', 1, 'b', 'x'), 'a b/c'),"
            + " (2, null, null, null, null, null, null, null, null, null, null, null, null, null,"
            + " null, null),"
            + " (3, false, -1, -0.0, double('NaN'), -0.01, 0, '', X'', DATE '1970-01-01',"
            + " TIMESTAMP '1969-12-31 23:59:59.999999', TIMESTAMP_NTZ '1900-01-01 00:00:00',"
            + " array(), map(), named_struct('a', null, 'b', null), 'ü=%')");
    SoftAssertions softly = new SoftAssertions();
    assertSameRows(softly, pair, "insert", "SELECT * FROM {t}");
    both(pair, "UPDATE {t} SET s = concat(coalesce(s, ''), '!') WHERE id IN (1, 2)");
    assertSameRows(softly, pair, "update", "SELECT * FROM {t}");
    softly
        .assertThat(
            publishedTable(pair).schema().select(spark.table(pair.iceberg).columns()).asStruct())
        .as("Iceberg types of the published table")
        .isEqualTo(nativeTable(pair).schema().asStruct());
    assertSameRows(softly, pair, "filter on null partition", "SELECT id FROM {t} WHERE p IS NULL");
    assertSameRows(softly, pair, "filter on partition", "SELECT id FROM {t} WHERE p = 'a b/c'");
    softly.assertAll();
  }

  @ParameterizedTest
  @ValueSource(strings = {"cow", "mor"})
  void metadataTablesDescribeTheSameData(String type) {
    Pair pair = create("meta_tables", "id INT, data STRING, dep STRING", "dep", type);
    both(pair, "INSERT INTO {t} VALUES (1, 'a', 'hr'), (2, null, 'hr'), (3, 'c', 'eng')");
    both(pair, "INSERT INTO {t} VALUES (4, 'd', 'eng'), (5, 'e', 'ops')");
    both(pair, "UPDATE {t} SET data = 'z' WHERE id = 1");
    both(pair, "DELETE FROM {t} WHERE id = 3");
    SoftAssertions softly = new SoftAssertions();

    String perPartition =
        "SELECT partition.dep, sum(record_count), min(readable_metrics.id.lower_bound),"
            + " max(readable_metrics.id.upper_bound), sum(readable_metrics.data.null_value_count)"
            + " FROM %s WHERE content = 0 GROUP BY partition.dep";
    List<String> nativeFiles =
        rows(spark.sql(String.format(perPartition, pair.iceberg + ".files")));
    spark
        .read()
        .format("iceberg")
        .load(pair.hudiPath + "#files")
        .createOrReplaceTempView("ptf_files");
    List<String> publishedFiles = rows(spark.sql(String.format(perPartition, "ptf_files")));
    softly
        .assertThat(publishedFiles)
        .as("data files per partition: records, id bounds, data null counts (before deletes)")
        .isEqualTo(nativeFiles);

    String partitions = "SELECT partition.dep, record_count FROM %s";
    List<String> nativePartitions =
        rows(spark.sql(String.format(partitions, pair.iceberg + ".partitions")));
    spark
        .read()
        .format("iceberg")
        .load(pair.hudiPath + "#partitions")
        .createOrReplaceTempView("ptf_partitions");
    softly
        .assertThat(rows(spark.sql(String.format(partitions, "ptf_partitions"))))
        .as("partitions metadata table")
        .isEqualTo(nativePartitions);

    List<Snapshot> nativeHistory = history(nativeTable(pair));
    List<String> nativeOps =
        nativeHistory.stream().map(Snapshot::operation).collect(Collectors.toList());
    List<String> publishedOps =
        SmallFileInserts.operations(nativeHistory, history(publishedTable(pair)));
    softly.assertThat(publishedOps).as("snapshot operations").isEqualTo(nativeOps);
    softly
        .assertThat(publishedTable(pair).currentSnapshot().summary().get("total-records"))
        .as("total-records")
        .isEqualTo(nativeTable(pair).currentSnapshot().summary().get("total-records"));
    softly.assertAll();
  }

  @ParameterizedTest
  @ValueSource(strings = {"cow", "mor"})
  void timeTravelToEverySnapshot(String type) {
    Pair pair = create("travel", "id INT, data STRING", null, type);
    both(pair, "INSERT INTO {t} VALUES (1, 'a'), (2, 'b')");
    both(pair, "INSERT INTO {t} VALUES (3, 'c')");
    both(pair, "UPDATE {t} SET data = 'x' WHERE id = 1");
    both(pair, "DELETE FROM {t} WHERE id = 2");
    List<Snapshot> nativeHistory = history(nativeTable(pair));
    List<Snapshot> publishedHistory = history(publishedTable(pair));
    assertThat(publishedHistory).hasSameSizeAs(nativeHistory);
    SoftAssertions softly = new SoftAssertions();
    for (int i = 0; i < nativeHistory.size(); i++) {
      // SQL VERSION AS OF is parsed by Hudi's session extension and fails on Iceberg tables, and
      // Spark 3.5 ignores read options on table(), so both sides read by path.
      String nativePath =
          new File(warehouse, "ice/db/" + pair.iceberg.substring("ice.db.".length()))
              .getAbsolutePath();
      List<String> nativeRows =
          rows(
              spark
                  .read()
                  .format("iceberg")
                  .option("versionAsOf", nativeHistory.get(i).snapshotId())
                  .load(nativePath));
      List<String> publishedRows =
          rows(
              withoutHudiMetaColumns(
                  spark
                      .read()
                      .format("iceberg")
                      .option("versionAsOf", publishedHistory.get(i).snapshotId())
                      .load(pair.hudiPath)));
      softly.assertThat(publishedRows).as("rows as of snapshot " + i).isEqualTo(nativeRows);
    }
    softly.assertAll();
  }

  /**
   * Iceberg's incremental append scan reads append snapshots only, so the Hudi table keeps its
   * inserts out of existing small files, see {@link SmallFileInserts}.
   */
  @Test
  void incrementalReadOfAppends() {
    Pair pair = create("incremental", "id INT, data STRING", null, "cow");
    spark.conf().set("hoodie.parquet.small.file.limit", "0");
    try {
      both(pair, "INSERT INTO {t} VALUES (1, 'a')");
      both(pair, "INSERT INTO {t} VALUES (2, 'b')");
      both(pair, "INSERT INTO {t} VALUES (3, 'c')");
    } finally {
      spark.conf().unset("hoodie.parquet.small.file.limit");
    }
    List<Snapshot> nativeHistory = history(nativeTable(pair));
    List<Snapshot> publishedHistory = history(publishedTable(pair));
    assertThat(publishedHistory.stream().map(Snapshot::operation))
        .containsExactlyElementsOf(
            nativeHistory.stream().map(Snapshot::operation).collect(Collectors.toList()));
    List<String> nativeRows =
        rows(
            spark
                .read()
                .format("iceberg")
                .option("start-snapshot-id", nativeHistory.get(0).snapshotId())
                .option("end-snapshot-id", nativeHistory.get(2).snapshotId())
                .table(pair.iceberg));
    List<String> publishedRows =
        rows(
            withoutHudiMetaColumns(
                spark
                    .read()
                    .format("iceberg")
                    .option("start-snapshot-id", publishedHistory.get(0).snapshotId())
                    .option("end-snapshot-id", publishedHistory.get(2).snapshotId())
                    .load(pair.hudiPath)));
    assertThat(publishedRows).isEqualTo(nativeRows);
  }

  @ParameterizedTest
  @ValueSource(strings = {"cow", "mor"})
  void insertOverwriteAndCtas(String type) {
    Pair pair = create("overwrite", "id INT, data STRING, dep STRING", "dep", type);
    both(pair, "INSERT INTO {t} VALUES (1, 'a', 'hr'), (2, 'b', 'eng'), (3, 'c', 'ops')");
    SoftAssertions softly = new SoftAssertions();
    both(pair, "INSERT OVERWRITE {t} PARTITION (dep = 'hr') SELECT 10, 'x'");
    assertSameRows(softly, pair, "static partition overwrite", "SELECT * FROM {t}");
    spark.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
    both(pair, "INSERT OVERWRITE {t} SELECT 20, 'y', 'eng'");
    assertSameRows(softly, pair, "dynamic partition overwrite", "SELECT * FROM {t}");
    spark.conf().set("spark.sql.sources.partitionOverwriteMode", "static");
    both(pair, "INSERT OVERWRITE {t} SELECT 30, 'z', 'new'");
    assertSameRows(softly, pair, "full overwrite", "SELECT * FROM {t}");
    softly.assertAll();
  }

  @Test
  void createTableAsSelect() {
    String suffix = UUID.randomUUID().toString().substring(0, 8);
    String hudi = "default.ctas_" + suffix;
    String path = new File(warehouse, "ctas_" + suffix).toURI().toString();
    created.add(hudi);
    spark.sql(
        String.format(
            "CREATE TABLE %s USING hudi LOCATION '%s' TBLPROPERTIES ('type' 'cow',"
                + " 'hoodie.table.format' 'ICEBERG') AS SELECT 1 AS id, 'a' AS data",
            hudi, path));
    assertThat(rows(withoutHudiMetaColumns(spark.read().format("iceberg").load(path))))
        .containsExactly("1|a");
  }

  @ParameterizedTest
  @ValueSource(strings = {"cow", "mor"})
  void schemaEvolution(String type) {
    Pair pair = create("evolve", "id INT, data STRING", null, type);
    both(pair, "INSERT INTO {t} VALUES (1, 'a')");
    int dataFieldId = publishedTable(pair).schema().findField("data").fieldId();
    SoftAssertions softly = new SoftAssertions();
    both(pair, "ALTER TABLE {t} ADD COLUMNS (extra STRING)");
    both(pair, "INSERT INTO {t} VALUES (2, 'b', 'e')");
    assertSameRows(softly, pair, "add column", "SELECT * FROM {t}");
    both(pair, "UPDATE {t} SET extra = 'u' WHERE id = 1");
    assertSameRows(softly, pair, "update after add column", "SELECT * FROM {t}");
    try {
      spark.conf().set("hoodie.schema.on.read.enable", "true");
      spark.sql(
          "ALTER TABLE "
              + pair.hudi
              + " SET TBLPROPERTIES ('hoodie.schema.on.read.enable' 'true')");
      both(pair, "ALTER TABLE {t} RENAME COLUMN extra TO renamed");
      both(pair, "INSERT INTO {t} VALUES (3, 'c', 'r')");
      assertSameRows(softly, pair, "rename column", "SELECT * FROM {t}");
      both(pair, "ALTER TABLE {t} ALTER COLUMN id TYPE BIGINT");
      both(pair, "INSERT INTO {t} VALUES (10000000000, 'd', 'big')");
      assertSameRows(softly, pair, "promote int to bigint", "SELECT * FROM {t}");
      both(pair, "ALTER TABLE {t} DROP COLUMN renamed");
      assertSameRows(softly, pair, "drop column", "SELECT * FROM {t}");
      Types.StructType publishedSchema = publishedTable(pair).schema().asStruct();
      softly
          .assertThat(publishedSchema.field("data").fieldId())
          .as("field id of an untouched column across schema changes")
          .isEqualTo(dataFieldId);
      softly
          .assertThat(
              publishedTable(pair).schema().select("id", "data").asStruct().fields().get(0).type())
          .as("Iceberg type of the promoted column")
          .isEqualTo(Types.LongType.get());
    } catch (Exception e) {
      softly.fail("schema evolution statement failed: " + rootMessage(e));
    } finally {
      spark.conf().unset("hoodie.schema.on.read.enable");
    }
    softly.assertAll();
  }

  @Test
  void rowLineageIsKeptAcrossAnUpdate() {
    Pair pair = create("lineage", "id INT, data STRING", null, "cow");
    both(pair, "INSERT INTO {t} VALUES (1, 'a'), (2, 'b')");
    String[] lineage = {"id", "data", "_row_id", "_last_updated_sequence_number"};
    List<String> nativeBefore =
        rows(spark.table(pair.iceberg).select(lineage[0], lineage[1], lineage[2], lineage[3]));
    List<String> publishedBefore =
        rows(icebergReadOfHudi(pair).select(lineage[0], lineage[1], lineage[2], lineage[3]));
    both(pair, "UPDATE {t} SET data = 'x' WHERE id = 1");
    List<String> nativeAfter =
        rows(spark.table(pair.iceberg).select(lineage[0], lineage[1], lineage[2], lineage[3]));
    List<String> publishedAfter =
        rows(icebergReadOfHudi(pair).select(lineage[0], lineage[1], lineage[2], lineage[3]));
    SoftAssertions softly = new SoftAssertions();
    softly.assertThat(publishedBefore).as("lineage after insert").isEqualTo(nativeBefore);
    softly.assertThat(publishedAfter).as("lineage after update").isEqualTo(nativeAfter);
    softly.assertAll();
  }

  @Test
  void rowLineageIsKeptAcrossAMerge() {
    Pair pair = create("lineage_merge", "id INT, data STRING", null, "cow");
    both(pair, "INSERT INTO {t} VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    both(
        pair,
        "MERGE INTO {t} t USING (SELECT 2 AS id, 'y' AS data) s ON t.id = s.id"
            + " WHEN MATCHED THEN UPDATE SET t.data = s.data WHEN NOT MATCHED THEN INSERT *");
    String[] lineage = {"id", "data", "_row_id", "_last_updated_sequence_number"};
    assertThat(rows(icebergReadOfHudi(pair).select(lineage[0], lineage[1], lineage[2], lineage[3])))
        .as("lineage after merge")
        .isEqualTo(
            rows(spark.table(pair.iceberg).select(lineage[0], lineage[1], lineage[2], lineage[3])));
  }

  @Test
  void orphanFileCleanupKeepsTheHudiTimeline() {
    Pair pair = create("orphans", "id INT, data STRING", null, "cow");
    both(pair, "INSERT INTO {t} VALUES (1, 'a')");
    both(pair, "UPDATE {t} SET data = 'b' WHERE id = 1");
    List<String> wouldDelete = new ArrayList<>();
    SparkActions.get(spark)
        .deleteOrphanFiles(publishedTable(pair))
        .olderThan(System.currentTimeMillis() + 60_000)
        .deleteWith(wouldDelete::add)
        .execute();
    assertThat(wouldDelete)
        .as("files Iceberg's orphan cleanup would delete from the published table")
        .noneMatch(path -> path.contains("/.hoodie/"));
  }

  @Test
  void expiringSnapshotsThroughIcebergKeepsTheTableWritable() {
    Pair pair = create("expire", "id INT, data STRING", null, "cow");
    both(pair, "INSERT INTO {t} VALUES (1, 'a')");
    both(pair, "INSERT INTO {t} VALUES (2, 'b')");
    both(pair, "UPDATE {t} SET data = 'x' WHERE id = 1");
    for (Table table : Arrays.asList(nativeTable(pair), publishedTable(pair))) {
      table.expireSnapshots().retainLast(1).expireOlderThan(System.currentTimeMillis()).commit();
    }
    SoftAssertions softly = new SoftAssertions();
    try {
      both(pair, "INSERT INTO {t} VALUES (3, 'c')");
      both(pair, "DELETE FROM {t} WHERE id = 2");
      assertSameRows(softly, pair, "write after expiry", "SELECT * FROM {t}");
      softly
          .assertThat(rows(withoutHudiMetaColumns(spark.read().format("hudi").load(pair.hudiPath))))
          .as("Hudi read after expiry")
          .isEqualTo(rowsOfBoth(pair, "SELECT * FROM {t}").get(0));
    } catch (Exception e) {
      softly.fail("write after Iceberg expired snapshots failed: " + rootMessage(e));
    }
    softly.assertAll();
  }

  @Test
  void appendByAnotherIcebergWriterSurvivesTheNextHudiCommit() throws IOException {
    Pair pair = create("foreign", "id INT, data STRING", null, "cow");
    both(pair, "INSERT INTO {t} VALUES (1, 'a')");
    // Another Iceberg writer appends a file to both tables through Iceberg.
    for (String target : Arrays.asList(pair.iceberg, "default_iceberg.`" + pair.hudiPath + "`")) {
      try {
        spark.sql("SELECT 2 AS id, 'foreign' AS data").writeTo(target).append();
      } catch (Exception e) {
        Table table = target.equals(pair.iceberg) ? nativeTable(pair) : publishedTable(pair);
        appendThroughIcebergApi(table);
      }
    }
    SoftAssertions softly = new SoftAssertions();
    assertSameRows(softly, pair, "after the foreign append", "SELECT * FROM {t}");
    try {
      both(pair, "INSERT INTO {t} VALUES (3, 'c')");
      both(pair, "UPDATE {t} SET data = 'u' WHERE id = 1");
      assertSameRows(softly, pair, "after the next Hudi commits", "SELECT * FROM {t}");
    } catch (Exception e) {
      softly.fail("Hudi write after a foreign Iceberg append failed: " + rootMessage(e));
    }
    softly.assertAll();
  }

  private static void appendThroughIcebergApi(Table table) throws IOException {
    Dataset<Row> df = spark.sql("SELECT 2 AS id, 'foreign' AS data");
    File dir = Files.createTempDirectory(warehouse.toPath(), "foreign").toFile();
    df.coalesce(1).write().parquet(new File(dir, "data").getAbsolutePath());
    File parquet =
        Arrays.stream(new File(dir, "data").listFiles())
            .filter(f -> f.getName().endsWith(".parquet"))
            .findFirst()
            .get();
    DataFile file =
        org.apache.iceberg.DataFiles.builder(table.spec())
            .withPath(parquet.toURI().toString())
            .withFileSizeInBytes(parquet.length())
            .withRecordCount(1)
            .withFormat(org.apache.iceberg.FileFormat.PARQUET)
            .build();
    table.newAppend().appendFile(file).commit();
  }

  /**
   * One Spark type per case, so a type the writer or the published schema cannot carry is named.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "BOOLEAN|true",
        "BIGINT|10000000000",
        "FLOAT|1.5",
        "DOUBLE|2.25",
        "DECIMAL(10, 2)|12345678.91",
        "DECIMAL(38, 10)|1234567890123456789012345678.1234567891",
        "STRING|'h\u00e9llo'",
        "BINARY|X'CAFE'",
        "DATE|DATE '2024-02-29'",
        "TIMESTAMP|TIMESTAMP '2024-02-29 12:34:56.123456'",
        "TIMESTAMP_NTZ|TIMESTAMP_NTZ '2024-02-29 12:34:56.123456'",
        "ARRAY<INT>|array(1, null, 3)",
        "MAP<STRING, INT>|map('k', 1)",
        "STRUCT<a: INT, b: STRING>|named_struct('a', 1, 'b', 'x')",
        "TINYINT|CAST(1 AS TINYINT)",
        "SMALLINT|CAST(1 AS SMALLINT)"
      })
  void typeRoundTrips(String spec) {
    String[] parts = spec.split("\\|", 2);
    Pair pair = create("type", "id INT, c " + parts[0], null, "cow");
    SoftAssertions softly = new SoftAssertions();
    try {
      both(pair, "INSERT INTO {t} VALUES (1, " + parts[1] + "), (2, null)");
      assertSameRows(softly, pair, parts[0], "SELECT * FROM {t}");
    } catch (Exception e) {
      softly.fail(parts[0] + ": " + rootMessage(e));
    }
    softly.assertAll();
  }

  /**
   * Hudi writes Parquet's two-level list layout unless told otherwise; null elements need three.
   */
  @Test
  void arrayWithANullElementUnderTheThreeLevelListLayout() {
    spark.conf().set("spark.hadoop.parquet.avro.write-old-list-structure", "false");
    spark
        .sparkContext()
        .hadoopConfiguration()
        .set("parquet.avro.write-old-list-structure", "false");
    try {
      Pair pair = create("array3", "id INT, c ARRAY<INT>", null, "cow");
      SoftAssertions softly = new SoftAssertions();
      try {
        both(pair, "INSERT INTO {t} VALUES (1, array(1, null, 3)), (2, null)");
        assertSameRows(softly, pair, "array with a null element", "SELECT * FROM {t}");
        both(pair, "UPDATE {t} SET c = array(9) WHERE id = 1");
        assertSameRows(softly, pair, "after update", "SELECT * FROM {t}");
      } catch (Exception e) {
        softly.fail(rootMessage(e));
      }
      softly.assertAll();
    } finally {
      spark.sparkContext().hadoopConfiguration().unset("parquet.avro.write-old-list-structure");
      spark.conf().unset("spark.hadoop.parquet.avro.write-old-list-structure");
    }
  }

  /** The same partition values on both tables, including ones that need escaping in a path. */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "STRING|'a b/c'",
        "STRING|'\u00fc=%'",
        "STRING|null",
        "STRING|''",
        "INT|-1",
        "DATE|DATE '2024-02-29'",
        "BIGINT|10000000000"
      })
  void partitionValueRoundTrips(String spec) {
    String[] parts = spec.split("\\|", 2);
    Pair pair = create("pvalue", "id INT, p " + parts[0], "p", "cow");
    SoftAssertions softly = new SoftAssertions();
    try {
      both(pair, "INSERT INTO {t} VALUES (1, " + parts[1] + ")");
      assertSameRows(softly, pair, "rows", "SELECT * FROM {t}");
      String filter = parts[1].equals("null") ? "p IS NULL" : "p = " + parts[1];
      assertSameRows(softly, pair, "filtered", "SELECT id FROM {t} WHERE " + filter);
      List<String> nativePartitions =
          rows(spark.sql("SELECT partition FROM " + pair.iceberg + ".files"));
      List<String> publishedPartitions =
          rows(spark.read().format("iceberg").load(pair.hudiPath + "#files").select("partition"));
      softly
          .assertThat(publishedPartitions)
          .as("partition values in the files table")
          .isEqualTo(nativePartitions);
    } catch (Exception e) {
      softly.fail(parts[0] + " " + parts[1] + ": " + rootMessage(e));
    }
    softly.assertAll();
  }

  /** Control for the rename case: the same statements on a Hudi table with no published format. */
  @Test
  void renameColumnOnAHudiTableWithoutThePublishedFormat() {
    String suffix = UUID.randomUUID().toString().substring(0, 8);
    String hudi = "default.native_" + suffix;
    created.add(hudi);
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, data STRING) USING hudi LOCATION '%s' TBLPROPERTIES ('type' 'cow')",
            hudi, new File(warehouse, "native_" + suffix).toURI()));
    spark.sql("INSERT INTO " + hudi + " VALUES (1, 'a')");
    spark.sql("ALTER TABLE " + hudi + " ADD COLUMNS (extra STRING)");
    spark.conf().set("hoodie.schema.on.read.enable", "true");
    try {
      spark.sql(
          "ALTER TABLE " + hudi + " SET TBLPROPERTIES ('hoodie.schema.on.read.enable' 'true')");
      spark.sql("ALTER TABLE " + hudi + " RENAME COLUMN extra TO renamed");
      spark.sql("INSERT INTO " + hudi + " VALUES (3, 'c', 'r')");
      assertThat(rows(spark.sql("SELECT id, data, renamed FROM " + hudi)))
          .containsExactly("1|a|null", "3|c|r");
    } finally {
      spark.conf().unset("hoodie.schema.on.read.enable");
    }
  }

  /** Whether turning off Hudi's prepped SQL writes lets a merge-on-read UPDATE publish. */
  @Test
  void mergeOnReadUpdateWithoutPreppedWrites() {
    Pair pair = create("mor_noprep", "id INT, data STRING", null, "mor");
    spark.conf().set("hoodie.spark.sql.optimized.writes.enable", "false");
    SoftAssertions softly = new SoftAssertions();
    try {
      both(pair, "INSERT INTO {t} VALUES (1, 'a'), (2, 'b')");
      both(pair, "UPDATE {t} SET data = 'x' WHERE id = 1");
      assertSameRows(softly, pair, "update", "SELECT * FROM {t}");
      both(pair, "DELETE FROM {t} WHERE id = 2");
      assertSameRows(softly, pair, "delete", "SELECT * FROM {t}");
      both(
          pair,
          "MERGE INTO {t} t USING (SELECT 1 AS id, 'm' AS data) s ON t.id = s.id"
              + " WHEN MATCHED THEN UPDATE SET t.data = s.data");
      assertSameRows(softly, pair, "merge", "SELECT * FROM {t}");
    } catch (Exception e) {
      softly.fail(rootMessage(e));
    } finally {
      spark.conf().unset("hoodie.spark.sql.optimized.writes.enable");
    }
    softly.assertAll();
  }

  /** Whether an INSERT publishes an append once Hudi stops packing new rows into existing files. */
  @Test
  void insertPublishesAnAppendWithoutSmallFileHandling() {
    Pair pair = create("append", "id INT, data STRING", null, "cow");
    spark.conf().set("hoodie.parquet.small.file.limit", "0");
    try {
      both(pair, "INSERT INTO {t} VALUES (1, 'a')");
      both(pair, "INSERT INTO {t} VALUES (2, 'b')");
      assertThat(history(publishedTable(pair)).stream().map(Snapshot::operation))
          .containsExactly("append", "append");
    } finally {
      spark.conf().unset("hoodie.parquet.small.file.limit");
    }
  }

  /** The root cause with its frames in Hudi, XTable and Iceberg code, so a failure names a line. */
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
}
