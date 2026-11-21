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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.common.model.HoodieRecord;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

/**
 * Runs the same SQL against a native Iceberg table and a copy-on-write Hudi table published to
 * Iceberg by the pluggable table format, and compares what Iceberg's changelog and Structured
 * Streaming readers produce for the two.
 *
 * <p>The Hudi table lives under the warehouse of a Hadoop catalog, {@code hcat}, so that the {@code
 * create_changelog_view} procedure, which only takes catalog identifiers, can reach it.
 */
class TestIcebergChangelogAndStreamingParity {

  private static final String DEFAULT = "default";
  private static final String NO_SMALL_FILES = "smallFileLimit0";

  private static SparkSession spark;
  private static File warehouse;

  private final List<String> created = new ArrayList<>();

  /** A native Iceberg table and a Hudi table published to Iceberg, created from the same DDL. */
  private static final class Pair {
    final String iceberg;
    final String hudi;
    final String hudiPath;
    final String hudiInCatalog;

    Pair(String iceberg, String hudi, String hudiPath, String hudiInCatalog) {
      this.iceberg = iceberg;
      this.hudi = hudi;
      this.hudiPath = hudiPath;
      this.hudiInCatalog = hudiInCatalog;
    }
  }

  @BeforeAll
  static void startSpark() throws IOException {
    warehouse = Files.createTempDirectory("ptf-changelog-parity").toFile();
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
            .config("spark.sql.catalog.hcat", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.hcat.type", "hadoop")
            .config(
                "spark.sql.catalog.hcat.warehouse", new File(warehouse, "hcat").getAbsolutePath())
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

  private Pair create(String name, String setting) {
    String suffix = name + "_" + UUID.randomUUID().toString().substring(0, 8);
    String iceberg = "ice.db.i_" + suffix;
    String hudi = "default.h_" + suffix;
    String hudiPath = new File(warehouse, "hcat/db/h_" + suffix).toURI().toString();
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, data STRING) USING iceberg TBLPROPERTIES"
                + " ('format-version' '3', 'write.delete.mode' 'copy-on-write',"
                + " 'write.update.mode' 'copy-on-write', 'write.merge.mode' 'copy-on-write')",
            iceberg));
    spark.conf().set("hoodie.write.updates.as.deletes.and.inserts", "false");
    spark.conf().set("hoodie.index.type", "SIMPLE");
    if (setting.equals(NO_SMALL_FILES)) {
      spark.conf().set("hoodie.parquet.small.file.limit", "0");
    } else {
      spark.conf().unset("hoodie.parquet.small.file.limit");
    }
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, data STRING) USING hudi LOCATION '%s' TBLPROPERTIES"
                + " ('type' 'cow', 'hoodie.table.format' 'ICEBERG',"
                + " 'hoodie.table.format.iceberg.format-version' '3')",
            hudi, hudiPath));
    created.add(iceberg);
    created.add(hudi);
    return new Pair(iceberg, hudi, hudiPath, "db.h_" + suffix);
  }

  /** Runs a statement, with {t} standing for the table, against both tables. */
  private static void both(Pair pair, String template) {
    spark.sql(template.replace("{t}", pair.iceberg));
    spark.sql(template.replace("{t}", pair.hudi));
  }

  private static Dataset<Row> withoutHudiMetaColumns(Dataset<Row> df) {
    return df.drop(
        Arrays.stream(df.columns())
            .filter(HoodieRecord.HOODIE_META_COLUMNS::contains)
            .toArray(String[]::new));
  }

  private static List<String> rows(Dataset<Row> df) {
    return df.collectAsList().stream()
        .map(row -> row.mkString("|"))
        .sorted()
        .collect(Collectors.toList());
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

  private static List<String> operations(Table table) {
    return history(table).stream().map(Snapshot::operation).collect(Collectors.toList());
  }

  /** Records what was compared, so a run's output shows both sides even when they match. */
  private static void compare(
      SoftAssertions softly, String step, List<String> nativeRows, List<String> publishedRows) {
    System.out.println(
        "PARITY " + step + "\n  native:    " + nativeRows + "\n  published: " + publishedRows);
    softly.assertThat(publishedRows).as(step + ": published vs native").isEqualTo(nativeRows);
  }

  private static final String CHANGE_COLUMNS = "id, data, _change_type, _change_ordinal";

  /** Insert, insert, update one row, delete one row; the changelog compared three ways. */
  @ParameterizedTest
  @ValueSource(strings = {DEFAULT, NO_SMALL_FILES})
  void changelogOfInsertsUpdateAndDelete(String setting) {
    Pair pair = create("changelog", setting);
    both(pair, "INSERT INTO {t} VALUES (1, 'a'), (2, 'b')");
    both(pair, "INSERT INTO {t} VALUES (3, 'c')");
    both(pair, "UPDATE {t} SET data = 'x' WHERE id = 1");
    both(pair, "DELETE FROM {t} WHERE id = 2");
    SoftAssertions softly = new SoftAssertions();
    String tag = "[" + setting + "] ";
    compare(
        softly,
        tag + "snapshot operations",
        operations(nativeTable(pair)),
        SmallFileInserts.operations(history(nativeTable(pair)), history(publishedTable(pair))));

    // The changes metadata table, with carry-over rows, read by name and by path.
    try {
      List<String> nativeChanges =
          rows(spark.table(pair.iceberg + ".changes").selectExpr(CHANGE_COLUMNS.split(", ")));
      List<String> publishedChanges =
          rows(
              spark
                  .read()
                  .format("iceberg")
                  .load(pair.hudiPath + "#changes")
                  .selectExpr(CHANGE_COLUMNS.split(", ")));
      compare(softly, tag + "changes metadata table", nativeChanges, publishedChanges);
    } catch (Exception e) {
      softly.fail(tag + "changes metadata table failed: " + rootMessage(e));
    }

    // The procedure, which drops carry-over rows and pairs updates by the identifier column.
    try {
      spark.sql(
          String.format(
              "CALL ice.system.create_changelog_view(table => '%s', changelog_view => 'cl_native',"
                  + " identifier_columns => array('id'))",
              pair.iceberg.substring("ice.".length())));
      spark.sql(
          String.format(
              "CALL hcat.system.create_changelog_view(table => '%s', changelog_view => 'cl_ptf',"
                  + " identifier_columns => array('id'))",
              pair.hudiInCatalog));
      compare(
          softly,
          tag + "create_changelog_view, all snapshots",
          rows(spark.sql("SELECT " + CHANGE_COLUMNS + " FROM cl_native")),
          rows(spark.sql("SELECT " + CHANGE_COLUMNS + " FROM cl_ptf")));

      long nativeFirst = history(nativeTable(pair)).get(0).snapshotId();
      long publishedFirst = history(publishedTable(pair)).get(0).snapshotId();
      spark.sql(
          String.format(
              "CALL ice.system.create_changelog_view(table => '%s', changelog_view => 'cl_native2',"
                  + " options => map('start-snapshot-id', '%d'), identifier_columns => array('id'))",
              pair.iceberg.substring("ice.".length()), nativeFirst));
      spark.sql(
          String.format(
              "CALL hcat.system.create_changelog_view(table => '%s', changelog_view => 'cl_ptf2',"
                  + " options => map('start-snapshot-id', '%d'), identifier_columns => array('id'))",
              pair.hudiInCatalog, publishedFirst));
      compare(
          softly,
          tag + "create_changelog_view, after the first snapshot",
          rows(spark.sql("SELECT " + CHANGE_COLUMNS + " FROM cl_native2")),
          rows(spark.sql("SELECT " + CHANGE_COLUMNS + " FROM cl_ptf2")));
    } catch (Exception e) {
      softly.fail(tag + "create_changelog_view failed: " + rootMessage(e));
    }

    // The procedure's carry-over removal compares every column, Hudi meta columns included.
    // Removing them before the comparison tells a meta-column difference from a data difference.
    try {
      Dataset<Row> raw =
          withoutHudiMetaColumns(spark.read().format("iceberg").load(pair.hudiPath + "#changes"));
      List<String> netOfCarryovers =
          rows(
              raw.groupBy("id", "data", "_change_ordinal")
                  .pivot("_change_type", Arrays.asList("INSERT", "DELETE"))
                  .count()
                  .na()
                  .fill(0)
                  .where("`INSERT` != `DELETE`")
                  .selectExpr(
                      "id",
                      "data",
                      "IF(`INSERT` > `DELETE`, 'INSERT', 'DELETE') AS _change_type",
                      "_change_ordinal"));
      List<String> nativeNet =
          rows(
              spark
                  .table(pair.iceberg + ".changes")
                  .groupBy("id", "data", "_change_ordinal")
                  .pivot("_change_type", Arrays.asList("INSERT", "DELETE"))
                  .count()
                  .na()
                  .fill(0)
                  .where("`INSERT` != `DELETE`")
                  .selectExpr(
                      "id",
                      "data",
                      "IF(`INSERT` > `DELETE`, 'INSERT', 'DELETE') AS _change_type",
                      "_change_ordinal"));
      compare(
          softly,
          tag + "changes without carry-overs, Hudi meta columns dropped first",
          nativeNet,
          netOfCarryovers);
    } catch (Exception e) {
      softly.fail(tag + "changes without meta columns failed: " + rootMessage(e));
    }

    // A carried-over row is deleted and re-inserted unchanged in the same snapshot. Iceberg drops
    // the pair only when every column, the Hudi meta columns included, is the same in both.
    try {
      List<Row> raw =
          spark.read().format("iceberg").load(pair.hudiPath + "#changes").collectAsList();
      // Per (id, data, ordinal): the change types seen and every value of each meta column.
      Map<String, List<Row>> byRow = new LinkedHashMap<>();
      for (Row row : raw) {
        String key = row.getAs("id") + "|" + row.getAs("data") + "|" + row.getAs("_change_ordinal");
        byRow.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
      }
      List<String> changedMetaColumns = new ArrayList<>();
      List<String> publishedColumns =
          raw.isEmpty() ? Collections.emptyList() : Arrays.asList(raw.get(0).schema().fieldNames());
      for (String column : HoodieRecord.HOODIE_META_COLUMNS) {
        if (!publishedColumns.contains(column)) {
          // Not part of the published schema, so it cannot keep a carry-over from cancelling.
          continue;
        }
        boolean changed =
            byRow.values().stream()
                .filter(
                    group ->
                        group.stream().map(r -> r.getAs("_change_type")).distinct().count() > 1)
                .anyMatch(
                    group ->
                        group.stream()
                                .map(r -> String.valueOf((Object) r.getAs(column)))
                                .distinct()
                                .count()
                            > 1);
        if (changed) {
          changedMetaColumns.add(column);
        }
      }
      compare(
          softly,
          tag + "Hudi meta columns that change on a carried-over row",
          Collections.emptyList(),
          changedMetaColumns);
    } catch (Exception e) {
      softly.fail(tag + "carry-over meta column check failed: " + rootMessage(e));
    }
    softly.assertAll();
  }

  /**
   * Three inserts streamed once after the fact, then once per insert, with and without skipping.
   */
  @ParameterizedTest
  @ValueSource(strings = {DEFAULT, NO_SMALL_FILES})
  void streamingReadOfAppends(String setting) throws IOException {
    assumeStreamingReadsAppends(setting);
    Pair pair = create("stream", setting);
    String tag = "[" + setting + "] ";
    SoftAssertions softly = new SoftAssertions();

    List<String> statements =
        Arrays.asList(
            "INSERT INTO {t} VALUES (1, 'a')",
            "INSERT INTO {t} VALUES (2, 'b')",
            "INSERT INTO {t} VALUES (3, 'c')");

    // One stream per option set, each restarted with its checkpoint after every insert.
    Map<String, Map<String, String>> optionSets = new LinkedHashMap<>();
    optionSets.put("no options", Collections.emptyMap());
    optionSets.put(
        "skip overwrites", Collections.singletonMap("streaming-skip-overwrite-snapshots", "true"));
    Map<String, IncrementalStream> nativeStreams = new LinkedHashMap<>();
    Map<String, IncrementalStream> publishedStreams = new LinkedHashMap<>();
    for (String key : optionSets.keySet()) {
      nativeStreams.put(key, new IncrementalStream());
      publishedStreams.put(key, new IncrementalStream());
    }
    for (String statement : statements) {
      both(pair, statement);
      for (Map.Entry<String, Map<String, String>> options : optionSets.entrySet()) {
        nativeStreams
            .get(options.getKey())
            .run(r -> r.options(options.getValue()).load(pair.iceberg));
        publishedStreams
            .get(options.getKey())
            .run(r -> withoutHudiMetaColumns(r.options(options.getValue()).load(pair.hudiPath)));
      }
    }
    compare(
        softly,
        tag + "snapshot operations",
        operations(nativeTable(pair)),
        operations(publishedTable(pair)));
    for (String key : optionSets.keySet()) {
      compare(
          softly,
          tag + "stream run after every insert, " + key,
          nativeStreams.get(key).outcome(),
          publishedStreams.get(key).outcome());
    }

    // One AvailableNow run into a memory sink after all three inserts.
    for (Map.Entry<String, Map<String, String>> options : optionSets.entrySet()) {
      compare(
          softly,
          tag + "one stream run into a memory sink after the inserts, " + options.getKey(),
          memorySinkOutcome(r -> r.options(options.getValue()).load(pair.iceberg)),
          memorySinkOutcome(
              r -> withoutHudiMetaColumns(r.options(options.getValue()).load(pair.hudiPath))));
    }
    softly.assertAll();
  }

  /** Iceberg fails a stream on an overwrite snapshot, or skips it when asked to. */
  @ParameterizedTest
  @ValueSource(strings = {DEFAULT, NO_SMALL_FILES})
  void streamingReadAfterAnUpdate(String setting) throws IOException {
    assumeStreamingReadsAppends(setting);
    Pair pair = create("stream_update", setting);
    String tag = "[" + setting + "] ";
    both(pair, "INSERT INTO {t} VALUES (1, 'a')");
    both(pair, "INSERT INTO {t} VALUES (2, 'b')");
    both(pair, "UPDATE {t} SET data = 'x' WHERE id = 1");
    SoftAssertions softly = new SoftAssertions();
    compare(
        softly,
        tag + "snapshot operations",
        operations(nativeTable(pair)),
        operations(publishedTable(pair)));
    Map<String, Map<String, String>> optionSets = new LinkedHashMap<>();
    optionSets.put("no options", Collections.emptyMap());
    optionSets.put(
        "skip overwrites", Collections.singletonMap("streaming-skip-overwrite-snapshots", "true"));
    for (Map.Entry<String, Map<String, String>> options : optionSets.entrySet()) {
      compare(
          softly,
          tag + "stream after an update, " + options.getKey(),
          memorySinkOutcome(r -> r.options(options.getValue()).load(pair.iceberg)),
          memorySinkOutcome(
              r -> withoutHudiMetaColumns(r.options(options.getValue()).load(pair.hudiPath))));
    }
    softly.assertAll();
  }

  /**
   * Iceberg's streaming source reads append snapshots and fails on, or skips, an overwrite, so a
   * Hudi table read as a stream keeps its inserts out of existing small files; with them packed,
   * the stream differs by design, see {@link SmallFileInserts}.
   */
  private static void assumeStreamingReadsAppends(String setting) {
    Assumptions.assumeTrue(
        NO_SMALL_FILES.equals(setting),
        "accepted: inserts packed into small files publish overwrite snapshots, which Iceberg"
            + " streaming reads fail on or skip; streaming readers need"
            + " hoodie.parquet.small.file.limit=0");
  }

  /** Rows a single AvailableNow run writes to a memory sink, or the failure that stopped it. */
  private static List<String> memorySinkOutcome(Function<DataStreamReader, Dataset<Row>> source)
      throws IOException {
    String queryName = "stream_" + UUID.randomUUID().toString().replace("-", "");
    File checkpoint = Files.createTempDirectory(warehouse.toPath(), "checkpoint").toFile();
    StreamingQuery query = null;
    try {
      query =
          source
              .apply(spark.readStream().format("iceberg"))
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .option("checkpointLocation", checkpoint.getAbsolutePath())
              .trigger(Trigger.AvailableNow())
              .start();
      query.awaitTermination();
      return rows(spark.table(queryName));
    } catch (Exception e) {
      return Collections.singletonList(failure(e));
    } finally {
      if (query != null) {
        try {
          query.stop();
        } catch (Exception ignored) {
          // already stopped
        }
      }
    }
  }

  /** A stream restarted from its checkpoint with an AvailableNow trigger after each write. */
  private static final class IncrementalStream {
    private final List<String> collected = Collections.synchronizedList(new ArrayList<>());
    private final File checkpoint;
    private String failure;

    IncrementalStream() throws IOException {
      checkpoint = Files.createTempDirectory(warehouse.toPath(), "checkpoint").toFile();
    }

    void run(Function<DataStreamReader, Dataset<Row>> source) {
      if (failure != null) {
        return;
      }
      StreamingQuery query = null;
      try {
        query =
            source
                .apply(spark.readStream().format("iceberg"))
                .writeStream()
                .option("checkpointLocation", checkpoint.getAbsolutePath())
                .trigger(Trigger.AvailableNow())
                .foreachBatch(
                    (VoidFunction2<Dataset<Row>, Long>)
                        (batch, id) -> collected.addAll(rows(batch)))
                .start();
        query.awaitTermination();
      } catch (Exception e) {
        failure = failure(e) + " (after rows " + sorted(collected) + ")";
      } finally {
        if (query != null) {
          try {
            query.stop();
          } catch (Exception ignored) {
            // already stopped
          }
        }
      }
    }

    List<String> outcome() {
      return failure != null ? Collections.singletonList(failure) : sorted(collected);
    }

    private static List<String> sorted(List<String> rows) {
      List<String> copy = new ArrayList<>(rows);
      Collections.sort(copy);
      return copy;
    }
  }

  /**
   * A failure with the snapshot id taken out, so the same failure on both tables compares equal.
   */
  private static String failure(Throwable e) {
    return "FAILED " + rootMessage(e).replaceAll("snapshot(:| id) -?\\d+", "snapshot$1 <id>");
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
