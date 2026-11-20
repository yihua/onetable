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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.common.model.HoodieRecord;

/**
 * A Hudi table partitioned by a TIMESTAMP column, written through Spark SQL. Hudi's SQL key
 * generator writes the partition path as the value truncated to whole seconds, so the path is a
 * one-second bucket rather than the value: rows whose values differ below the second share a path
 * and a file group. An Iceberg reader fills an identity partition column from the file's partition
 * value, so whatever value is published for the file is what every row of it reads back as.
 *
 * <p>Each case writes the same rows to a native Iceberg table and to the published Hudi table and
 * compares what Iceberg and Hudi read. A write the plugin refuses must leave both readers agreeing
 * on the rows that did land, and the table writable. Each step's views are printed with a {@code
 * [SUBSEC]} prefix.
 */
class TestSubSecondTimestampPartitionParity {

  private static SparkSession spark;
  private static File warehouse;

  private final List<String> created = new ArrayList<>();

  @BeforeAll
  static void startSpark() throws IOException {
    warehouse = Files.createTempDirectory("ptf-subsecond").toFile();
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
  }

  private static List<String> rows(Dataset<Row> df) {
    return df.collectAsList().stream()
        .map(row -> row.mkString("|"))
        .sorted()
        .collect(Collectors.toList());
  }

  private static List<String> read(String format, String path) {
    try {
      Dataset<Row> df = spark.read().format(format).load(path);
      return rows(
          df.drop(
                  Arrays.stream(df.columns())
                      .filter(HoodieRecord.HOODIE_META_COLUMNS::contains)
                      .toArray(String[]::new))
              .select("id", "ts"));
    } catch (Exception e) {
      return Collections.singletonList("READ FAILED: " + rootMessage(e));
    }
  }

  /**
   * Each case is a sequence of INSERT value lists separated by ';'. "whole" holds whole seconds
   * only; "single" one sub-second value; "shared" two sub-second values of the same second in one
   * insert; "packed" the same two values in two inserts, which Hudi's small-file handling writes
   * into one file group.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "whole|(1, TIMESTAMP '2024-02-29 10:30:00'), (2, TIMESTAMP '2024-02-29 10:30:00')",
        "single|(1, TIMESTAMP '2024-02-29 10:30:00.123456')",
        "shared|(1, TIMESTAMP '2024-02-29 10:30:00.123456'), (2, TIMESTAMP '2024-02-29 10:30:00.654321')",
        "packed|(1, TIMESTAMP '2024-02-29 10:30:00.123456');(2, TIMESTAMP '2024-02-29 10:30:00.654321')"
      })
  void subSecondTimestampPartitionValues(String spec) {
    String[] parts = spec.split("\\|", 2);
    String name = parts[0];
    String suffix = name + "_" + UUID.randomUUID().toString().substring(0, 8);
    String iceberg = "ice.db.i_" + suffix;
    String hudi = "default.h_" + suffix;
    String hudiPath = new File(warehouse, "h_" + suffix).toURI().toString();
    spark.sql(
        "CREATE TABLE "
            + iceberg
            + " (id INT, ts TIMESTAMP) USING iceberg PARTITIONED BY (ts)"
            + " TBLPROPERTIES ('format-version' '3')");
    spark.conf().set("hoodie.write.updates.as.deletes.and.inserts", "false");
    spark.conf().set("hoodie.index.type", "SIMPLE");
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, ts TIMESTAMP) USING hudi PARTITIONED BY (ts) LOCATION '%s'"
                + " TBLPROPERTIES ('type' 'cow', 'hoodie.table.format' 'ICEBERG',"
                + " 'hoodie.table.format.iceberg.format-version' '3')",
            hudi, hudiPath));
    created.add(iceberg);
    created.add(hudi);
    // The catalog's own location: another suite in the same JVM may have created the session,
    // and with it the catalog's warehouse.
    String nativePath = nativeLocation(iceberg);

    // A whole-second row first, so the Iceberg table exists before any refused write.
    String first = "(0, TIMESTAMP '2024-01-01 00:00:00')";
    spark.sql("INSERT INTO " + iceberg + " VALUES " + first);
    spark.sql("INSERT INTO " + hudi + " VALUES " + first);
    SoftAssertions softly = new SoftAssertions();
    List<String> refusals = new ArrayList<>();
    for (String values : parts[1].split(";")) {
      spark.sql("INSERT INTO " + iceberg + " VALUES " + values);
      try {
        spark.sql("INSERT INTO " + hudi + " VALUES " + values);
      } catch (Exception e) {
        refusals.add(rootMessage(e));
        System.out.printf(
            "[SUBSEC] %s | Hudi INSERT %s refused: %s%n", name, values, rootMessage(e));
      }
      List<String> nativeRows = read("iceberg", nativePath);
      List<String> icebergRows = read("iceberg", hudiPath);
      List<String> hudiRows = read("hudi", hudiPath);
      List<String> files;
      try {
        files =
            rows(
                spark
                    .read()
                    .format("iceberg")
                    .load(hudiPath + "#files")
                    .selectExpr(
                        "partition.ts",
                        "record_count",
                        "readable_metrics.ts.lower_bound",
                        "readable_metrics.ts.upper_bound"));
      } catch (Exception e) {
        files = Collections.singletonList("READ FAILED: " + rootMessage(e));
      }
      System.out.printf(
          "[SUBSEC] %s | after INSERT %s | native Iceberg=%s | published via Iceberg=%s |"
              + " published via Hudi=%s | published files (partition|records|min|max)=%s%n",
          name, values, nativeRows, icebergRows, hudiRows, files);
      if (refusals.isEmpty()) {
        softly
            .assertThat(icebergRows)
            .as(name + ": published via Iceberg vs native")
            .isEqualTo(nativeRows);
        softly
            .assertThat(hudiRows)
            .as(name + ": published via Hudi vs native")
            .isEqualTo(nativeRows);
      } else {
        // A refused write lands nowhere: both readers of the published table agree, and every row
        // they show is a row the native table holds with the same value.
        softly
            .assertThat(icebergRows)
            .as(name + ": published via Iceberg vs via Hudi")
            .isEqualTo(hudiRows);
        softly
            .assertThat(nativeRows)
            .as(name + ": published rows are real rows")
            .containsAll(icebergRows);
      }
    }
    for (String refusal : refusals) {
      softly
          .assertThat(refusal)
          .as(name + ": refusal names the cause")
          .contains("fractional seconds");
    }
    // The table stays writable after a refusal, and a later whole-second write publishes.
    String later = "(9, TIMESTAMP '2024-03-01 00:00:00')";
    spark.sql("INSERT INTO " + iceberg + " VALUES " + later);
    try {
      spark.sql("INSERT INTO " + hudi + " VALUES " + later);
      List<String> icebergRows = read("iceberg", hudiPath);
      List<String> hudiRows = read("hudi", hudiPath);
      System.out.printf(
          "[SUBSEC] %s | after a later whole-second INSERT | published via Iceberg=%s |"
              + " published via Hudi=%s%n",
          name, icebergRows, hudiRows);
      softly
          .assertThat(icebergRows)
          .as(name + ": later write, Iceberg vs Hudi")
          .isEqualTo(hudiRows);
      softly
          .assertThat(icebergRows)
          .as(name + ": later write landed")
          .contains("9|2024-03-01 00:00:00.0");
    } catch (Exception e) {
      softly.fail(name + ": a later whole-second write failed: " + rootMessage(e));
    }
    softly.assertAll();
  }

  private static String nativeLocation(String table) {
    try {
      return org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, table).location();
    } catch (Exception e) {
      throw new IllegalStateException(e);
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
                        || f.getClassName().startsWith("org.apache.hudi."))
            .limit(3)
            .map(f -> f.getClassName() + "." + f.getMethodName() + ":" + f.getLineNumber())
            .collect(Collectors.joining(" < "));
    return c.getClass().getSimpleName() + ": " + c.getMessage() + " @ " + frames;
  }
}
