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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

/** Probes that pin down two parity differences case by case; each prints PROBE lines. */
class TestTriageProbes {

  private static SparkSession spark;
  private static File warehouse;

  @BeforeAll
  static void startSpark() throws IOException {
    warehouse = Files.createTempDirectory("ptf-triage-probes").toFile();
    spark =
        SparkSession.builder()
            .master("local[2]")
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

  private static String hudiTable(String name, String columns, String partition, boolean publish) {
    String path = new File(warehouse, name).toURI().toString();
    spark.sql(
        String.format(
            "CREATE TABLE default.%s (%s) USING hudi PARTITIONED BY (%s) LOCATION '%s'"
                + " TBLPROPERTIES ('type' 'cow'%s)",
            name, columns, partition, path, publish ? ", 'hoodie.table.format' 'ICEBERG'" : ""));
    return path;
  }

  private static List<String> rows(Dataset<Row> df) {
    String[] meta =
        Arrays.stream(df.columns()).filter(c -> c.startsWith("_hoodie_")).toArray(String[]::new);
    return df.drop(meta).collectAsList().stream()
        .map(
            r ->
                r.mkString("|").replace("\u0000", "\\0")
                    + (r.isNullAt(r.length() - 1) ? "<null>" : ""))
        .sorted()
        .collect(Collectors.toList());
  }

  private static void print(String probe, String what, Object value) {
    System.out.println("PROBE " + probe + " " + what + " = " + value);
  }

  @Test
  void emptyStringPartitionValue() {
    String suffix = UUID.randomUUID().toString().substring(0, 6);
    String published = hudiTable("pub_" + suffix, "id INT, p STRING", "p", true);
    String plain = hudiTable("plain_" + suffix, "id INT, p STRING", "p", false);
    spark.sql(
        "CREATE TABLE ice.db.nat_"
            + suffix
            + " (id INT, p STRING) USING iceberg PARTITIONED BY (p)");
    for (String t :
        Arrays.asList("default.pub_" + suffix, "default.plain_" + suffix, "ice.db.nat_" + suffix)) {
      spark.sql("INSERT INTO " + t + " VALUES (1, ''), (2, 'x')");
    }
    print("empty", "native iceberg rows", rows(spark.table("ice.db.nat_" + suffix)));
    print(
        "empty",
        "native iceberg WHERE p=''",
        rows(spark.sql("SELECT * FROM ice.db.nat_" + suffix + " WHERE p = ''")));
    print("empty", "plain hudi rows", rows(spark.read().format("hudi").load(plain)));
    print(
        "empty",
        "plain hudi WHERE p=''",
        rows(spark.sql("SELECT * FROM default.plain_" + suffix + " WHERE p = ''")));
    print(
        "empty",
        "plain hudi partition dirs",
        Arrays.toString(new File(java.net.URI.create(plain)).list((d, n) -> !n.startsWith("."))));
    print("empty", "published hudi-read rows", rows(spark.read().format("hudi").load(published)));
    print(
        "empty",
        "published iceberg-read rows",
        rows(spark.read().format("iceberg").load(published)));
    print(
        "empty",
        "published iceberg partitions",
        rows(spark.read().format("iceberg").load(published + "#files").select("partition")));
  }

  @Test
  void updateWithoutConditionFileCounts() {
    for (String limit : Arrays.asList("default", "0")) {
      String suffix = UUID.randomUUID().toString().substring(0, 6);
      if (!limit.equals("default")) {
        spark.conf().set("hoodie.parquet.small.file.limit", limit);
      }
      try {
        String path = hudiTable("upd_" + suffix, "id INT, dep STRING", "dep", true);
        String t = "default.upd_" + suffix;
        spark.sql("INSERT INTO " + t + " VALUES (1, 'hr')");
        spark.sql("INSERT INTO " + t + " VALUES (2, 'hardware')");
        spark.sql("INSERT INTO " + t + " VALUES (null, 'hr')");
        Table table = new HadoopTables(spark.sessionState().newHadoopConf()).load(path);
        print(
            "files[" + limit + "]",
            "live data files before update",
            table.currentSnapshot().summary().get("total-data-files"));
        print("files[" + limit + "]", "insert operations", ops(table));
        spark.sql("UPDATE " + t + " SET id = -1");
        table.refresh();
        Snapshot s = table.currentSnapshot();
        print(
            "files[" + limit + "]",
            "update op/deleted/added",
            s.operation()
                + "/"
                + s.summary().get("deleted-data-files")
                + "/"
                + s.summary().get("added-data-files"));
        print(
            "files[" + limit + "]",
            "rows after update",
            rows(spark.read().format("iceberg").load(path)));
      } finally {
        spark.conf().unset("hoodie.parquet.small.file.limit");
      }
    }
  }

  private static String ops(Table table) {
    StringBuilder sb = new StringBuilder();
    for (Snapshot s = table.currentSnapshot();
        s != null;
        s = s.parentId() == null ? null : table.snapshot(s.parentId())) {
      sb.insert(0, s.operation() + " ");
    }
    return sb.toString().trim();
  }
}
