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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * With Hudi's default Avro writer settings, lists use the legacy two-level Parquet layout, which
 * cannot hold null elements. This checks whether writing the standard three-level layout ({@code
 * parquet.avro.write-old-list-structure=false}) makes arrays with null elements round-trip through
 * a published table, read by both Hudi and Iceberg. Prints PROBE lines.
 */
class TestListLayoutProbe {

  private static SparkSession spark;
  private static File warehouse;

  @BeforeAll
  static void startSpark() throws IOException {
    warehouse = Files.createTempDirectory("ptf-list-layout").toFile();
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
            .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static List<String> rows(Dataset<Row> df) {
    String[] meta =
        Arrays.stream(df.columns()).filter(c -> c.startsWith("_hoodie_")).toArray(String[]::new);
    return df.drop(meta).collectAsList().stream()
        .map(Row::toString)
        .sorted()
        .collect(Collectors.toList());
  }

  private static void print(String probe, String what, Object value) {
    System.out.println("PROBE " + probe + " " + what + " = " + value);
  }

  @ParameterizedTest
  @ValueSource(strings = {"cow:legacy", "cow:standard", "mor:legacy", "mor:standard"})
  void arrayWithNullElements(String variant) {
    String type = variant.split(":")[0];
    boolean standard = variant.endsWith("standard");
    String key = "parquet.avro.write-old-list-structure";
    spark.sparkContext().hadoopConfiguration().set(key, standard ? "false" : "true");
    String name = "lists_" + type + "_" + UUID.randomUUID().toString().substring(0, 6);
    String path = new File(warehouse, name).toURI().toString();
    try {
      spark.sql(
          String.format(
              "CREATE TABLE default.%s (id INT, arr ARRAY<INT>, names ARRAY<STRING>) USING hudi"
                  + " LOCATION '%s' TBLPROPERTIES ('type' '%s', 'primaryKey' 'id',"
                  + " 'hoodie.table.format' 'ICEBERG')",
              name, path, type));
      run(
          variant,
          "insert",
          "INSERT INTO default."
              + name
              + " VALUES (1, array(1, null, 3), array('a', null)),"
              + " (2, array(), array()), (3, null, null)");
      run(variant, "update", "UPDATE default." + name + " SET arr = array(null, 7) WHERE id = 1");
      print(variant, "hudi rows", rows(spark.read().format("hudi").load(path)));
      print(variant, "iceberg rows", rows(spark.read().format("iceberg").load(path)));
      print(variant, "iceberg files", spark.read().format("iceberg").load(path + "#files").count());
    } catch (Exception e) {
      Throwable root = e;
      while (root.getCause() != null) {
        root = root.getCause();
      }
      print(variant, "FAILED", root.getClass().getSimpleName() + ": " + root.getMessage());
    } finally {
      spark.sparkContext().hadoopConfiguration().unset(key);
    }
  }

  private static void run(String variant, String step, String sql) {
    spark.sql(sql);
    print(variant, step, "ok");
  }
}
