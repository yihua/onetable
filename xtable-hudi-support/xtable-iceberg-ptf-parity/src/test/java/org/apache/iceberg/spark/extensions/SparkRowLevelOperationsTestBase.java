/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.DataOperations.DELETE;
import static org.apache.iceberg.DataOperations.OVERWRITE;
import static org.apache.iceberg.PlanningMode.DISTRIBUTED;
import static org.apache.iceberg.PlanningMode.LOCAL;
import static org.apache.iceberg.SnapshotSummary.ADDED_DELETE_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.ADDED_DVS_PROP;
import static org.apache.iceberg.SnapshotSummary.ADDED_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.CHANGED_PARTITION_COUNT_PROP;
import static org.apache.iceberg.SnapshotSummary.DELETED_FILES_PROP;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_HASH;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_NONE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkTestHelperBase;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.FileSourceScanExec;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Stands in for Iceberg's base of the Spark row-level operation suites. The table under test is a
 * Hudi table that the pluggable table format publishes to Iceberg: Hudi executes every statement
 * that writes it, and every query that reads it goes through Iceberg, so the expected rows of the
 * Iceberg suites check what an Iceberg reader sees.
 *
 * <p>By default the Hudi table is keyless. {@code -Dparity.primaryKey=<column>} gives every table
 * that has that column a Hudi primary key on it, with no ordering field, so merge-on-read keeps the
 * commit-time ordering that the update-as-delete-insert mode requires. The Iceberg suites sometimes
 * insert duplicate or null values into that column; with a key Hudi upserts or rejects those rows,
 * so such cases differ from Iceberg because of key semantics alone. {@code
 * -Dparity.mergePartialUpdates=<true|false>} sets {@code hoodie.spark.sql.merge.into.partial.updates}
 * in the session, which decides whether a MERGE that assigns some columns writes partial updates.
 */
@ExtendWith(ParameterizedTestExtension.class)
public abstract class SparkRowLevelOperationsTestBase extends SparkTestHelperBase {

  private static final String READ_VIEW = "ptf_iceberg_read";

  /**
   * Control mode: the same Hudi tables without the published format, read through Hudi. A case
   * that fails the same way here fails because of Hudi SQL, not the pluggable table format.
   */
  private static final boolean HUDI_ONLY = Boolean.getBoolean("parity.hudiOnly");

  /** Lets the Hudi-only control run merge-on-read without the update-as-delete-insert mode. */
  private static final boolean UPDATES_AS_DELETES =
      Boolean.parseBoolean(System.getProperty("parity.updatesAsDeletesAndInserts", "true"));

  /** The column that becomes the Hudi primary key of a created table that has it; empty for none. */
  private static final String PRIMARY_KEY = System.getProperty("parity.primaryKey", "").trim();

  /**
   * {@code hoodie.index.type} of the session and of a merge-on-read table in the update-as-delete-insert
   * mode; {@code RECORD_INDEX} also enables the global record level index in the metadata table.
   */
  private static final String INDEX_TYPE = System.getProperty("parity.indexType", "SIMPLE").trim();

  /** {@code hoodie.spark.sql.merge.into.partial.updates} for the session, when given. */
  private static final String MERGE_PARTIAL_UPDATES = System.getProperty("parity.mergePartialUpdates");
  private static final String APPEND_VIEW = "ptf_append_source";

  private static final String WRITE_DISTRIBUTION =
      "not applicable: asserts the shuffle that Iceberg's writer requests through its write"
          + " distribution mode and advisory partition size; Hudi plans its own write";
  private static final String WAP_BRANCH =
      "not applicable: Iceberg branches and write-audit-publish; a Hudi table has no branches";
  private static final String PARTITION_EVOLUTION =
      "product gap H10: partition evolution and partition transforms (ADD PARTITION FIELD on a"
          + " table with data, bucket/days/truncate); Hudi partitions by identity only. Size:"
          + " large, a spec-evolution model in Hudi plus transform-aware partition paths";

  /**
   * Cases that test an Iceberg feature the published table cannot have, or a known product gap,
   * skipped with the reason so a run reports them apart from failures. Each reason starts with
   * "not applicable" or "product gap".
   */
  private static final Map<String, String> SKIPPED_CASES =
      ImmutableMap.<String, String>builder()
          .put("testCoalesceDelete", WRITE_DISTRIBUTION)
          .put("testSkewDelete", WRITE_DISTRIBUTION)
          .put("testCoalesceUpdate", WRITE_DISTRIBUTION)
          .put("testSkewUpdate", WRITE_DISTRIBUTION)
          .put("testSkewMerge", WRITE_DISTRIBUTION)
          .put(
              "testMergeConditionSplitIntoTargetPredicateAndJoinCondition",
              "not applicable: asserts the join type and target filter of Iceberg's MERGE rewrite"
                  + " (RewriteMergeIntoTable); Hudi runs MERGE in its own command")
          .put(
              "testMergeWithVectorizedReads",
              "not applicable: checks the scans in the executed plan of the MERGE; Hudi's MERGE"
                  + " command plans and runs its join inside the command, so its executed plan"
                  + " holds no scan")
          .put("testDeleteToWapBranch", WAP_BRANCH)
          .put("testDeleteToWapBranchCanDeleteWhereScansWapBranch", WAP_BRANCH)
          .put("testMetadataDeleteToWapBranchCommitsToWapBranch", WAP_BRANCH)
          .put("testUpdateToWAPBranch", WAP_BRANCH)
          .put("testMergeToWapBranch", WAP_BRANCH)
          .put(
              "testDeleteWithPartitionedTable",
              "not applicable: reads the Iceberg metadata table <table>.partitions through the"
                  + " session catalog, where the table is a Hudi table; the published metadata"
                  + " tables are checked through an Iceberg catalog in TestIcebergReadParity")
          .put(
              "testDeleteFileThenMetadataDelete",
              "not applicable: loads the table through Iceberg's Spark catalog"
                  + " (Spark3Util.loadIcebergTable), where it is a Hudi table")
          .put(
              "testDeleteWithoutScanningTable",
              "product gap: a DELETE whose condition selects whole partitions is not run as a"
                  + " metadata-only delete (Iceberg's DeleteFromTableWithFilters); Hudi scans the"
                  + " rows. Size: medium, plan such a DELETE as a Hudi delete_partition")
          .put("testDeleteWithMultipleSpecs", PARTITION_EVOLUTION)
          .put("testMergeWithBucketTransform", PARTITION_EVOLUTION)
          .put("testMergeWithDaysTransform", PARTITION_EVOLUTION)
          .put("testMergeWithTruncateTransform", PARTITION_EVOLUTION)
          .put("testUpdateModifyPartitionSourceField", PARTITION_EVOLUTION)
          .put("testUpdateWithAlias", PARTITION_EVOLUTION)
          .put("testUpdateWithUserDefinedDistribution", PARTITION_EVOLUTION)
          .build();

  private static final String MERGE_ON_READ_ROW_READS =
      "product gap: Hudi reads a merge-on-read table row by row, since its file group reader"
          + " merges the log records into the rows of the base file; the scan never returns"
          + " columnar batches (HoodieFileGroupReaderBasedFileFormat.supportBatch). Size: large, a"
          + " columnar merge in the file group reader";

  /** Cases skipped as {@link #SKIPPED_CASES} are, only in the merge-on-read suites. */
  private static final Map<String, String> MERGE_ON_READ_SKIPPED_CASES =
      ImmutableMap.of(
          "testDeleteWithVectorizedReads", MERGE_ON_READ_ROW_READS,
          "testUpdateWithVectorizedReads", MERGE_ON_READ_ROW_READS);
  private static final Pattern CREATE_ICEBERG_TABLE =
      Pattern.compile("(?is)^\\s*CREATE\\s+TABLE\\s+(\\S+)\\s*\\((.*)\\)\\s*USING\\s+iceberg\\b(.*)$");
  private static final Pattern TBLPROPERTIES = Pattern.compile("(?i)TBLPROPERTIES\\s*\\(");

  protected static SparkSession spark = null;
  protected static JavaSparkContext sparkContext = null;
  protected static File warehouse = null;

  @TempDir protected Path temp;

  @Parameter(index = 0)
  protected String catalogName;

  @Parameter(index = 1)
  protected String implementation;

  @Parameter(index = 2)
  protected Map<String, String> catalogConfig;

  @Parameter(index = 3)
  protected FileFormat fileFormat;

  @Parameter(index = 4)
  protected boolean vectorized;

  @Parameter(index = 5)
  protected String distributionMode;

  @Parameter(index = 6)
  protected boolean fanoutEnabled;

  @Parameter(index = 7)
  protected String branch;

  @Parameter(index = 8)
  protected PlanningMode planningMode;

  @Parameter(index = 9)
  protected int formatVersion;

  protected Catalog validationCatalog;
  protected TableIdentifier tableIdent = TableIdentifier.of(Namespace.of("default"), "table");
  protected String tableName;
  protected String tablePath;
  private String currentTest;

  /**
   * Hudi has no branches, so every case runs on main. Merge-on-read is published with deletion
   * vectors, which need format version 3; the version 2 case is skipped for it.
   */
  @Parameters(
      name =
          "catalogName = {0}, implementation = {1}, config = {2},"
              + " format = {3}, vectorized = {4}, distributionMode = {5},"
              + " fanout = {6}, branch = {7}, planningMode = {8}, formatVersion = {9}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        ImmutableMap.of(),
        FileFormat.PARQUET,
        true,
        WRITE_DISTRIBUTION_MODE_NONE,
        false,
        null,
        LOCAL,
        3
      },
      {
        "spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        ImmutableMap.of(),
        FileFormat.PARQUET,
        false,
        WRITE_DISTRIBUTION_MODE_HASH,
        false,
        null,
        DISTRIBUTED,
        2
      },
    };
  }

  @BeforeAll
  public static void startSpark() throws IOException {
    if (spark != null) {
      return;
    }
    warehouse = Files.createTempDirectory("ptf-parity-warehouse").toFile();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.testing", "true")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath())
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
            .config(
                "spark.sql.extensions",
                IcebergSparkSessionExtensions.class.getName()
                    + ",org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.default_iceberg.type", "hadoop")
            .config(
                "spark.sql.catalog.default_iceberg.warehouse",
                new File(warehouse, "default_iceberg").getAbsolutePath())
            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
            .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();
    sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @BeforeEach
  public void before(TestInfo testInfo) {
    currentTest =
        testInfo.getTestClass().map(Class::getSimpleName).orElse("?")
            + "."
            + testInfo.getTestMethod().map(m -> m.getName()).orElse("?")
            + "[v"
            + formatVersion
            + "]";
    String skipReason = testInfo.getTestMethod().map(m -> SKIPPED_CASES.get(m.getName())).orElse(null);
    Assumptions.assumeTrue(skipReason == null, skipReason);
    if (isMergeOnRead()) {
      String morSkipReason =
          testInfo.getTestMethod().map(m -> MERGE_ON_READ_SKIPPED_CASES.get(m.getName())).orElse(null);
      Assumptions.assumeTrue(morSkipReason == null, morSkipReason);
      Assumptions.assumeTrue(
          formatVersion >= 3,
          "the pluggable table format publishes merge-on-read only at format version 3");
    }
    tableName = "default.table";
    tablePath = new File(warehouse, "table_" + UUID.randomUUID()).toURI().toString();
    validationCatalog = new PathCatalog();
    spark.conf().set("hoodie.write.updates.as.deletes.and.inserts", String.valueOf(isMergeOnRead() && UPDATES_AS_DELETES));
    spark.conf().set("hoodie.index.type", INDEX_TYPE);
    spark.conf().set("hoodie.metadata.global.record.level.index.enable", String.valueOf(INDEX_TYPE.equals("RECORD_INDEX")));
    if (MERGE_PARTIAL_UPDATES != null) {
      spark.conf().set("hoodie.spark.sql.merge.into.partial.updates", MERGE_PARTIAL_UPDATES);
    }
  }

  /**
   * Gates the concurrent isolation cases, which race Iceberg API commits against the writer. They
   * are a separate pass: the table under test has no Iceberg writer of its own.
   */
  protected boolean cachingCatalogEnabled() {
    return Boolean.getBoolean("parity.concurrent");
  }

  protected String tableName(String name) {
    return "default." + name;
  }

  protected abstract Map<String, String> extraTableProperties();

  protected boolean isMergeOnRead() {
    return extraTableProperties().containsValue(RowLevelOperationMode.MERGE_ON_READ.modeName());
  }

  /** Iceberg sets its write properties here; the Hudi table takes its settings at creation. */
  protected void initTable() {}

  protected void createAndInitTable(String schema) {
    createAndInitTable(schema, null);
  }

  protected void createAndInitTable(String schema, String jsonData) {
    createAndInitTable(schema, "", jsonData);
  }

  protected void createAndInitTable(String schema, String partitioning, String jsonData) {
    sql("CREATE TABLE %s (%s) USING iceberg %s", tableName, schema, partitioning);
    initTable();

    if (jsonData != null) {
      append(tableName, schema, jsonData);
      createBranchIfNeeded();
    }
  }

  protected void append(String table, String jsonData) {
    append(table, null, jsonData);
  }

  protected void append(String table, String schema, String jsonData) {
    Dataset<Row> ds = toDS(schema, jsonData);
    if (isTableUnderTest(table)) {
      appendToTableUnderTest(ds);
    } else {
      try {
        ds.coalesce(1).writeTo(table).append();
      } catch (Exception e) {
        throw new RuntimeException("Failed to write data", e);
      }
    }
  }

  protected void append(String table, String... jsonRecords) {
    String schema = spark.table(table).schema().toDDL();
    Dataset<Row> df = jsonToDF(schema, jsonRecords);
    if (isTableUnderTest(table)) {
      appendToTableUnderTest(df);
    } else {
      try {
        df.coalesce(1).writeTo(table).append();
      } catch (Exception e) {
        throw new RuntimeException("Failed to write data", e);
      }
    }
  }

  /**
   * Replaces {@code df.writeTo(target).append()} in the copied suites: Hudi does not take
   * DataFrameWriterV2 appends, so rows for the table under test go through Hudi SQL.
   */
  protected <T> Dataset<T> appendTo(String target, Dataset<T> df) {
    if (isTableUnderTest(target)) {
      appendToTableUnderTest(df.toDF());
    } else {
      try {
        df.writeTo(target).append();
      } catch (Exception e) {
        throw new RuntimeException("Failed to write data", e);
      }
    }
    return df;
  }

  private boolean isTableUnderTest(String table) {
    return table.equals(tableName) || table.equals(commitTarget());
  }

  /** Inserts through Hudi SQL, aligning the rows to the table's columns by name. */
  private void appendToTableUnderTest(Dataset<Row> ds) {
    tableHasData = true;
    StructType tableSchema = spark.table(tableName).schema();
    Set<String> provided = Arrays.stream(ds.columns()).collect(Collectors.toSet());
    List<Column> columns = new ArrayList<>();
    for (StructField field : tableSchema.fields()) {
      if (HoodieRecord.HOODIE_META_COLUMNS.contains(field.name())) {
        continue;
      }
      Column column =
          provided.contains(field.name()) ? functions.col(field.name()) : functions.lit(null);
      columns.add(column.cast(field.dataType()).as(field.name()));
    }
    ds.coalesce(1).select(columns.toArray(new Column[0])).createOrReplaceTempView(APPEND_VIEW);
    spark.sql(String.format("INSERT INTO %s SELECT * FROM %s", tableName, APPEND_VIEW));
  }

  protected void createOrReplaceView(String name, String jsonData) {
    createOrReplaceView(name, null, jsonData);
  }

  /**
   * Spark infers BIGINT for JSON integers; Hudi's MERGE refuses a BIGINT source key against an INT
   * target where Iceberg casts it. Narrowing the inferred type keeps the rest of each case running;
   * the refusal itself is reported separately.
   */
  protected void createOrReplaceView(String name, String schema, String jsonData) {
    Dataset<Row> ds = toDS(schema, jsonData);
    if (schema == null && !Boolean.getBoolean("parity.keepInferredBigint")) {
      for (StructField field : ds.schema().fields()) {
        if (field.dataType().equals(org.apache.spark.sql.types.DataTypes.LongType)) {
          ds = ds.withColumn(field.name(), functions.col(field.name()).cast("int"));
        }
      }
    }
    ds.createOrReplaceTempView(name);
  }

  protected <T> void createOrReplaceView(String name, List<T> data, Encoder<T> encoder) {
    spark.createDataset(data, encoder).createOrReplaceTempView(name);
  }

  private Dataset<Row> toDS(String schema, String jsonData) {
    List<String> jsonRows =
        Arrays.stream(jsonData.split("\n"))
            .filter(str -> !str.trim().isEmpty())
            .collect(Collectors.toList());
    Dataset<String> jsonDS = spark.createDataset(jsonRows, Encoders.STRING());

    if (schema != null) {
      return spark.read().schema(schema).json(jsonDS);
    } else {
      return spark.read().json(jsonDS);
    }
  }

  private static final Pattern ADD_IDENTITY_PARTITION_FIELD =
      Pattern.compile("(?i)^\\s*ALTER\\s+TABLE\\s+(\\S+)\\s+ADD\\s+PARTITION\\s+FIELD\\s+(?:identity\\((\\w+)\\)|(\\w+))\\s*$");
  private static final Pattern WRITE_DISTRIBUTION_OR_ORDER =
      Pattern.compile("(?i)^\\s*ALTER\\s+TABLE\\s+(\\S+)\\s+WRITE\\s+.*$");
  private static final Pattern WRITES_TABLE =
      Pattern.compile("(?i)^\\s*(INSERT|UPDATE|DELETE|MERGE)\\b.*");

  private String lastCreateColumns;
  private boolean tableHasData;

  protected List<Object[]> sql(String query, Object... args) {
    String formatted = String.format(query, args);
    if (adaptsIcebergOnlyDdl(formatted)) {
      return ImmutableList.of();
    }
    if (WRITES_TABLE.matcher(formatted).matches() && mentionsTableUnderTest(formatted)) {
      tableHasData = true;
    }
    String statement = toHudiDdl(formatted);
    if (isQuery(statement) && mentionsTableUnderTest(statement)) {
      statement = readThroughIceberg(statement);
    }
    Dataset<Row> result = spark.sql(statement);
    if (isQuery(statement)) {
      result = withoutHudiMetaColumns(result);
    }
    List<Row> rows = result.collectAsList();
    if (rows.isEmpty()) {
      return ImmutableList.of();
    }
    return rowsToJava(rows);
  }

  protected Object scalarSql(String query, Object... args) {
    List<Object[]> rows = sql(query, args);
    assertThat(rows).as("Scalar SQL should return one row").hasSize(1);
    Object[] row = Iterables.getOnlyElement(rows);
    assertThat(row).as("Scalar SQL should return one value").hasSize(1);
    return row[0];
  }

  protected Object[] row(Object... values) {
    return values;
  }

  private static final Pattern WRITE_KEYWORD =
      Pattern.compile("(?i)\\b(MERGE\\s+INTO|INSERT\\s+(INTO|OVERWRITE)|UPDATE|DELETE\\s+FROM)\\b");

  /** A SELECT, or a WITH that ends in one; a WITH that feeds a MERGE or other write is not a read. */
  private static boolean isQuery(String statement) {
    String head = statement.trim().toUpperCase();
    return head.startsWith("SELECT")
        || (head.startsWith("WITH") && !WRITE_KEYWORD.matcher(statement).find());
  }

  private boolean mentionsTableUnderTest(String statement) {
    return tableNamePattern().matcher(statement).find();
  }

  private Pattern tableNamePattern() {
    return Pattern.compile("(?<![\\w.`])" + Pattern.quote(tableName) + "(?![\\w])");
  }

  /**
   * A query that reads the table by name through Spark, as a view the suites define over it does,
   * reads it through Hudi, which adds its meta columns; the rows are compared on the table's own
   * columns.
   */
  private static Dataset<Row> withoutHudiMetaColumns(Dataset<Row> df) {
    return df.drop(
        Arrays.stream(df.columns())
            .filter(HoodieRecord.HOODIE_META_COLUMNS::contains)
            .toArray(String[]::new));
  }

  /** Points the query at a fresh Iceberg read of the table, without Hudi's meta columns. */
  private String readThroughIceberg(String statement) {
    if (HUDI_ONLY) {
      Dataset<Row> hudi = spark.read().format("hudi").load(tablePath);
      hudi.drop(
              Arrays.stream(hudi.columns())
                  .filter(HoodieRecord.HOODIE_META_COLUMNS::contains)
                  .toArray(String[]::new))
          .createOrReplaceTempView(READ_VIEW);
      return tableNamePattern().matcher(statement).replaceAll(READ_VIEW);
    }
    Dataset<Row> df = spark.read().format("iceberg").load(tablePath);
    String[] metaColumns =
        Arrays.stream(df.columns())
            .filter(HoodieRecord.HOODIE_META_COLUMNS::contains)
            .toArray(String[]::new);
    Dataset<Row> icebergRows = df.drop(metaColumns);
    icebergRows.createOrReplaceTempView(READ_VIEW);
    compareWithHudiRead(icebergRows);
    return tableNamePattern().matcher(statement).replaceAll(READ_VIEW);
  }

  /** Records any difference between what Iceberg and Hudi read from the same table. */
  private void compareWithHudiRead(Dataset<Row> icebergRows) {
    Dataset<Row> hudi = spark.read().format("hudi").load(tablePath);
    String[] hudiMeta =
        Arrays.stream(hudi.columns()).filter(HoodieRecord.HOODIE_META_COLUMNS::contains).toArray(String[]::new);
    List<String> fromIceberg = sortedRows(icebergRows);
    List<String> fromHudi = sortedRows(hudi.drop(hudiMeta).select(icebergRows.columns()[0], Arrays.copyOfRange(icebergRows.columns(), 1, icebergRows.columns().length)));
    if (!fromIceberg.equals(fromHudi)) {
      String report = System.getProperty("parity.readdiff.report", "target/parity-iceberg-vs-hudi.tsv");
      try (PrintWriter out = new PrintWriter(new FileWriter(report, true))) {
        out.println(currentTest + "\ticeberg=" + fromIceberg + "\thudi=" + fromHudi);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private static List<String> sortedRows(Dataset<Row> df) {
    return df.collectAsList().stream().map(r -> r.mkString("|")).sorted().collect(Collectors.toList());
  }

  /**
   * The Iceberg suites partition a freshly created, still empty table with ADD PARTITION FIELD and
   * tune Iceberg's own write layout with WRITE DISTRIBUTED/ORDERED BY. Neither changes what the
   * statements under test must return, so an identity partition field on an empty table recreates
   * it partitioned, and a write layout clause is ignored. Partition evolution of a table that holds
   * data, or with a transform, still reaches Hudi and fails as unsupported.
   */
  private boolean adaptsIcebergOnlyDdl(String statement) {
    Matcher partition = ADD_IDENTITY_PARTITION_FIELD.matcher(statement);
    if (partition.matches() && partition.group(1).equals(tableName) && !tableHasData && lastCreateColumns != null) {
      String column = partition.group(2) != null ? partition.group(2) : partition.group(3);
      spark.sql("DROP TABLE IF EXISTS " + tableName);
      tablePath = new File(warehouse, "table_" + UUID.randomUUID()).toURI().toString();
      sql("CREATE TABLE %s (%s) USING iceberg PARTITIONED BY (%s)", tableName, lastCreateColumns, column);
      return true;
    }
    Matcher layout = WRITE_DISTRIBUTION_OR_ORDER.matcher(statement);
    return layout.matches() && layout.group(1).equals(tableName);
  }

  /** Turns a CREATE TABLE of the table under test into a Hudi table published to Iceberg. */
  private String toHudiDdl(String statement) {
    Matcher matcher = CREATE_ICEBERG_TABLE.matcher(statement);
    if (!matcher.matches() || !matcher.group(1).equals(tableName)) {
      return statement;
    }
    lastCreateColumns = matcher.group(2);
    tableHasData = false;
    Map<String, String> properties = new HashMap<>();
    properties.put("type", isMergeOnRead() ? "mor" : "cow");
    if (!HUDI_ONLY) {
      properties.put("hoodie.table.format", "ICEBERG");
      properties.put("hoodie.table.format.iceberg.format-version", String.valueOf(formatVersion));
    }
    properties.put("hoodie.table.base.file.format", fileFormat.name());
    if (!PRIMARY_KEY.isEmpty() && hasTopLevelColumn(matcher.group(2), PRIMARY_KEY)) {
      properties.put("primaryKey", PRIMARY_KEY);
    }
    if (isMergeOnRead() && UPDATES_AS_DELETES) {
      properties.put("hoodie.write.updates.as.deletes.and.inserts", "true");
      properties.put("hoodie.index.type", INDEX_TYPE);
      if (INDEX_TYPE.equals("RECORD_INDEX")) {
        properties.put("hoodie.metadata.global.record.level.index.enable", "true");
      }
    }
    String hudiProperties = tablePropsAsString(properties);
    String clauses = matcher.group(3);
    Matcher existing = TBLPROPERTIES.matcher(clauses);
    if (existing.find()) {
      clauses =
          clauses.substring(0, existing.end()) + hudiProperties + ", " + clauses.substring(existing.end());
    } else {
      clauses = clauses + " TBLPROPERTIES (" + hudiProperties + ")";
    }
    return String.format(
        "CREATE TABLE %s (%s) USING hudi %s LOCATION '%s'",
        tableName, matcher.group(2), clauses, tablePath);
  }

  /** Whether a column list of a CREATE TABLE declares the column at the top level. */
  private static boolean hasTopLevelColumn(String columns, String column) {
    int depth = 0;
    int start = 0;
    for (int i = 0; i <= columns.length(); i++) {
      char c = i < columns.length() ? columns.charAt(i) : ',';
      if (c == '<' || c == '(') {
        depth++;
      } else if (c == '>' || c == ')') {
        depth--;
      } else if (c == ',' && depth == 0) {
        String[] parts = columns.substring(start, i).trim().split("\\s+", 2);
        if (parts[0].replace("`", "").equalsIgnoreCase(column)) {
          return true;
        }
        start = i + 1;
      }
    }
    return false;
  }

  protected String tablePropsAsString(Map<String, String> tableProps) {
    StringBuilder stringBuilder = new StringBuilder();
    for (Map.Entry<String, String> property : tableProps.entrySet()) {
      if (stringBuilder.length() > 0) {
        stringBuilder.append(", ");
      }
      stringBuilder.append(String.format("'%s' '%s'", property.getKey(), property.getValue()));
    }
    return stringBuilder.toString();
  }

  protected void validateDelete(
      Snapshot snapshot, String changedPartitionCount, String deletedDataFiles) {
    validateSnapshot(snapshot, DELETE, changedPartitionCount, deletedDataFiles, null, null);
  }

  protected void validateCopyOnWrite(
      Snapshot snapshot,
      String changedPartitionCount,
      String deletedDataFiles,
      String addedDataFiles) {
    String operation = null == addedDataFiles && null != deletedDataFiles ? DELETE : OVERWRITE;
    validateSnapshot(
        snapshot, operation, changedPartitionCount, deletedDataFiles, null, addedDataFiles);
  }

  protected void validateMergeOnRead(
      Snapshot snapshot,
      String changedPartitionCount,
      String addedDeleteFiles,
      String addedDataFiles) {
    String operation = null == addedDataFiles && null != addedDeleteFiles ? DELETE : OVERWRITE;
    validateSnapshot(
        snapshot, operation, changedPartitionCount, null, addedDeleteFiles, addedDataFiles);
  }

  /**
   * The operation is part of the contract Iceberg readers rely on, so it is asserted. File counts
   * depend on how the writer lays out files, so differences are recorded rather than failed.
   */
  protected void validateSnapshot(
      Snapshot snapshot,
      String operation,
      String changedPartitionCount,
      String deletedDataFiles,
      String addedDeleteFiles,
      String addedDataFiles) {
    assertThat(snapshot.operation()).as("Operation must match").isEqualTo(operation);
    validateProperty(snapshot, CHANGED_PARTITION_COUNT_PROP, changedPartitionCount);
    validateProperty(snapshot, DELETED_FILES_PROP, deletedDataFiles);
    validateProperty(snapshot, ADDED_DELETE_FILES_PROP, addedDeleteFiles);
    validateProperty(snapshot, ADDED_FILES_PROP, addedDataFiles);
    if (formatVersion >= 3) {
      validateProperty(snapshot, ADDED_DVS_PROP, addedDeleteFiles);
    }
  }

  protected void validateProperty(Snapshot snapshot, String property, Set<String> expectedValues) {
    String actual = snapshot.summary().get(property);
    if (!expectedValues.contains(actual)) {
      recordSummaryDifference(property, String.join("|", expectedValues), actual);
    }
  }

  protected void validateProperty(Snapshot snapshot, String property, String expectedValue) {
    String actual = snapshot.summary().get(property);
    if (expectedValue == null ? actual != null : !expectedValue.equals(actual)) {
      recordSummaryDifference(property, expectedValue, actual);
    }
  }

  private void recordSummaryDifference(String property, String expected, String actual) {
    String report =
        System.getProperty("parity.summary.report", "target/parity-summary-differences.tsv");
    try (PrintWriter out = new PrintWriter(new FileWriter(report, true))) {
      out.println(currentTest + "\t" + property + "\t" + expected + "\t" + actual);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  protected DataFile writeDataFile(Table table, List<GenericRecord> records) {
    try {
      OutputFile file =
          org.apache.iceberg.Files.localOutput(
              temp.resolve(fileFormat.addExtension(UUID.randomUUID().toString())).toFile());

      DataWriter<GenericRecord> dataWriter =
          Parquet.writeData(file)
              .forTable(table)
              .createWriterFunc(GenericParquetWriter::create)
              .overwrite()
              .build();

      try {
        for (GenericRecord record : records) {
          dataWriter.write(record);
        }
      } finally {
        dataWriter.close();
      }

      return dataWriter.toDataFile();

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected String commitTarget() {
    return branch == null ? tableName : String.format("%s.branch_%s", tableName, branch);
  }

  protected String selectTarget() {
    return branch == null ? tableName : String.format("%s VERSION AS OF '%s'", tableName, branch);
  }

  protected void createBranchIfNeeded() {
    if (branch != null && !branch.equals(SnapshotRef.MAIN_BRANCH)) {
      sql("ALTER TABLE %s CREATE BRANCH %s", tableName, branch);
    }
  }

  protected boolean supportsVectorization() {
    return vectorized && (fileFormat.equals(FileFormat.PARQUET) || !isMergeOnRead());
  }

  /**
   * Hudi reads the table of a row-level operation through a file source scan rather than a data
   * source v2 batch scan, so both kinds of scan must read columnar batches.
   */
  protected void assertAllBatchScansVectorized(SparkPlan plan) {
    List<SparkPlan> scans =
        SparkPlanUtil.collectLeaves(plan).stream()
            .filter(scan -> scan instanceof BatchScanExec || scan instanceof FileSourceScanExec)
            .collect(Collectors.toList());
    assertThat(scans).hasSizeGreaterThan(0).allMatch(SparkPlan::supportsColumnar);
  }

  protected void createTableWithDeleteGranularity(
      String schema, String partitionedBy, DeleteGranularity deleteGranularity) {
    createAndInitTable(schema, partitionedBy, null /* empty */);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s')",
        tableName, TableProperties.DELETE_GRANULARITY, deleteGranularity);
  }

  protected long waitUntilAfter(long timestampMillis) {
    long current = System.currentTimeMillis();
    while (current <= timestampMillis) {
      current = System.currentTimeMillis();
    }
    return current;
  }

  protected void withUnavailableFiles(Iterable<? extends ContentFile<?>> files, Action action) {
    Iterable<String> fileLocations = Iterables.transform(files, ContentFile::location);
    withUnavailableLocations(fileLocations, action);
  }

  private void move(String location, String newLocation) {
    Path path = Paths.get(URI.create(location));
    Path tempPath = Paths.get(URI.create(newLocation));
    try {
      Files.move(path, tempPath);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to move: " + location, e);
    }
  }

  protected void withUnavailableLocations(Iterable<String> locations, Action action) {
    for (String location : locations) {
      move(location, location + "_temp");
    }
    try {
      action.invoke();
    } finally {
      for (String location : locations) {
        move(location + "_temp", location);
      }
    }
  }

  protected void withDefaultTimeZone(String zoneId, Action action) {
    TimeZone currentZone = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(zoneId));
      action.invoke();
    } finally {
      TimeZone.setDefault(currentZone);
    }
  }

  protected void withSQLConf(Map<String, String> conf, Action action) {
    SQLConf sqlConf = SQLConf.get();
    Map<String, String> currentConfValues = new HashMap<>();
    conf.keySet()
        .forEach(
            confKey -> {
              if (sqlConf.contains(confKey)) {
                currentConfValues.put(confKey, sqlConf.getConfString(confKey));
              }
            });
    conf.forEach(
        (confKey, confValue) -> {
          if (SQLConf.isStaticConfigKey(confKey)) {
            throw new RuntimeException("Cannot modify the value of a static config: " + confKey);
          }
          sqlConf.setConfString(confKey, confValue);
        });
    try {
      action.invoke();
    } finally {
      conf.forEach(
          (confKey, confValue) -> {
            if (currentConfValues.containsKey(confKey)) {
              sqlConf.setConfString(confKey, currentConfValues.get(confKey));
            } else {
              sqlConf.unsetConf(confKey);
            }
          });
    }
  }

  protected Dataset<Row> jsonToDF(String schema, String... records) {
    Dataset<String> jsonDF = spark.createDataset(ImmutableList.copyOf(records), Encoders.STRING());
    return spark.read().schema(schema).json(jsonDF);
  }

  protected SparkPlan executeAndKeepPlan(String query, Object... args) {
    return executeAndKeepPlan(() -> sql(query, args));
  }

  protected SparkPlan executeAndKeepPlan(Action action) {
    AtomicReference<SparkPlan> executedPlanRef = new AtomicReference<>();
    QueryExecutionListener listener =
        new QueryExecutionListener() {
          @Override
          public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
            executedPlanRef.set(qe.executedPlan());
          }

          @Override
          public void onFailure(String funcName, QueryExecution qe, Exception exception) {}
        };
    spark.listenerManager().register(listener);
    action.invoke();
    try {
      spark.sparkContext().listenerBus().waitUntilEmpty();
    } catch (TimeoutException e) {
      throw new RuntimeException("Timeout while waiting for processing events", e);
    }
    SparkPlan executedPlan = executedPlanRef.get();
    if (executedPlan instanceof AdaptiveSparkPlanExec) {
      return ((AdaptiveSparkPlanExec) executedPlan).executedPlan();
    } else {
      return executedPlan;
    }
  }

  @FunctionalInterface
  protected interface Action {
    void invoke();
  }

  /** Loads the table under test from its location, where the pluggable table format keeps it. */
  private class PathCatalog implements Catalog {
    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return Collections.emptyList();
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return false;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
      throw new UnsupportedOperationException("rename");
    }

    /**
     * The suites read the row-level operation mode from Iceberg's write mode properties, which
     * Hudi does not set; the Hudi table type decides it, and the suite's own properties name it.
     */
    @Override
    public Table loadTable(TableIdentifier identifier) {
      Table table = new HadoopTables(spark.sessionState().newHadoopConf()).load(tablePath);
      return new BaseTable(((HasTableOperations) table).operations(), table.name()) {
        @Override
        public Map<String, String> properties() {
          Map<String, String> properties = new HashMap<>(super.properties());
          properties.putAll(extraTableProperties());
          return properties;
        }
      };
    }
  }
}
