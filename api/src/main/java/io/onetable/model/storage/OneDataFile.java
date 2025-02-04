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
 
package io.onetable.model.storage;

import java.util.Collections;
import java.util.Map;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.SchemaVersion;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;

/**
 * Represents a data file in the table.
 *
 * @since 0.1
 */
@Builder(toBuilder = true)
@Value
public class OneDataFile {
  // written schema version
  SchemaVersion schemaVersion;
  // physical path of the file (absolute)
  @NonNull String physicalPath;
  // file format
  @Builder.Default @NonNull FileFormat fileFormat = FileFormat.APACHE_PARQUET;
  // partition ranges for the data file
  @Builder.Default @NonNull Map<OnePartitionField, Range> partitionValues = Collections.emptyMap();

  String partitionPath;
  long fileSizeBytes;
  long recordCount;
  // column stats for each column in the data file
  @Builder.Default @NonNull Map<OneField, ColumnStat> columnStats = Collections.emptyMap();
  // last modified time in millis since epoch
  long lastModified;
}
