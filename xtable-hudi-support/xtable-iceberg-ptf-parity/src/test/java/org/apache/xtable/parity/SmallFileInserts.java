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

import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;

/**
 * Hudi packs inserted rows into an existing small file of their partition, which rewrites that
 * file, so the insert is published as an overwrite where Iceberg's writer publishes an append. That
 * is Hudi's file sizing and valid Iceberg, so the label comparisons accept it; readers that take
 * appends only, such as streaming and incremental append reads, need {@code
 * hoodie.parquet.small.file.limit=0}.
 */
final class SmallFileInserts {
  private SmallFileInserts() {}

  /**
   * The operation of each published snapshot, with an overwrite read as an append where the native
   * snapshot at the same position is an append that adds the same number of rows. A packed insert
   * removes the rows of the file it rewrites and adds them back with the new ones, so its net row
   * count matches the native append; an update or delete labelled overwrite does not.
   */
  static List<String> operations(List<Snapshot> nativeHistory, List<Snapshot> publishedHistory) {
    List<String> operations = new ArrayList<>();
    for (int i = 0; i < publishedHistory.size(); i++) {
      Snapshot published = publishedHistory.get(i);
      boolean packedInsert =
          i < nativeHistory.size()
              && DataOperations.OVERWRITE.equals(published.operation())
              && DataOperations.APPEND.equals(nativeHistory.get(i).operation())
              && netRecords(publishedHistory, i) == netRecords(nativeHistory, i);
      operations.add(packedInsert ? DataOperations.APPEND : published.operation());
    }
    return operations;
  }

  private static long netRecords(List<Snapshot> history, int index) {
    long before = index == 0 ? 0 : totalRecords(history.get(index - 1));
    return totalRecords(history.get(index)) - before;
  }

  private static long totalRecords(Snapshot snapshot) {
    return Long.parseLong(snapshot.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP));
  }
}
