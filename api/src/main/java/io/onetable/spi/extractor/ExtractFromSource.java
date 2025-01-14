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
 
package io.onetable.spi.extractor;

import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;

import io.onetable.model.*;
import io.onetable.model.CommitsBacklog;

@AllArgsConstructor(staticName = "of")
@Getter
public class ExtractFromSource<COMMIT> {
  private final SourceClient<COMMIT> sourceClient;

  public OneSnapshot extractSnapshot() {
    return sourceClient.getCurrentSnapshot();
  }

  public IncrementalTableChanges extractTableChanges(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    CommitsBacklog<COMMIT> commitsBacklog =
        sourceClient.getCommitsBacklog(instantsForIncrementalSync);
    // No overlap between updatedPendingCommits and commitList, process separately.
    List<TableChange> tableChangeList = new ArrayList<>();
    for (COMMIT commit : commitsBacklog.getCommitsToProcess()) {
      TableChange tableChange = sourceClient.getTableChangeForCommit(commit);
      tableChangeList.add(tableChange);
    }
    return IncrementalTableChanges.builder()
        .tableChanges(tableChangeList)
        .pendingCommits(commitsBacklog.getInFlightInstants())
        .build();
  }
}
