/**
 * Copyright 2013 Justin Santa Barbara
 * Copyright 2013 David Rusek <dave dot rusek at gmail dot com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.robotninjas.barge;

import javax.annotation.Nonnull;

import org.robotninjas.barge.proto.RaftEntry.SnapshotInfo;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

public interface StateMachine {

  Object applyOperation(@Nonnull ByteBuffer entry);

  // void installSnapshot(@Nonnull InputStream snapshot);

  public interface Snapshotter {
    SnapshotInfo finishSnapshot() throws InterruptedException, ExecutionException;
  }

  Snapshotter prepareSnapshot(long currentTerm, long currentIndex);

  void gotSnapshot(SnapshotInfo snapshotInfo);

  void close();

  void init(RaftService raft);
}
