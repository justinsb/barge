/**
 * Copyright 2014 Justin Santa Barbara
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

package org.robotninjas.barge.log;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.proto.RaftEntry.Membership;
import org.robotninjas.barge.proto.RaftEntry.SnapshotInfo;
import org.robotninjas.barge.proto.RaftProto.AppendEntries;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;

@NotThreadSafe
public interface RaftLog {

  long getCurrentTerm();

  void setCurrentTerm(long currentTerm);

  GetEntriesResult getEntriesFrom(@Nonnegative long beginningIndex, @Nonnegative int max);

  public String getName();

  public long getLastLogTerm();

  public long getLastLogIndex();

  public long getCommitIndex();

  public void setCommitIndex(long index);

  // TODO: Remove?
  public Replica self();

  // TODO: This seems wrong / should have a term?
  @Nonnull
  public Optional<Replica> votedFor();

  // TODO: This seems wrong / should have a term?
  // TODO: Create one append method?
  public void votedFor(@Nonnull Optional<Replica> vote);

  // TODO: Create one append method?
  public ListenableFuture<Object> append(@Nonnull byte[] operation, @Nonnull Membership membership);

  // TODO: Create one append method?
  public boolean append(@Nonnull AppendEntries appendEntries);

  public void close() throws Exception;

  public void load() throws IOException;

  boolean isEmpty();

  ListenableFuture<SnapshotInfo> performSnapshot() throws RaftException;

  boolean shouldSnapshot();

  SnapshotInfo getLastSnapshotInfo();

}
