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

package org.robotninjas.barge.log.journalio;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

import journal.io.api.ClosedJournalException;
import journal.io.api.CompactedDataFileException;
import journal.io.api.Journal;
import journal.io.api.JournalBuilder;
import journal.io.api.Location;
import journal.io.api.Journal.ReadType;
import journal.io.api.Journal.WriteType;

import org.robotninjas.barge.RaftService;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.StateMachine.Snapshotter;
import org.robotninjas.barge.log.GetEntriesResult;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.log.RaftLogBase;
import org.robotninjas.barge.log.StateMachineProxy;
import org.robotninjas.barge.proto.LogProto;
import org.robotninjas.barge.proto.LogProto.JournalEntry;
import org.robotninjas.barge.proto.RaftEntry.Entry;
import org.robotninjas.barge.proto.RaftEntry.Membership;
import org.robotninjas.barge.proto.RaftEntry.SnapshotInfo;
import org.robotninjas.barge.state.ConfigurationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

import static com.google.common.base.Functions.toStringFunction;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;

@NotThreadSafe
public class JournalRaftLog extends RaftLogBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(JournalRaftLog.class);
  private static final Entry SENTINEL = Entry.newBuilder().setCommand(ByteString.EMPTY).setTerm(0).build();

  private final TreeMap<Long, Location> log = Maps.newTreeMap();
  private final Journal journal;

  JournalRaftLog(@Nonnull ConfigurationState config, @Nonnull StateMachineProxy stateMachine, @Nonnull Journal journal) {
    super(config, stateMachine);
    this.journal = checkNotNull(journal);
  }

  public void close() throws Exception {
    this.journal.close();
    super.close();
  }

  public boolean isEmpty() {
    return journal.getFiles().isEmpty();
  }

  public static class Builder extends RaftLogBase.Builder {
    public File logDirectory;

    @Nonnull
    public JournalRaftLog build() {
      checkNotNull(logDirectory);

      Journal journal;
      try {
        journal = JournalBuilder.of(logDirectory).setPhysicalSync(true).open();
      } catch (IOException e) {
        throw new IllegalStateException("Error building journal", e);
      }

      buildDefaults();

      return new JournalRaftLog(config, stateMachineProxy, journal);
    }

  }

  @Override
  protected void replay(Visitor visitor) throws IOException {
    for (Location location : journal.redo()) {
      LogProto.JournalEntry entry = read(location);

      if (entry.hasAppend()) {
        LogProto.Append append = entry.getAppend();
        long index = append.getIndex();
        log.put(index, location);
        visitor.append(append.getEntry(), index);
      } else if (entry.hasCommit()) {
        LogProto.Commit commit = entry.getCommit();
        visitor.commit(commit.getIndex());
      } else if (entry.hasTerm()) {
        LogProto.Term term = entry.getTerm();
        visitor.term(term.getTerm());
      } else if (entry.hasVote()) {
        LogProto.Vote vote = entry.getVote();
        String votedfor = vote.getVotedFor();
        Replica replica = votedfor == null ? null : Replica.fromString(votedfor);
        visitor.vote(Optional.fromNullable(replica));
      }

    }
  }

  @Override
  protected void writeEntry(long index, Entry entry) {
    LogProto.JournalEntry je = LogProto.JournalEntry.newBuilder()
        .setAppend(LogProto.Append.newBuilder().setEntry(entry).setIndex(index)).build();

    Location location = write(je);
    log.put(index, location);
  }

  @Override
  protected Entry readLogEntry(long index) {
    Location mark = log.get(index);
    if (mark == null) {
      LOGGER.warn("Cannot find mark for log entry @{}", index);
      return null;
    }

    return getEntry(index, mark);
  }

  private Entry getEntry(long index, Location mark) {
    LogProto.JournalEntry journalEntry = read(mark);
    Entry entry = journalEntry.getAppend().getEntry();
    if (entry == null) {
      LOGGER.warn("Cannot find log entry @{}", index);
      return null;
    }
    return entry;
  }

  private LogProto.JournalEntry read(Location loc) {
    try {
      byte[] data = journal.read(loc, ReadType.SYNC);
      return LogProto.JournalEntry.parseFrom(data);
    } catch (IOException e) {
      throw propagate(e);
    }
  }

  private Location write(LogProto.JournalEntry entry) {
    try {
      return journal.write(entry.toByteArray(), WriteType.SYNC);
    } catch (IOException e) {
      throw propagate(e);
    }
  }

  @Override
  protected void recordCommit(long commit) {
    write(LogProto.JournalEntry.newBuilder().setCommit(LogProto.Commit.newBuilder().setIndex(commit)).build());
  }

  @Override
  protected void recordTerm(long term) {
    write(LogProto.JournalEntry.newBuilder().setTerm(LogProto.Term.newBuilder().setTerm(term)).build());
  }

  @Override
  protected void recordVote(Optional<Replica> vote) {
    write(LogProto.JournalEntry.newBuilder()
        .setVote(LogProto.Vote.newBuilder().setVotedFor(vote.transform(toStringFunction()).or(""))).build());
  }

  @Override
  protected boolean truncateLog(long afterIndex) throws IOException {
    Location location = log.get(afterIndex);
    if (location == null) {
      return false;
    }

    Iterable<Location> locations = FluentIterable.from(journal.redo(location)).skip(1);

    for (Location loc : locations) {
      journal.delete(loc);
    }
    log.tailMap(afterIndex, false).clear();
    return true;
  }

  @Override
  protected GetEntriesResult readEntriesFrom(long beginningIndex, int max) {

    long previousIndex = beginningIndex - 1;
    Entry previous = previousIndex <= 0 ? SENTINEL : readLogEntry(previousIndex);
    if (previous == null) {
      throw new IllegalStateException();
    }
    Iterable<Entry> entries = FluentIterable.from(log.tailMap(beginningIndex).values()).limit(max)
        .transform(new Function<Location, Entry>() {
          @Nullable
          @Override
          public Entry apply(@Nullable Location mark) {
            LogProto.JournalEntry journalEntry = read(mark);
            Entry entry = journalEntry.getAppend().getEntry();
            if (entry == null) {
              throw new IllegalStateException();
            }
            return entry;
          }
        });

    return new GetEntriesResult(previous.getTerm(), previousIndex, entries);
  }

  @Override
  protected void removeLogEntriesBefore(long snapshotIndex) throws IOException {
    Location end = log.get(snapshotIndex);
    if (end == null) {
      return;
    }

    Iterable<Location> locations = journal.redo();
    for (Location loc : locations) {
      if (loc.compareTo(end) >= 0) {
        break;
      }
      //LOGGER.debug("Deleting entry {}", loc);
      journal.delete(loc);
    }
    journal.compact();
    log.headMap(snapshotIndex, false).clear();
  }


//  @Override
//  protected Location recordSnapshot(SnapshotInfo snapshotInfo) {
//    Location location = write(LogProto.JournalEntry.newBuilder().setSnapshot(snapshotInfo).build());
//    return location;
//  }
}
