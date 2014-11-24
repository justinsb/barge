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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.protobuf.ByteString;

import io.netty.util.concurrent.DefaultThreadFactory;
import journal.io.api.Journal;
import journal.io.api.JournalBuilder;

import org.robotninjas.barge.Replica;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.StateMachine.Snapshotter;
import org.robotninjas.barge.log.GetEntriesResult;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.log.StateMachineProxy;
import org.robotninjas.barge.log.journalio.RaftJournal.Mark;
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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.unmodifiableList;
import static org.robotninjas.barge.proto.RaftEntry.Entry;
import static org.robotninjas.barge.proto.RaftProto.AppendEntries;

@NotThreadSafe
public class JournalRaftLog implements RaftLog {

  private static final Logger LOGGER = LoggerFactory.getLogger(JournalRaftLog.class);
  private static final Entry SENTINEL = Entry.newBuilder().setCommand(ByteString.EMPTY).setTerm(0).build();

  private final TreeMap<Long, RaftJournal.Mark> log = Maps.newTreeMap();
  private final ConfigurationState config;
  private final StateMachineProxy stateMachine;
  private final RaftJournal journal;

  private final ConcurrentMap<Object, SettableFuture<Object>> operationResults = Maps.newConcurrentMap();

  private volatile long lastLogIndex = 0;
  private volatile long lastLogTerm = 0;
  private volatile long currentTerm = 0;
  private volatile Optional<Replica> votedFor = Optional.absent();
  private volatile long commitIndex = 0;
  private volatile long lastApplied = 0;
  private final String name;
//  private final ListeningExecutorService executor;
  

  JournalRaftLog(@Nonnull Journal journal, @Nonnull ConfigurationState config, @Nonnull StateMachineProxy stateMachine) {
    this.journal = new RaftJournal(checkNotNull(journal));
    this.config = checkNotNull(config);
    this.stateMachine = checkNotNull(stateMachine);
//    this.executor = checkNotNull(bargeThreadPools.getRaftExecutor());

    this.name = journal.getDirectory().getName();
  }

  public void close() throws Exception {
    this.journal.close();
    this.stateMachine.close();
  }
  
  public boolean isEmpty() {
    return journal.isEmpty();
  }
  
  public void load() {

    LOGGER.info("Replaying log");
    
//    journal.init();
    
    long oldCommitIndex = commitIndex;

 // TODO: fireCommitted more often??
    
    journal.replay(new RaftJournal.Visitor() {
      @Override
      public void term(RaftJournal.Mark mark, long term) {
        currentTerm = Math.max(currentTerm, term);
      }

      @Override
      public void vote(RaftJournal.Mark mark, Optional<Replica> vote) {
        votedFor = vote;
      }

      @Override
      public void commit(RaftJournal.Mark mark, long commit) {
        commitIndex = Math.max(commitIndex, commit);
      }

      @Override
      public void append(RaftJournal.Mark mark, Entry entry, long index) {
        lastLogIndex = Math.max(index, lastLogIndex);
        lastLogTerm = Math.max(entry.getTerm(), lastLogTerm);
        log.put(index, mark);
        
        if (entry.hasMembership()) {
          config.addMembershipEntry(index, entry);
        }
      }
    });

    final SettableFuture<Object> lastResult;
    if (oldCommitIndex != commitIndex) {
      lastResult = SettableFuture.create();
      operationResults.put(commitIndex, lastResult);
    } else {
      lastResult = null;
    }
    
    fireComitted();

    if (lastResult != null) {
      Futures.getUnchecked(lastResult);
    }

    LOGGER.info("Finished replaying log lastIndex {}, currentTerm {}, commitIndex {}, lastVotedFor {}",
        lastLogIndex, currentTerm, commitIndex, votedFor.orNull());
  }

  private SettableFuture<Object> storeEntry(final long index, @Nonnull Entry entry) {
//    LOGGER.debug("{} storing {}", config.self(), entry);
    RaftJournal.Mark mark = journal.appendEntry(entry, index);
    log.put(index, mark);
    
    if (entry.hasMembership()) {
      config.addMembershipEntry(index, entry);
    }

    SettableFuture<Object> result = SettableFuture.create();
    operationResults.put(index, result);
    return result;
  }

  public ListenableFuture<Object> append(@Nonnull byte[] operation,
      @Nonnull Membership membership) {
    long index = ++lastLogIndex;
    lastLogTerm = currentTerm;

    Entry.Builder entry = Entry.newBuilder().setTerm(currentTerm);
    if (operation != null) {
      checkArgument(membership == null);
      entry.setCommand(ByteString.copyFrom(operation));
    } else if (membership != null) {
      checkArgument(operation == null);
      entry.setMembership(membership);
    } else {
      checkArgument(false);
    }
    
    return storeEntry(index, entry.build());

  }

  public boolean append(@Nonnull AppendEntries appendEntries) {

    final long prevLogIndex = appendEntries.getPrevLogIndex();
    final long prevLogTerm = appendEntries.getPrevLogTerm();
    final List<Entry> entries = appendEntries.getEntriesList();

    RaftJournal.Mark previousLogIndexMark = log.get(prevLogIndex);
    if (prevLogIndex > 0 && previousLogIndexMark == null) {
      LOGGER.debug("Can't append; missing tail: prevLogIndex {} prevLogTerm {}", prevLogIndex, prevLogTerm);
      return false;
    }

    if (previousLogIndexMark != null) {
      Entry previousEntry = journal.get(previousLogIndexMark);

      if ((prevLogIndex > 0) && previousEntry.getTerm() != prevLogTerm) {
        LOGGER.debug("Can't append; missing tail: prevLogIndex {} prevLogTerm {}", prevLogIndex, prevLogTerm);
        return false;
      }

      journal.truncateTail(previousLogIndexMark);
      log.tailMap(prevLogIndex, false).clear();
    }

    lastLogIndex = prevLogIndex;
    for (Entry entry : entries) {
      storeEntry(++lastLogIndex, entry);
      lastLogTerm = entry.getTerm();
    }

    return true;

  }

  @Nonnull
  public GetEntriesResult getEntriesFrom(@Nonnegative long beginningIndex, @Nonnegative int max) {

    checkArgument(beginningIndex >= 0);

    long previousIndex = beginningIndex - 1;
    Entry previous = previousIndex <= 0 ? SENTINEL : journal.get(log.get(previousIndex));
    Iterable<Entry> entries = FluentIterable
        .from(log.tailMap(beginningIndex).values())
        .limit(max)
        .transform(new Function<RaftJournal.Mark, Entry>() {
          @Nullable
          @Override
          public Entry apply(@Nullable RaftJournal.Mark input) {
            return journal.get(input);
          }
        });

    return new GetEntriesResult(previous.getTerm(), previousIndex, entries);

  }

//  SnapshotInfo doSnapshot() {
//    Snapshotter snapshotter;
//    synchronized (this) {
//      ListenableFuture<Snapshotter> snapshotterFuture = stateMachine.prepareSnapshot(currentTerm, index);
//      snapshotter = snapshotterFuture.get();
//    }
//    
//    SnapshotInfo snapshotInfo = snapshotter.finishSnapshot();
//    return snapshotInfo;
//  }
  
  void fireComitted() {
    synchronized (this) {
      for (long i = lastApplied + 1; i <= Math.min(commitIndex, lastLogIndex); ++i, ++lastApplied) {
        // Entry entry = journal.get(log.get(i));
        // byte[] rawCommand = entry.getCommand().toByteArray();
        // final ByteBuffer operation = ByteBuffer.wrap(rawCommand).asReadOnlyBuffer();
        // ListenableFuture<Object> result = stateMachine.dispatchOperation(operation);
        //
        // final SettableFuture<Object> returnedResult = operationResults.remove(i);
        // // returnedResult may be null on log replay
        // if (returnedResult != null) {
        // Futures.addCallback(result, new PromiseBridge<Object>(returnedResult));
        // }

        Mark mark = log.get(i);
        if (mark == null) {
          LOGGER.warn("Cannot find log entry @{}", i);
          throw new IllegalStateException();
        }
        Entry entry = journal.get(mark);
        final SettableFuture<Object> operationResult = operationResults.remove(i);
        assert operationResult != null;

        if (entry.hasCommand()) {
          ByteString command = entry.getCommand();
          // byte[] rawCommand = command.toByteArray();
          // final ByteBuffer operation =
          // ByteBuffer.wrap(rawCommand).asReadOnlyBuffer();
          final ByteBuffer operation = command.asReadOnlyByteBuffer();

          ListenableFuture<Object> result = stateMachine.dispatchOperation(operation);
          if (operationResult != null) {
            Futures.addCallback(result, new PromiseBridge<Object>(operationResult));
          }
        } else if (entry.hasMembership()) {
          if (operationResult != null) {
            operationResult.set(Boolean.TRUE);
          }
        } else {
          LOGGER.warn("Ignoring unusual log entry: {}", entry);
        }

      }
    }
  }

  public String getName() {
    return name;
  }

  public long lastLogIndex() {
    return lastLogIndex;
  }

  public long lastLogTerm() {
    return lastLogTerm;
  }

  public long commitIndex() {
    return commitIndex;
  }

//  public ClusterConfig config() {
//    return config;
//  }

  public void setCommitIndex(long index) {
    commitIndex = index;
    journal.appendCommit(index);
    fireComitted();
  }

  public long currentTerm() {
    return currentTerm;
  }

  public void currentTerm(@Nonnegative long term) {
    checkArgument(term >= 0);
    MDC.put("term", Long.toString(term));
    LOGGER.debug("New term {}", term);
    currentTerm = term;
    votedFor = Optional.absent();
    journal.appendTerm(term);
  }

  @Nonnull
  public Optional<Replica> votedFor() {
    return votedFor;
  }

  public void votedFor(@Nonnull Optional<Replica> vote) {
    LOGGER.debug("Voting for {}", vote.orNull());
    votedFor = vote;
    journal.appendVote(vote);
  }


//  @Nonnull
//  public Replica getReplica(String info) {
//    return config.getReplica(info);
//  }

  @Override
  public String toString() {
    return Objects.toStringHelper(getClass())
        .add("lastLogIndex", lastLogIndex)
        .add("lastApplied", lastApplied)
        .add("commitIndex", commitIndex)
        .add("lastVotedFor", votedFor)
        .toString();
  }

  private static class PromiseBridge<V> implements FutureCallback<V> {

    private final SettableFuture<V> promise;

    private PromiseBridge(SettableFuture<V> promise) {
      checkNotNull(promise);
      this.promise = promise;
    }

    @Override
    public void onSuccess(@Nullable V result) {
      promise.set(result);
    }

    @Override
    public void onFailure(Throwable t) {
      promise.setException(t);
    }
  }
  
  public Replica self() {
    return config.self();
  }

  public static class Builder extends RaftLog.Builder {
    public File logDirectory;
    
    @Nonnull
    public JournalRaftLog build()  {
      checkNotNull(logDirectory);

      Journal journal;
      try {
        journal = JournalBuilder.of(logDirectory).setPhysicalSync(true).open();
      } catch (IOException e) {
        throw new IllegalStateException("Error building journal", e);
      }

      buildDefaults();

      return new JournalRaftLog(journal, config, stateMachineProxy);
    }

  }
}
