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

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.StateMachine.Snapshotter;
import org.robotninjas.barge.proto.RaftEntry;
import org.robotninjas.barge.proto.RaftEntry.Entry;
import org.robotninjas.barge.proto.RaftEntry.Membership;
import org.robotninjas.barge.proto.RaftEntry.SnapshotInfo;
import org.robotninjas.barge.state.ConfigurationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.DefaultThreadFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.robotninjas.barge.proto.RaftProto.AppendEntries;

@NotThreadSafe
public abstract class RaftLogBase implements RaftLog {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftLogBase.class);

  private final String name;

  private final ConfigurationState config;
  private final StateMachineProxy stateMachine;

  private final ConcurrentMap<Object, SettableFuture<Object>> operationResults = Maps.newConcurrentMap();

  private volatile long lastLogIndex = 0;
  private volatile long lastLogTerm = 0;
  private volatile long currentTerm = 0;
  private volatile Optional<Replica> votedFor = Optional.absent();
  private volatile long commitIndex = 0;

  /**
   * Most recent snapshot
   */
  private SnapshotInfo lastSnapshot;

  /**
   * Index applied to stateMachine
   */
  private volatile long lastApplied = 0;

  // private final ListeningExecutorService executor;

  public static interface Visitor {

    void term(long term);

    void vote(Optional<Replica> vote);

    void commit(long commit);

    void append(RaftEntry.Entry entry, long index);

  };

  protected RaftLogBase(@Nonnull ConfigurationState config, @Nonnull StateMachineProxy stateMachine) {
    this.config = checkNotNull(config);
    this.stateMachine = checkNotNull(stateMachine);
    // this.executor = checkNotNull(bargeThreadPools.getRaftExecutor());

    this.name = config.self().getKey();
  }

  public void close() throws Exception {
    this.stateMachine.close();
  }

  public void load() throws IOException {

    LOGGER.info("Replaying log");

    // journal.init();

    long oldCommitIndex = commitIndex;

    // TODO: fireCommitted more often??

    replay(new Visitor() {

      @Override
      public void term(long term) {
        currentTerm = Math.max(currentTerm, term);
      }

      @Override
      public void vote(Optional<Replica> vote) {
        votedFor = vote;
      }

      @Override
      public void commit(long commit) {
        commitIndex = Math.max(commitIndex, commit);
      }

      @Override
      public void append(Entry entry, long index) {
        lastLogIndex = Math.max(index, lastLogIndex);
        lastLogTerm = Math.max(entry.getTerm(), lastLogTerm);

        if (entry.hasMembership()) {
          config.addMembershipEntry(index, entry);
        }

        if (entry.hasSnapshot()) {
          lastSnapshot = entry.getSnapshot();
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

    LOGGER.info("Finished replaying log lastIndex {}, currentTerm {}, commitIndex {}, lastVotedFor {}", lastLogIndex,
        currentTerm, commitIndex, votedFor.orNull());
  }

  protected abstract void replay(Visitor visitor) throws IOException;

  private SettableFuture<Object> storeEntry(final long index, @Nonnull Entry entry) {
    // LOGGER.debug("{} storing {}", config.self(), entry);
    writeEntry(index, entry);

    if (entry.hasMembership()) {
      config.addMembershipEntry(index, entry);
    }

    SettableFuture<Object> result = SettableFuture.create();
    operationResults.put(index, result);
    return result;
  }

  protected abstract void writeEntry(long index, Entry entry);

  public ListenableFuture<Object> append(@Nonnull byte[] operation, @Nonnull Membership membership) {
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

    return proposeEntry(entry);
  }

  private ListenableFuture<Object> proposeEntry(Entry.Builder entry) {
    long index = ++lastLogIndex;
    lastLogTerm = currentTerm;

    entry.setTerm(currentTerm);

    return storeEntry(index, entry.build());
  }

  public boolean append(@Nonnull AppendEntries appendEntries) {

    long prevLogIndex = appendEntries.getPrevLogIndex();
    final long prevLogTerm = appendEntries.getPrevLogTerm();
    List<Entry> entries = appendEntries.getEntriesList();

    Entry previousEntry = readLogEntry(prevLogIndex);
    boolean restoreFromSnapshot = false;
    
    if (previousEntry == null) {
      // Check if we can restore from a snapshot
      int snapshotIndex = -1;
      for (int i = 0; i < entries.size(); i++) {
        Entry entry = entries.get(i);
        if (entry.hasSnapshot()) {
          snapshotIndex = i;
        }
      }
      if (snapshotIndex != -1) {
        LOGGER.debug("Got appendEntries that includes snapshot; will use: {}", entries.get(snapshotIndex));
        restoreFromSnapshot = true;
        prevLogIndex = appendEntries.getPrevLogIndex() + snapshotIndex;
        entries = entries.subList(snapshotIndex, entries.size());
      }
    }
    
    if (!restoreFromSnapshot && prevLogIndex > 0 && previousEntry == null) {
      LOGGER.debug("Can't append; missing tail: prevLogIndex {} prevLogTerm {}", prevLogIndex, prevLogTerm);
      return false;
    }

    if (previousEntry != null) {
      if ((prevLogIndex > 0) && previousEntry.getTerm() != prevLogTerm) {
        LOGGER.debug("Can't append; missing tail: prevLogIndex {} prevLogTerm {}", prevLogIndex, prevLogTerm);
        return false;
      }

      try {
        truncateLog(prevLogIndex);
      } catch (IOException e) {
        throw new IllegalStateException("Error truncating log", e);
      }
    }

    lastLogIndex = prevLogIndex;
    for (Entry entry : entries) {
      storeEntry(++lastLogIndex, entry);
      lastLogTerm = entry.getTerm();
    }

    return true;

  }

  protected abstract boolean truncateLog(long afterIndex) throws IOException;

  @Nonnull
  public GetEntriesResult getEntriesFrom(@Nonnegative long beginningIndex, @Nonnegative int max) {
    checkArgument(beginningIndex >= 0);

    return readEntriesFrom(beginningIndex, max);
  }

  protected abstract GetEntriesResult readEntriesFrom(long beginningIndex, int max);

  public ListenableFuture<SnapshotInfo> performSnapshot() throws RaftException {
    SnapshotInfo snapshotInfo;
    try {
      Snapshotter snapshotter;
      synchronized (this) {
        ListenableFuture<Snapshotter> snapshotterFuture = stateMachine.prepareSnapshot(currentTerm, lastApplied);
        snapshotter = snapshotterFuture.get();
      }

      snapshotInfo = snapshotter.finishSnapshot();
    } catch (Exception e) {
      LOGGER.error("Error during snapshot", e);
      throw RaftException.propagate(e);
    }

    LOGGER.info("Wrote snapshot {}", snapshotInfo);
    
    ListenableFuture<Object> entryFuture = proposeEntry(Entry.newBuilder().setSnapshot(snapshotInfo));

    return Futures.transform(entryFuture, G8.fn((result) -> {
//      if (!Objects.equal(result, Boolean.TRUE))
//        throw new IllegalStateException();
      this.lastSnapshot = snapshotInfo;
      return snapshotInfo;
    }));
  }

  void fireComitted() {
    synchronized (this) {
      long end = Math.min(commitIndex, lastLogIndex);
      long start = lastApplied + 1;
      
      for (long i = start; i <= end; ++i) {
        Entry entry = readLogEntry(i);
        if (entry == null && i == start) {
          // Check if this is a snapshot restore
          long pos = end;
          boolean isSnapshotRestore = false;
          while (pos >= start) {
            entry = readLogEntry(pos);
            if (entry == null) {
              break;
            }
            if (entry.hasSnapshot()) {
              isSnapshotRestore = true;
              break;
            }
            pos--;
          }
          if (!isSnapshotRestore) {
            LOGGER.warn("Cannot find log entry @{}", i);
            throw new IllegalStateException();
          } else {
            LOGGER.info("Detected snapshot restore @{}", pos);
            i = pos;
            
            // Fall through
          }
        }
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
        } else if (entry.hasSnapshot()) {
          SnapshotInfo snapshotInfo = entry.getSnapshot();
          
          ListenableFuture<Object> result = stateMachine.gotSnapshot(snapshotInfo);
          if (operationResult != null) {
            Futures.addCallback(result, new PromiseBridge<Object>(operationResult));
          }
          Futures.addCallback(result, new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
              LOGGER.info("Deleting entries before snapshot: {}", snapshotInfo);
              
              try {
                removeLogEntriesBefore(snapshotInfo.getLastIncludedIndex());
              } catch (IOException e) {
                LOGGER.error("Unable to perform log compaction", e);
              }
            }

            @Override
            public void onFailure(Throwable t) {
               LOGGER.warn("Error from state machine on snapshot; won't truncate log", t);
            }
          });
        } else {
          LOGGER.warn("Ignoring unusual log entry: {}", entry);
        }

        assert (lastApplied + 1) == i;
        lastApplied = i;
      }
    }
  }

  protected abstract void removeLogEntriesBefore(long snapshotIndex) throws IOException;
  
  public String getName() {
    return name;
  }

  public long getLastLogIndex() {
    return lastLogIndex;
  }

  public long getLastLogTerm() {
    return lastLogTerm;
  }

  public long getCommitIndex() {
    return commitIndex;
  }

  // public ClusterConfig config() {
  // return config;
  // }

  public void setCommitIndex(long index) {
    commitIndex = index;
    recordCommit(index);
    fireComitted();
  }

  protected abstract void recordCommit(long index);

  protected abstract void recordTerm(long term);

  protected abstract void recordVote(@Nonnull Optional<Replica> vote);

  protected abstract Entry readLogEntry(long index);

  public long getCurrentTerm() {
    return currentTerm;
  }

  public void setCurrentTerm(@Nonnegative long term) {
    checkArgument(term >= 0);
    // MDC.put("term", Long.toString(term));
    LOGGER.debug("New term {}", term);
    currentTerm = term;
    votedFor = Optional.absent();
    recordTerm(term);
  }

  @Nonnull
  public Optional<Replica> votedFor() {
    return votedFor;
  }

  public void votedFor(@Nonnull Optional<Replica> vote) {
    LOGGER.debug("Voting for {}", vote.orNull());
    votedFor = vote;
    recordVote(vote);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(getClass()).add("lastLogIndex", lastLogIndex).add("lastApplied", lastApplied)
        .add("commitIndex", commitIndex).add("lastVotedFor", votedFor).toString();
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

  public abstract static class Builder {
    public StateMachine stateMachine;
    public ConfigurationState config;

    public ListeningExecutorService stateMachineExecutor;

    protected StateMachineProxy stateMachineProxy;

    @Nonnull
    public abstract RaftLog build();

    protected void buildDefaults() {
      String key = self().getKey();

      boolean closeStateMachineExecutor = false;
      if (stateMachineExecutor == null) {
        stateMachineExecutor = MoreExecutors.listeningDecorator(Executors
            .newSingleThreadExecutor(new DefaultThreadFactory("pool-state-worker-" + key)));
        closeStateMachineExecutor = true;
      }

      stateMachineProxy = new StateMachineProxy(stateMachine, stateMachineExecutor, closeStateMachineExecutor);
    }

    public Replica self() {
      return config.self();
    }

  }

  @Override
  public boolean shouldSnapshot() {
    long lastSnapshotIndex = 0;
    SnapshotInfo lastSnapshot = this.lastSnapshot;

    if (lastSnapshot != null) {
      lastSnapshotIndex = lastSnapshot.getLastIncludedIndex();
    }

    long delta = this.lastApplied - lastSnapshotIndex;
    long snapshotIntervalIndexes = 1000;
    if (delta > snapshotIntervalIndexes) {
      return true;
    }
    return false;

  }
  
  @Override
  public SnapshotInfo getLastSnapshotInfo() {
    return lastSnapshot;
  }

}
