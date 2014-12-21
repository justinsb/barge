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

package org.robotninjas.barge.state;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.robotninjas.barge.RaftClusterHealth;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftEntry.ConfigTimeouts;
import org.robotninjas.barge.proto.RaftEntry.Membership;
import org.robotninjas.barge.proto.RaftEntry.SnapshotInfo;
import org.robotninjas.barge.proto.RaftProto.RequestVoteResponse;
import org.robotninjas.barge.rpc.RaftClient;
import org.robotninjas.barge.rpc.RaftClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import io.netty.util.concurrent.DefaultThreadFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.robotninjas.barge.proto.RaftProto.*;

@NotThreadSafe
public class RaftStateContext implements Raft {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftStateContext.class);

  private final ListeningScheduledExecutorService raftExecutor;

  private BaseState currentState;

  private boolean stop;

  private final ConfigurationState configurationState;

  private final RaftClientProvider raftClientProvider;

  private final RaftLog log;

//  private final BargeThreadPools threadPools;

  private final Random random = new Random();

  public RaftStateContext(RaftLog log, RaftClientProvider raftClientProvider, ConfigurationState configurationState) {
    this.log = checkNotNull(log);
    this.configurationState = checkNotNull(configurationState);
    this.raftClientProvider = checkNotNull(raftClientProvider);
    // BargeThreadPools threadPools) {
    String name = log.self().toString();
    MDC.put("self", name);

    this.raftExecutor = MoreExecutors.listeningDecorator(Executors
        .newSingleThreadScheduledExecutor(new DefaultThreadFactory("pool-raft-executor-" + name)));
    // this.raftExecutor = threadPools.getRaftExecutor();
    // this.threadPools = threadPools;
  }

  public void init() throws RaftException {
    Futures.get(onRaftThreadAsync(() -> { setState(null, buildStateStart()); return null; }), RaftException.class);
  }

  @Override
  @Nonnull
  public RequestVoteResponse requestVote(@Nonnull final RequestVote request) throws RaftException {
    ListenableFuture<RequestVoteResponse> future;
    
    synchronized (this) {
      final BaseState state = this.currentState;
      future = onRaftThreadAsync(() -> state.requestVote(request));
    }
    
    return Futures.get(future, RaftException.class);
  }

  @Override
  @Nonnull
  public AppendEntriesResponse appendEntries(@Nonnull final AppendEntries request) throws RaftException {
    ListenableFuture<AppendEntriesResponse> future;
    
    synchronized (this) {
      final BaseState state = this.currentState;
      future = onRaftThreadAsync(() -> state.appendEntries(request));
    }
    
    return Futures.get(future, RaftException.class);
  }

  @Nonnull
  public ListenableFuture<Object> commitOperation(@Nonnull final byte[] op) throws RaftException {
    checkNotNull(op);
    
    synchronized (this) {
      final BaseState state = this.currentState;
      return Futures.dereference(onRaftThreadAsync(() -> state.commitOperation(op)));
    }
  }


  public synchronized void setState(BaseState oldState, @Nonnull BaseState newState) {

    if (this.currentState != oldState) {
      LOGGER.info("Previous state was not correct (transitioning to {}). Expected {}, was {}", newState, currentState,
          oldState);
      throw new IllegalStateException();
    }

    // StateType newState;
    if (stop && this.currentState.type() != RaftState.STOPPED) {
      newState = buildStateStopped();
      LOGGER.info("Service stopping; replaced state with {}", newState);
      // } else {
      // newState = checkNotNull(state);
    }

    LOGGER.info("Transition: old state: {}, new state: {}", this.currentState, newState);
    if (this.currentState != null) {
      this.currentState.destroy();
    }

    this.currentState = checkNotNull(newState);

    MDC.put("state", this.currentState.toString());

    if (currentState != null) {
      currentState.init();
    }

    // if (this.state.type() == StateType.LEADER) {
    // if (log.isEmpty()) {
    // RaftMembership initialMembership = getConfigurationState().getClusterMembership();
    // try {
    // state.setConfiguration(this, null, initialMembership);
    // } catch (RaftException e) {
    // LOGGER.error("Error during bootstrap", e);
    // throw new IllegalStateException("Error during bootstrap", e);
    // }
    // }
    // }
  }

  @Nonnull
  public synchronized RaftState type() {
    return currentState.type();
  }

  public synchronized void stop() throws Exception {
    stop = true;
    if (this.currentState != null) {
      this.currentState.doStop();
    }
    while (!isStopped()) {
      Thread.sleep(10);
    }
    log.close();

    raftExecutor.shutdown();
    raftExecutor.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Nonnull
  public ConfigurationState getConfigurationState() {
    return configurationState;
  }

  public synchronized boolean shouldStop() {
    return stop;
  }

  public synchronized boolean isStopped() {
    return this.currentState.type() == RaftState.STOPPED;
  }

  public synchronized boolean isLeader() {
    return this.currentState.type() == RaftState.LEADER;
  }

  public RaftClusterHealth getClusterHealth() throws RaftException {
    ListenableFuture<RaftClusterHealth> future;
    
    synchronized (this) {
      final BaseState state = this.currentState;
      future = onRaftThreadAsync(() -> state.getClusterHealth());
    }
    
    return Futures.get(future, RaftException.class);
  }
  

  /**
   * Call callable on the raft thread (i.e. serialized)
   */
  <T> ListenableFuture<T> onRaftThreadAsync(Callable<T> callable) {
    ListenableFuture<T> response = raftExecutor.submit(callable);
    return response;
  }
   
//  public RaftClientManager getClientManager() {
//    return clientManager;
//  }

  @Override
  public synchronized String toString() {
    return "RaftStateContext [state=" + currentState + ", configurationState=" + configurationState + "]";
  }

  Follower buildStateFollower(Optional<Replica> leader) {
    return new Follower(this, leader);
  }

  Start buildStateStart() {
    return new Start(this);
  }

  Stopped buildStateStopped() {
    return new Stopped(this);
  }

  Candidate buildStateCandidate() {
    return new Candidate(this);
  }

  Leader buildStateLeader() {
    return new Leader(this);
  }

  public Replica self() {
    return this.log.self();
  }

  Random random() {
    return random;
  }

  ScheduledExecutorService getRaftScheduler() {
    return raftExecutor;
  }

  RaftLog getLog() {
    return log;
  }

  ConfigTimeouts getTimeouts() {
    return configurationState.getTimeouts();
  }

  public synchronized Optional<Replica> getLeader() {
    return this.currentState.getLeader();
  }
  

  // TODO: This should probably take a RaftMembership
  public void bootstrap(Membership membership) {
    LOGGER.info("Bootstrapping log with {}", membership);
    if (!log.isEmpty()) {
      LOGGER.warn("Cannot bootstrap, as raft log already contains data");
      throw new IllegalStateException();
    }
    log.append(null, membership);
  }

  public synchronized ListenableFuture<Boolean> setConfiguration(final RaftMembership oldMembership,
      final RaftMembership newMembership) {
    checkNotNull(oldMembership);
    checkNotNull(newMembership);

    final BaseState state = this.currentState;
    return Futures.dereference(onRaftThreadAsync(() -> state.setConfiguration(oldMembership, newMembership)));
  }

  RaftClient getRaftClient(Replica replica) {
    return raftClientProvider.get(replica);
  }

  public SnapshotInfo getLastSnapshotInfo() {
    return this.log.getLastSnapshotInfo();
  }

  synchronized boolean isActive(BaseState state) {
    // Strict reference equality
    return this.currentState == state;
  }


}
