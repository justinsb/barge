/**
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
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;

import org.robotninjas.barge.BargeThreadPools;
import org.robotninjas.barge.RaftClusterHealth;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.RaftClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.robotninjas.barge.proto.RaftProto.*;

@NotThreadSafe
public class RaftStateContext implements Raft {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftStateContext.class);

  private final Executor executor;
  private final Set<StateTransitionListener> listeners = Sets.newConcurrentHashSet();

  private volatile State state;

  private boolean stop;

  private final ConfigurationState configurationState;

  private final RaftClientManager clientManager;

  private final RaftLog log;

  private final BargeThreadPools threadPools;

  private final Random random = new Random();
  
  @Inject
  RaftStateContext(RaftLog log, RaftClientManager clientManager, ConfigurationState configurationState,
      BargeThreadPools threadPools) {
    this(log.self().toString(), log, clientManager, configurationState, threadPools);
  }

  RaftStateContext(String name, RaftLog log, RaftClientManager clientManager, ConfigurationState configurationState,
      BargeThreadPools threadPools) {
    this.configurationState = configurationState;
    this.log = log;
    MDC.put("self", name);

    this.executor = threadPools.getRaftExecutor();
    this.threadPools = threadPools;
    this.listeners.add(new LogListener());
    this.clientManager = clientManager;
  }

  @Override
  public ListenableFuture<Void> init() {

    ListenableFutureTask<Void> init = ListenableFutureTask.create(new Callable<Void>() {
      @Override
      public Void call() throws RaftException {
        setState(null, buildStateStart());
        return null;
      }
    });

    executor.execute(init);

    return init;

  }


  @Override
  @Nonnull
  public RequestVoteResponse requestVote(@Nonnull final RequestVote request) {

    checkNotNull(request);

    ListenableFutureTask<RequestVoteResponse> response = ListenableFutureTask
        .create(new Callable<RequestVoteResponse>() {
          @Override
          public RequestVoteResponse call() throws Exception {
            return state.requestVote(RaftStateContext.this, request);
          }
        });

    executor.execute(response);

    try {
      return response.get();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

  }

  @Override
  @Nonnull
  public AppendEntriesResponse appendEntries(@Nonnull final AppendEntries request) {

    checkNotNull(request);

    ListenableFutureTask<AppendEntriesResponse> response = ListenableFutureTask
        .create(new Callable<AppendEntriesResponse>() {
          @Override
          public AppendEntriesResponse call() throws Exception {
            return state.appendEntries(RaftStateContext.this, request);
          }
        });

    executor.execute(response);

    try {
      return response.get();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

  }

  @Override
  @Nonnull
  public ListenableFuture<Object> commitOperation(@Nonnull final byte[] op) throws RaftException {

    checkNotNull(op);

    ListenableFutureTask<ListenableFuture<Object>> response = ListenableFutureTask
        .create(new Callable<ListenableFuture<Object>>() {
          @Override
          public ListenableFuture<Object> call() throws Exception {
            return state.commitOperation(RaftStateContext.this, op);
          }
        });

    executor.execute(response);

    return Futures.dereference(response);

  }

  @Nonnull
  public ListenableFuture<Boolean> setConfiguration(@Nonnull RaftMembership oldMembership,
      @Nonnull RaftMembership newMembership) throws RaftException {
    checkNotNull(oldMembership);
    checkNotNull(newMembership);
    return state.setConfiguration(this, oldMembership, newMembership);
  }

  public synchronized void setState(State oldState, @Nonnull State newState) {

    if (this.state != oldState) {
      LOGGER.info("Previous state was not correct (transitioning to {}). Expected {}, was {}", newState, state, oldState);
      notifiesInvalidTransition(oldState);
      throw new IllegalStateException();
    }

//    StateType newState;
    if (stop && state.type() != StateType.STOPPED) {
      newState = buildStateStopped();
      LOGGER.info("Service stopping; replaced state with {}", newState);
//    } else {
//      newState = checkNotNull(state);
    }

    LOGGER.info("Transition: old state: {}, new state: {}", this.state, newState);
    if (this.state != null) {
      this.state.destroy(this);
    }

    this.state = checkNotNull(newState);

    MDC.put("state", this.state.toString());

    notifiesChangeState(oldState);

    if (state != null) {
      state.init(this);
    }

//    if (this.state.type() == StateType.LEADER) {
//      if (log.isEmpty()) {
//        RaftMembership initialMembership = getConfigurationState().getClusterMembership();
//        try {
//          state.setConfiguration(this, null, initialMembership);
//        } catch (RaftException e) {
//          LOGGER.error("Error during bootstrap", e);
//          throw new IllegalStateException("Error during bootstrap", e);
//        }
//      }
//    }
  }

  private void notifiesInvalidTransition(State oldState) {
    for (StateTransitionListener listener : listeners) {
      listener.invalidTransition(this, state.type(), oldState == null ? null : oldState.type());
    }
  }

  private void notifiesChangeState(State oldState) {
    for (StateTransitionListener listener : listeners) {
      listener.changeState(this, oldState == null ? null : oldState.type(), state.type());
    }
  }

  @Override
  public void addTransitionListener(@Nonnull StateTransitionListener transitionListener) {
    listeners.add(transitionListener);
  }

  @Override
  @Nonnull
  public StateType type() {
    return state.type();
  }

  public synchronized void stop() {
    stop = true;
    if (this.state != null) {
      this.state.doStop(this);
    }
  }

  private class LogListener implements StateTransitionListener {
    @Override
    public void changeState(@Nonnull Raft context, @Nullable StateType from, @Nonnull StateType to) {
      LOGGER.info("LogListener: old state: {}, new state: {}", from, to);
    }

    @Override
    public void invalidTransition(@Nonnull Raft context, @Nonnull StateType actual, @Nullable StateType expected) {
      LOGGER
          .warn("LogListener: State transition from incorrect previous state.  Expected {}, was {}", actual, expected);
    }
  }

  @Nonnull
  public ConfigurationState getConfigurationState() {
    return configurationState;
  }

  public synchronized boolean shouldStop() {
    return stop;
  }

  public synchronized boolean isStopped() {
    return this.state.type() == StateType.STOPPED;
  }

  public boolean isLeader() {
    return this.state.type() == StateType.LEADER;
  }

  public RaftClusterHealth getClusterHealth() throws RaftException {
    return state.getClusterHealth(this);
  }

  public RaftClientManager getClientManager() {
    return clientManager;
  }

  @Override
  public String toString() {
    return "RaftStateContext [state=" + state + ", configurationState=" + configurationState + "]";
  }

  Follower buildStateFollower(Optional<Replica> leader) {
    return new Follower(log, threadPools, configurationState.getElectionTimeout() * 3, leader);
  }

  Start buildStateStart() {
    return new Start(log);
  }

  Stopped buildStateStopped() {
    return new Stopped(log);
  }

  Candidate buildStateCandidate() {
    return new Candidate(log, threadPools, configurationState.getElectionTimeout() * 3, clientManager);
  }
  
  Leader buildStateLeader() {
    return new Leader(log, threadPools, configurationState.getElectionTimeout());
  }

  public Replica self() {
    return this.log.self();
  }

  Random random() {
    return random;
  }
}
