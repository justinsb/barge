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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.robotninjas.barge.BargeThreadPools;
import org.robotninjas.barge.NoLeaderException;
import org.robotninjas.barge.RaftClusterHealth;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftProto.RequestVoteResponse;
import org.robotninjas.barge.rpc.RaftClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.addCallback;
import static org.robotninjas.barge.proto.RaftProto.RequestVote;
import static org.robotninjas.barge.state.MajorityCollector.majorityResponse;
import static org.robotninjas.barge.state.Raft.StateType.*;
import static org.robotninjas.barge.state.RaftPredicates.voteGranted;

@NotThreadSafe
class Candidate extends BaseState {

  private static final Logger LOGGER = LoggerFactory.getLogger(Candidate.class);
  private static final Random RAND = new Random(System.nanoTime());

  private final ScheduledExecutorService scheduler;
  private final long electionTimeout;
  private final RaftClientManager clientManager;
  private DeadlineTimer electionTimer;
  private ListenableFuture<Boolean> electionResult;

  @Inject
  Candidate(RaftLog log, BargeThreadPools bargeThreadPools, long electionTimeout, RaftClientManager clientManager) {
    super(CANDIDATE, log);
    this.scheduler = bargeThreadPools.getRaftScheduler();
    this.electionTimeout = electionTimeout;
    this.clientManager = clientManager;
  }

  @Override
  public void init(@Nonnull final RaftStateContext ctx) {

    final RaftLog log = getLog();

    log.currentTerm(log.currentTerm() + 1);
    log.votedFor(Optional.of(log.self()));

    ConfigurationState configurationState = ctx.getConfigurationState();
    
    if (configurationState.getAllVotingMembers().isEmpty()) {
      // If there is nobody to vote in an election, we can't run it...
      LOGGER.debug("No voting members; won't run election");
      return;
    }
    
    LOGGER.debug("Election starting for term {}", log.currentTerm());

    List<ListenableFuture<RequestVoteResponse>> responses = Lists.newArrayList();
    Replica self = configurationState.self();
    LOGGER.info("Starting election with members: {}", configurationState.getAllVotingMembers());
    for (Replica replica : configurationState.getAllVotingMembers()) {
      if (replica == self) {
        // We always vote for ourselves
        responses.add(Futures.immediateFuture(RequestVoteResponse.newBuilder().setVoteGranted(true).buildPartial()));
      } else {
        ListenableFuture<RequestVoteResponse> response = sendVoteRequest(ctx, replica);
        Futures.addCallback(response, checkTerm(ctx));
        responses.add(response);
      }
    }

    electionResult = majorityResponse(responses, voteGranted());

    long timeout = electionTimeout + (RAND.nextLong() % electionTimeout);
    electionTimer = DeadlineTimer.start(scheduler, new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("Election timeout");
        ctx.setState(Candidate.this, CANDIDATE);
      }
    }, timeout);

    addCallback(electionResult, new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(@Nullable Boolean elected) {
        checkNotNull(elected);
        //noinspection ConstantConditions
        if (elected) {
          ctx.setState(Candidate.this, LEADER);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (!electionResult.isCancelled()) {
          LOGGER.debug("Election failed with exception:", t);
        }
      }

    });

  }

  @Override
  public void destroy(RaftStateContext ctx) {
    if (electionResult != null) {
      electionResult.cancel(false);
    }
    if (electionTimer != null) {
      electionTimer.cancel();
    }
  }

  @VisibleForTesting
  ListenableFuture<RequestVoteResponse> sendVoteRequest(RaftStateContext ctx, Replica replica) {
    ConfigurationState configurationState = ctx.getConfigurationState();
    
    RaftLog log = getLog();
    RequestVote request =
      RequestVote.newBuilder()
        .setTerm(log.currentTerm())
        .setCandidateId(configurationState.self().toString())
        .setLastLogIndex(log.lastLogIndex())
        .setLastLogTerm(log.lastLogTerm())
        .build();

    ListenableFuture<RequestVoteResponse> response = clientManager.requestVote(replica, request);
    Futures.addCallback(response, checkTerm(ctx));

    return response;
  }

  private FutureCallback<RequestVoteResponse> checkTerm(final RaftStateContext ctx) {
    return new FutureCallback<RequestVoteResponse>() {
      @Override
      public void onSuccess(@Nullable RequestVoteResponse response) {
        if (response.getTerm() > getLog().currentTerm()) {
          getLog().currentTerm(response.getTerm());
          ctx.setState(Candidate.this, FOLLOWER);
        }
      }

      @Override
      public void onFailure(Throwable t) {}
    };
  }
  
  @Override
  public ListenableFuture<Boolean> setConfiguration(RaftStateContext ctx, RaftMembership oldMembership,  RaftMembership newMembership) throws RaftException {
    throw new NoLeaderException();
  }

  @Override
  public RaftClusterHealth getClusterHealth(@Nonnull RaftStateContext ctx) throws NoLeaderException {
    throw new NoLeaderException();
  }

  @Nonnull
  @Override
  public ListenableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation) throws RaftException {
    throw new NoLeaderException();
  }
  
  @Override
  public String toString() {
    return "Candidate [" + log.getName() + " @ " + log.self() + "]";
  }
  
}
