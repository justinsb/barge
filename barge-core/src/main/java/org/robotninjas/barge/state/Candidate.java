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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.robotninjas.barge.NotLeaderException;
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

import java.util.List;
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

  private DeadlineTimer electionTimer;
  private ListenableFuture<Boolean> electionResult;

  Candidate(RaftStateContext ctx) {
    super(CANDIDATE, ctx);
  }

  @Override
  public void init() {
    electionResult = runElection();
  }

  ListenableFuture<Boolean> runElection() {
    final SettableFuture<Boolean> result = SettableFuture.create();
    final RaftLog log = getLog();

    log.currentTerm(log.currentTerm() + 1);
    log.votedFor(Optional.of(log.self()));

    ConfigurationState configurationState = ctx.getConfigurationState();

    LOGGER.debug("Election starting for term {}", log.currentTerm());

    List<ListenableFuture<RequestVoteResponse>> responses = Lists.newArrayList();
    Replica self = configurationState.self();
    LOGGER.info("Starting election with members: {}", configurationState.getAllVotingMembers());
    for (Replica replica : configurationState.getAllVotingMembers()) {
      if (replica == self) {
        // We always vote for ourselves
        responses.add(Futures.immediateFuture(RequestVoteResponse.newBuilder().setVoteGranted(true).buildPartial()));
      } else {
        ListenableFuture<RequestVoteResponse> response = sendVoteRequest(replica);
        Futures.addCallback(response, checkTerm());
        responses.add(response);
      }
    }

    ListenableFuture<Boolean> majorityResponse = majorityResponse(responses, voteGranted());

    long timeout = ctx.getTimeouts().getCandidateElectionTimeout();
    ScheduledExecutorService scheduler = ctx.getRaftScheduler();

    electionTimer = DeadlineTimer.start(scheduler, new Runnable() {
      @Override
      public void run() {
        synchronized (result) {
          if (!result.isDone()) {
            LOGGER.debug("Election timeout");
            result.set(null);
            ctx.setState(Candidate.this, ctx.buildStateCandidate());
          }
        }
      }
    }, timeout);

    addCallback(majorityResponse, new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(@Nullable Boolean elected) {
        synchronized (result) {
          checkNotNull(elected);
          if (!result.isDone()) {
            result.set(elected);
            if (elected) {
              ctx.setState(Candidate.this, ctx.buildStateLeader());
            }
          }
        }
      }

      @Override
      public void onFailure(Throwable t) {
        synchronized (result) {
          if (!result.isDone()) {
            LOGGER.debug("Election failed with exception:", t);
            result.setException(t);
          }
        }
      }

    });

    return result;
  }

  @Override
  public void destroy() {
    if (electionResult != null) {
      electionResult.cancel(false);
    }
    if (electionTimer != null) {
      electionTimer.cancel();
    }
  }

  @VisibleForTesting
  ListenableFuture<RequestVoteResponse> sendVoteRequest(Replica replica) {
    ConfigurationState configurationState = ctx.getConfigurationState();

    RaftLog log = getLog();
    RequestVote request = RequestVote.newBuilder().setTerm(log.currentTerm())
        .setCandidateId(configurationState.self().toString()).setLastLogIndex(log.lastLogIndex())
        .setLastLogTerm(log.lastLogTerm()).build();

    RaftClientManager clientManager = ctx.getClientManager();
    ListenableFuture<RequestVoteResponse> response = clientManager.requestVote(replica, request);
    return response;
  }

  private FutureCallback<RequestVoteResponse> checkTerm() {
    return new FutureCallback<RequestVoteResponse>() {
      @Override
      public void onSuccess(@Nullable RequestVoteResponse response) {
        if (response.getTerm() > getLog().currentTerm()) {
          getLog().currentTerm(response.getTerm());
          ctx.setState(Candidate.this, ctx.buildStateFollower(Optional.<Replica> absent()));
        }
      }

      @Override
      public void onFailure(Throwable t) {
      }
    };
  }

  @Override
  public ListenableFuture<Boolean> setConfiguration(RaftMembership oldMembership, RaftMembership newMembership)
      throws RaftException {
    throw new NotLeaderException(Optional.<Replica> absent());
  }

  @Override
  public RaftClusterHealth getClusterHealth() throws NotLeaderException {
    throw new NotLeaderException(Optional.<Replica> absent());
  }

  @Nonnull
  @Override
  public ListenableFuture<Object> commitOperation(@Nonnull byte[] operation) throws RaftException {
    throw new NotLeaderException(Optional.<Replica> absent());
  }

}
