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

import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

import org.robotninjas.barge.*;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.state.Raft.StateType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.robotninjas.barge.proto.RaftProto.*;
import static org.robotninjas.barge.state.Raft.StateType.CANDIDATE;
import static org.robotninjas.barge.state.Raft.StateType.FOLLOWER;

@NotThreadSafe
class Follower extends BaseState {

  private static final Logger LOGGER = LoggerFactory.getLogger(Follower.class);

  private final ScheduledExecutorService scheduler;
  private final long electionTimeout;
  private DeadlineTimer electionTimeoutTimer;

  Follower(RaftLog log, BargeThreadPools threadPools, long electionTimeout, Optional<Replica> leader) {
    super(FOLLOWER, log);

    this.leader = leader;
    
    this.scheduler = checkNotNull(threadPools.getRaftScheduler());
    checkArgument(electionTimeout >= 0);
    this.electionTimeout = electionTimeout;
  }

  @Override
  public void init(@Nonnull final RaftStateContext ctx) {
    LOGGER.debug("Init on follower: {}", this);

//    Raft uses randomized election timeouts to ensure that
//    split votes are rare and that they are resolved quickly. To
//    prevent split votes in the first place, election timeouts are
//    chosen randomly from a fixed interval (e.g., 150–300ms).

    int t = (int) (electionTimeout * (1.0 + ctx.random().nextFloat()));
    electionTimeoutTimer = DeadlineTimer.start(scheduler, new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("DeadlineTimer expired, starting election");
        ctx.setState(Follower.this, ctx.buildStateCandidate());
      }
    }, t);
  }

  @Override
  public void destroy(RaftStateContext ctx) {
    LOGGER.debug("destroy on follower: {}", this);
    electionTimeoutTimer.cancel();
  }


  protected void resetTimer() {
    LOGGER.debug("resetTimer on follower: {}", this);
    electionTimeoutTimer.reset();
  }

  @Nonnull
  @Override
  public ListenableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation)
      throws RaftException {
    throw throwMustBeLeader();
  }

  protected RuntimeException throwMustBeLeader() throws RaftException {
      throw new NotLeaderException(leader);
  }

  @Override
  public ListenableFuture<Boolean> setConfiguration(RaftStateContext ctx, RaftMembership oldMembership,
      RaftMembership newMembership) throws RaftException {
    throw throwMustBeLeader();
  }
  

  @Override
  public RaftClusterHealth getClusterHealth(@Nonnull RaftStateContext ctx) throws RaftException {
    throw  throwMustBeLeader();
  }
  
  
  @Override
  public String toString() {
    return "Follower [" + log.getName() + " @ " + log.self() + "]";
  }
   
}
