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

import java.util.concurrent.ScheduledExecutorService;

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

  Follower(RaftLog log, BargeThreadPools threadPools, long electionTimeout) {
    super(FOLLOWER, log);

    this.scheduler = checkNotNull(threadPools.getRaftScheduler());
    checkArgument(electionTimeout >= 0);
    this.electionTimeout = electionTimeout;
  }

  @Override
  public void init(@Nonnull final RaftStateContext ctx) {
    LOGGER.debug("Init on follower: {}", this);
    
    electionTimeoutTimer = DeadlineTimer.start(scheduler, new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("DeadlineTimer expired, starting election");
        ctx.setState(Follower.this, CANDIDATE);
      }
    }, electionTimeout * 2);
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

  @Override
  public void doStop(RaftStateContext ctx) {
    LOGGER.debug("doStop on follower: {}", this);
    electionTimeoutTimer.cancel();
    super.doStop(ctx);
  }

  @Nonnull
  @Override
  public ListenableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation)
      throws RaftException {
    throw throwMustBeLeader();
  }

  protected RuntimeException throwMustBeLeader() throws RaftException {
    if (leader.isPresent()) {
      throw new NotLeaderException(leader.get());
    } else {
      throw new NoLeaderException();
    }
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
