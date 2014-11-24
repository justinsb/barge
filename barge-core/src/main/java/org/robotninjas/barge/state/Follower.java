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
import com.google.common.util.concurrent.ListenableFuture;

import org.robotninjas.barge.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;


@NotThreadSafe
class Follower extends BaseState {

  private static final Logger LOGGER = LoggerFactory.getLogger(Follower.class);

  private DeadlineTimer electionTimeoutTimer;

  Follower(RaftStateContext ctx, Optional<Replica> leader) {
    super(RaftState.FOLLOWER, ctx);

    this.leader = leader;
  }

  @Override
  public void init() {
    LOGGER.debug("Init on follower: {}", this);

    // Raft uses randomized election timeouts to ensure that
    // split votes are rare and that they are resolved quickly. To
    // prevent split votes in the first place, election timeouts are
    // chosen randomly from a fixed interval (e.g., 150–300ms).

    long timeout = ctx.getTimeouts().getFollowerElectionStartDelay();
    timeout = timeout + (ctx.random().nextLong() % timeout);

    ScheduledExecutorService scheduler = ctx.getRaftScheduler();

    electionTimeoutTimer = DeadlineTimer.start(scheduler, new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("DeadlineTimer expired, starting election");
        ctx.setState(Follower.this, ctx.buildStateCandidate());
      }
    }, timeout);
  }

  @Override
  public void destroy() {
    LOGGER.debug("destroy on follower: {}", this);
    electionTimeoutTimer.cancel();
  }

  protected void resetTimer() {
    LOGGER.debug("resetTimer on follower: {}", this);
    electionTimeoutTimer.reset();
  }

  @Nonnull
  @Override
  public ListenableFuture<Object> commitOperation(@Nonnull byte[] operation) throws RaftException {
    throw throwMustBeLeader();
  }

  protected RuntimeException throwMustBeLeader() throws RaftException {
    throw new NotLeaderException(leader);
  }

  @Override
  public ListenableFuture<Boolean> setConfiguration(RaftMembership oldMembership, RaftMembership newMembership)
      throws RaftException {
    throw throwMustBeLeader();
  }

  @Override
  public RaftClusterHealth getClusterHealth() throws RaftException {
    throw throwMustBeLeader();
  }

}
