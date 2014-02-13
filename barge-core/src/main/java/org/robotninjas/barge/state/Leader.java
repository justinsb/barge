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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftExecutor;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftEntry.Membership;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.robotninjas.barge.proto.RaftProto.*;
import static org.robotninjas.barge.state.Raft.StateType.FOLLOWER;
import static org.robotninjas.barge.state.Raft.StateType.LEADER;

@NotThreadSafe
class Leader extends BaseState {

  private static final Logger LOGGER = LoggerFactory.getLogger(Leader.class);

  private final Fiber scheduler;
  private final long timeout;
  private final Map<Replica, ReplicaManager> managers = Maps.newHashMap();
  private final ReplicaManagerFactory replicaManagerFactory;
  private Disposable heartbeatTask;

  private long replicaManagersLastUpdated;

  @Inject
  Leader(RaftLog log, @RaftExecutor Fiber scheduler,
         @ElectionTimeout @Nonnegative long timeout, ReplicaManagerFactory replicaManagerFactory) {

    super(LEADER, log);

    this.scheduler = checkNotNull(scheduler);
    checkArgument(timeout > 0);
    this.timeout = timeout;
    this.replicaManagerFactory = checkNotNull(replicaManagerFactory);

  }

  @Override
  public void init(@Nonnull RaftStateContext ctx) {

    sendRequests(ctx);
    resetTimeout(ctx);

  }

  private ReplicaManager getReplicaManager(RaftStateContext ctx, Replica replica) {
    assert replica != ctx.getConfigurationState().self();
    ReplicaManager replicaManager = managers.get(replica);
    if (replicaManager == null) {
      replicaManager = replicaManagerFactory.create(replica);
      managers.put(replica, replicaManager);
    }

    // Remove old entries
    {
      long configurationVersion = ctx.getConfigurationState().getVersion();
      if (configurationVersion != replicaManagersLastUpdated) {
        Set<Replica> allMembers = Sets.newHashSet(ctx.getConfigurationState().getAllVotingMembers());
        Iterator<Entry<Replica, ReplicaManager>> it = managers.entrySet().iterator();
        while (it.hasNext()) {
          Entry<Replica, ReplicaManager> entry = it.next();
          assert entry.getKey() != ctx.getConfigurationState().self();
          if (!allMembers.contains(entry.getKey())) {
            it.remove();
          }
        }
        replicaManagersLastUpdated = configurationVersion;
      }
    }

    return replicaManager;
  }

  @Override
  public void doStop(RaftStateContext ctx) {
    destroy(ctx);
    super.doStop(ctx);
  }

  public void destroy(RaftStateContext ctx) {
    LOGGER.info("Stepping down from leadership");

    heartbeatTask.dispose();
    for (ReplicaManager mgr : managers.values()) {
      mgr.shutdown();
    }
  }

  void resetTimeout(@Nonnull final RaftStateContext ctx) {

    if (null != heartbeatTask) {
      heartbeatTask.dispose();
    }

    heartbeatTask = scheduler.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("Sending heartbeat");
        sendRequests(ctx);
      }
    }, timeout, timeout, MILLISECONDS);

  }

  @Nonnull
  @Override
  public ListenableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation) throws RaftException {
    resetTimeout(ctx);
    ListenableFuture<Object> result = getLog().append(operation, null);
    sendRequests(ctx);
    return result;

  }

  @Nonnull
  public ListenableFuture<Object> commitMembership(@Nonnull RaftStateContext ctx, @Nonnull Membership membership) throws RaftException {
    resetTimeout(ctx);
    ListenableFuture<Object> result = getLog().append(null, membership);
    sendRequests(ctx);
    return result;

  }

  /**
   * Find the median value of the list of matchIndex, this value is the committedIndex since, by definition, half of the
   * matchIndex values are greater and half are less than this value. So, at least half of the replicas have stored the
   * median value, this is the definition of committed.
   * @return 
   */
  private long getQuorumMatchIndex(RaftStateContext ctx, List<Replica> replicas) {

    RaftLog log = getLog();
    
    Replica self = log.self();

    List<Long> sorted = newArrayList();
    for (Replica replica : replicas) {
      if (replica == self) {
        sorted.add(log.lastLogIndex());
      } else {
        ReplicaManager replicaManager = getReplicaManager(ctx, replica);
        if (replicaManager == null) {
          throw new IllegalStateException("No replica manager for server: " + replica);
        }
        sorted.add(replicaManager.getMatchIndex());
       }
    }
  
    Collections.sort(sorted);

    int n = sorted.size();
    int quorumSize = (n / 2) + 1;
    final long committed = sorted.get(quorumSize - 1);

    LOGGER.debug("updating commitIndex to {}; sorted is {}", committed, sorted);
    return committed;

  }

  long getQuorumMatchIndex(RaftStateContext ctx, ConfigurationState configurationState) {
    if (configurationState.isTransitional()) {
      return Math.min(getQuorumMatchIndex(ctx, configurationState.getCurrentMembers()),
          getQuorumMatchIndex(ctx, configurationState.getProposedMembers()));
    } else {
      return getQuorumMatchIndex(ctx, configurationState.getCurrentMembers());
    }
  }

  private void updateCommitted(RaftStateContext ctx) {
    ConfigurationState configurationState = ctx.getConfigurationState();
 
    long committed = getQuorumMatchIndex(ctx, configurationState);
    getLog().commitIndex(committed);
 
    if (committed >= configurationState.getId()) {
      handleConfigurationUpdate(ctx);
    }

  }

  private void handleConfigurationUpdate(RaftStateContext ctx) {
    ConfigurationState configurationState = ctx.getConfigurationState();

    // Upon committing a configuration that excludes itself, the leader
    // steps down.
    if (!configurationState.hasVote(configurationState.self())) {
      destroy(ctx /* logcabin: currentTerm + 1 */);
      return;
    }

    // Upon committing a reconfiguration (Cold,new) entry, the leader
    // creates the next configuration (Cnew) entry.
    if (configurationState.isTransitional()) {
      final Membership membership = configurationState.getMembership();
      
      Membership.Builder members = Membership.newBuilder();
      members.addAllMembers(membership.getProposedMembersList());

      Futures.addCallback(commitMembership(ctx, members.build()), new FutureCallback<Object>() {

        @Override
        public void onSuccess(Object result) {
          if (Boolean.TRUE.equals(result)) {
            LOGGER.info("Committed new cluster configuration: {}", membership);
          } else {
            LOGGER.warn("Failed to commit new cluster configuration: {}", membership);
          }
        }

        @Override
        public void onFailure(Throwable t) {
          LOGGER.warn("Error committing new cluster configuration: {}", membership);
        }
      });
    }
  }

  private void checkTermOnResponse(RaftStateContext ctx, AppendEntriesResponse response) {

      if (response.getTerm() > getLog().currentTerm()) {
        getLog().currentTerm(response.getTerm());
        ctx.setState(this, FOLLOWER);
      }

  }

  /**
   * Notify the {@link ReplicaManager} to send an update the next possible time it can
   *
   * @return futures with the result of the update
   */
  @Nonnull
  @VisibleForTesting
  List<ListenableFuture<AppendEntriesResponse>> sendRequests(final RaftStateContext ctx) {
    if (ctx.shouldStop()) {
      destroy(ctx);
      return null;
    }

    List<ListenableFuture<AppendEntriesResponse>> responses = newArrayList();
    Replica self = ctx.getConfigurationState().self();
    for (Replica replica : ctx.getConfigurationState().getAllVotingMembers()) {
      if (replica == self) {
        continue;
      }

      ReplicaManager replicaManager = getReplicaManager(ctx, replica);
      ListenableFuture<AppendEntriesResponse> response = replicaManager.requestUpdate();
      responses.add(response);
      Futures.addCallback(response, new FutureCallback<AppendEntriesResponse>() {
        @Override
        public void onSuccess(@Nullable AppendEntriesResponse result) {
          updateCommitted(ctx);
          checkTermOnResponse(ctx, result);
        }

        @Override
        public void onFailure(Throwable t) {
          LOGGER.debug("Failure response from replica", t);
        }

      });

    }

    // Cope if we're the only node in the cluster.
    if (responses.isEmpty()) {
      updateCommitted(ctx);
    }

    return responses;
  }

  
  public ListenableFuture<Boolean> setConfiguration(@Nonnull RaftStateContext ctx, RaftMembership oldMembership,
      RaftMembership newMembership) throws RaftException {
    // TODO: Check quorums overlap?
    final Membership targetConfiguration;
    {
      Membership.Builder b = Membership.newBuilder();

      List<String> members = Lists.newArrayList(newMembership.getMembers());
      Collections.sort(members);
      b.addAllMembers(members);
      targetConfiguration = b.build();
    }
    
    final ConfigurationState configuration = ctx.getConfigurationState();
    if (configuration.isTransitional()) {
      LOGGER.warn("Failing setConfiguration due to transitional configuration");
      return Futures.immediateFuture(Boolean.FALSE);
    }

    if (configuration.getId() != oldMembership.getId()) {
      // configuration has changed in the meantime
      LOGGER.warn("Failing setConfiguration due to version mismatch: {} vs {}", configuration.getId(), oldMembership);
      return Futures.immediateFuture(Boolean.FALSE);
    }

    // TODO: Introduce Staging state; wait for staging servers to catch up

    // Commit a transitional configuration with old and new
    final Membership transitionalMembership;
    {
      Membership.Builder b = Membership.newBuilder();

      List<String> members = Lists.newArrayList(oldMembership.getMembers());
      Collections.sort(members);
      b.addAllMembers(members);

      b.addAllProposedMembers(targetConfiguration.getMembersList());
      transitionalMembership = b.build();
    }
    ListenableFuture<Object> transitionFuture = commitMembership(ctx, transitionalMembership);

    return Futures.transform(transitionFuture, new AsyncFunction<Object, Boolean>() {
      // In response to the transitional configuration, the leader commits the final configuration
      // We poll for this configuration
      @Override
      public ListenableFuture<Boolean> apply(Object input) throws Exception {
        if (!Boolean.TRUE.equals(input)) {
          LOGGER.warn("Failed to apply transitional configuration");
          return Futures.immediateFuture(Boolean.FALSE);
        }
        
        return new PollFor<Boolean>(scheduler, 200, 200, TimeUnit.MILLISECONDS) {
          @Override
          protected Optional<Boolean> poll() {
            if (!configuration.isTransitional()) {
              boolean success = configuration.getMembership().equals(targetConfiguration);
              if (success) {
                LOGGER.info("Applied new configuration: {}", configuration);
              } else {
                LOGGER.warn("Could not apply configuration: wanted={} actual={}", targetConfiguration, configuration.getMembership());
              }
              return Optional.of(success);
            }
            return Optional.absent();
          }
        }.start();
      }
    });
  }

  
}
