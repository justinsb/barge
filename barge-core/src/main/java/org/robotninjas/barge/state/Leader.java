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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.robotninjas.barge.RaftClusterHealth;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftEntry.Membership;
import org.robotninjas.barge.proto.RaftEntry.SnapshotInfo;
import org.robotninjas.barge.rpc.RaftClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.robotninjas.barge.proto.RaftProto.*;

@NotThreadSafe
class Leader extends BaseState {

  private static final Logger LOGGER = LoggerFactory.getLogger(Leader.class);

  private static final long HEALTH_TIMEOUT = TimeUnit.MINUTES.toNanos(1);

  private final Map<Replica, ReplicaManager> replicaManagers = Maps.newHashMap();
  private long replicaManagersLastUpdated;

  private ScheduledFuture<?> heartbeatTask;

  private boolean isShutdown;

  final long minSnapshotInterval;

  Leader(RaftStateContext ctx) {
    super(RaftState.LEADER, ctx);

    this.leader = Optional.of(ctx.self());

    this.minSnapshotInterval = 60 * 1000;
  }

  long lastRequest;
  long lastSnapshot;

  @Override
  public void init() {
    sendRequests(false);
    startHeartbeatTimer();
  }

  private ReplicaManager getManagerForReplica(Replica replica) {
    assert replica != ctx.getConfigurationState().self();
    ReplicaManager replicaManager = replicaManagers.get(replica);
    if (replicaManager == null) {
      // replicaManager = replicaManagerFactory.create(replica);
      RaftClient raftClient = ctx.getRaftClient(replica);
      replicaManager = new ReplicaManager(ctx.getLog(), raftClient, replica);
      replicaManagers.put(replica, replicaManager);
    }

    // Remove old entries
    {
      long configurationVersion = ctx.getConfigurationState().getVersion();
      if (configurationVersion != replicaManagersLastUpdated) {
        Set<Replica> allMembers = Sets.newHashSet(ctx.getConfigurationState().getAllMembers());
        Iterator<Entry<Replica, ReplicaManager>> it = replicaManagers.entrySet().iterator();
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
  public void destroy() {
    this.isShutdown = true;
    
    if (heartbeatTask != null) {
      heartbeatTask.cancel(false);
      heartbeatTask = null;
    }

    for (ReplicaManager mgr : replicaManagers.values()) {
      mgr.shutdown();
    }
  }

  public void stepDown(Optional<Replica> newLeader) {
    if (!this.isShutdown) {
      this.isShutdown = true;
      ctx.setState(this, ctx.buildStateFollower(newLeader));
    }
  }

  void startHeartbeatTimer() {
    lastRequest = System.nanoTime();

    if (heartbeatTask != null) {
      heartbeatTask.cancel(false);
      heartbeatTask = null;
    }

    long heartbeatInterval = ctx.getTimeouts().getHeartbeatInterval();

    ScheduledExecutorService scheduler = ctx.getRaftScheduler();

    heartbeatTask = scheduler.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        if (isActive()) {
          doHeartbeat();
        }
      }
    }, heartbeatInterval, heartbeatInterval, MILLISECONDS);

  }

  void doHeartbeat() {
    long now = System.nanoTime();
    long timeSinceLastRequest = now - lastRequest;
    long timeSinceLastSnapshot = now - lastSnapshot;

    long heartbeatInterval = ctx.getTimeouts().getHeartbeatInterval();
    heartbeatInterval *= 1E6;
    if (timeSinceLastRequest > heartbeatInterval) {
      LOGGER.debug("Sending heartbeat");
      sendRequests(true);
    }

    if (timeSinceLastSnapshot > (minSnapshotInterval * 1E6)) {
      if (getLog().shouldSnapshot()) {
        LOGGER.debug("Starting snapshot");

        lastSnapshot = now;
        ctx.onRaftThreadAsync(() -> performSnapshot());
      }
    }
  }

  void resetTimeout() {
    lastRequest = System.nanoTime();
  }

  @Nonnull
  @Override
  public ListenableFuture<Object> commitOperation(@Nonnull byte[] operation) throws RaftException {
    resetTimeout();
    ListenableFuture<Object> result = getLog().append(operation, null);
    sendRequests(false);

    return result;
  }

  ListenableFuture<Object> commitMembership(@Nonnull Membership membership) {
    resetTimeout();

    ListenableFuture<Object> result = ctx.getLog().append(null, membership);
    sendRequests(false);
    return result;
  }

  ListenableFuture<SnapshotInfo> performSnapshot() throws RaftException {
    ListenableFuture<SnapshotInfo> result = ctx.getLog().performSnapshot();
    sendRequests(false);
    return result;
  }

  /**
   * Find the median value of the list of matchIndex, this value is the committedIndex since, by definition, half of the
   * matchIndex values are greater and half are less than this value. So, at least half of the replicas have stored the
   * median value, this is the definition of committed.
   */
  long getQuorumMatchIndex(List<Replica> replicas) {
    RaftLog log = ctx.getLog();

    Replica self = self();

    List<Long> sorted = newArrayList();
    for (Replica replica : replicas) {
      if (replica == self) {
        sorted.add(log.getLastLogIndex());
      } else {
        ReplicaManager replicaManager = getManagerForReplica(replica);
        if (replicaManager == null) {
          throw new IllegalStateException("No replica manager for server: " + replica);
        }
        sorted.add(replicaManager.getMatchIndex());
      }

    }
    Collections.sort(sorted);

    int n = sorted.size();
    int quorumSize;
    if ((n & 1) == 1) {
      // Odd
      quorumSize = (n + 1) / 2;
    } else {
      // Even
      quorumSize = (n / 2) + 1;
    }
    final int middle = quorumSize - 1;
    assert middle < sorted.size();
    assert middle >= 0;
    final long committed = sorted.get(middle);

    LOGGER.info("State={} Quorum={}", sorted, committed);
    return committed;
  }

  long getQuorumMatchIndex(ConfigurationState configurationState) {
    if (configurationState.isTransitional()) {
      return Math.min(getQuorumMatchIndex(configurationState.getCurrentMembers()),
          getQuorumMatchIndex(configurationState.getProposedMembers()));
    } else {
      return getQuorumMatchIndex(configurationState.getCurrentMembers());
    }
  }

  private void updateCommitted() {
    RaftLog log = ctx.getLog();
    ConfigurationState configurationState = ctx.getConfigurationState();

    long committed = getQuorumMatchIndex(configurationState);
    log.setCommitIndex(committed);

    if (committed >= configurationState.getId()) {
      handleConfigurationUpdate();
    }

  }

  private void handleConfigurationUpdate() {
    ConfigurationState configurationState = ctx.getConfigurationState();

    // Upon committing a configuration that excludes itself, the leader
    // steps down.
    if (!configurationState.hasVote(configurationState.self())) {
      LOGGER.info("Don't have vote in new configuration; stepping down");

      stepDown( /* logcabin: currentTerm + 1 */Optional.<Replica> absent());
      return;
    }

    // Upon committing a reconfiguration (Cold,new) entry, the leader
    // creates the next configuration (Cnew) entry.
    if (configurationState.isTransitional()) {
      final Membership membership = configurationState.getMembership();

      Membership.Builder members = Membership.newBuilder();
      members.addAllMembers(membership.getProposedMembersList());

      Futures.addCallback(commitMembership(members.build()), new FutureCallback<Object>() {

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

  private void checkTermOnResponse(AppendEntriesResponse response) {
    RaftLog log = ctx.getLog();

    long currentTerm = log.getCurrentTerm();
    if (response.getTerm() < currentTerm) {
      LOGGER.info("Responder has out-of-date term; ignoring response: {}", response);
    }

    if (response.getTerm() > currentTerm) {
      LOGGER.debug("Got response with greater term; stepping down: {}", response);

      log.setCurrentTerm(response.getTerm());
      Optional<Replica> newLeader = Optional.absent();

      stepDown(newLeader);
    }

  }

  /**
   * Notify the {@link ReplicaManager} to send an update the next possible time it can
   *
   * @return futures with the result of the update
   */
  @Nonnull
  @VisibleForTesting
  List<ListenableFuture<AppendEntriesResponse>> sendRequests(boolean heartbeat) {
    if (ctx.shouldStop()) {
      LOGGER.info("Context stopping; stepping down");

      stepDown(Optional.<Replica> absent());
      return null;
    }

    List<ListenableFuture<AppendEntriesResponse>> responses = newArrayList();

    ConfigurationState configurationState = ctx.getConfigurationState();
    Replica self = configurationState.self();
    for (Replica replica : configurationState.getAllMembers()) {
      if (replica == self) {
        continue;
      }

      boolean isVoting = configurationState.hasVote(replica);
      ReplicaManager replicaManager = getManagerForReplica(replica);
      ListenableFuture<AppendEntriesResponse> response = replicaManager.requestUpdate();
      SettableFuture<AppendEntriesResponse> returned = SettableFuture.create();
      responses.add(returned);
      Futures.addCallback(response, new FutureCallback<AppendEntriesResponse>() {
        @Override
        public void onSuccess(@Nullable AppendEntriesResponse result) {
          try {
            if (!isActive()) {
              LOGGER.warn("Ignoring response for inactive state");
              throw new IllegalStateException("No longer active");
            }
            updateCommitted();
            checkTermOnResponse(result);
            returned.set(result);
          } catch (Exception e) {
            returned.setException(e);
          }
        }

        @Override
        public void onFailure(Throwable t) {
          LOGGER.debug("Failure response from replica", t);
          returned.setException(t);
        }

      });

    }

    // Cope if we're the only node in the cluster.
    if (responses.isEmpty()) {
      updateCommitted();
    }

    return responses;
  }

  @Override
  public ListenableFuture<Boolean> setConfiguration(RaftMembership oldMembership, final RaftMembership newMembership)
      throws RaftException {
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
    ListenableFuture<Object> transitionFuture = commitMembership(transitionalMembership);

    return Futures.transform(transitionFuture, new AsyncFunction<Object, Boolean>() {
      // In response to the transitional configuration, the leader commits the final configuration
      // We poll for this configuration
      @Override
      public ListenableFuture<Boolean> apply(Object input) throws Exception {
        if (!Boolean.TRUE.equals(input)) {
          LOGGER.warn("Failed to apply transitional configuration");
          return Futures.immediateFuture(Boolean.FALSE);
        }

        return new PollFor<Boolean>(ctx.getRaftScheduler(), 200, 200, TimeUnit.MILLISECONDS) {
          @Override
          protected Optional<Boolean> poll() {
            if (!configuration.isTransitional()) {
              boolean success = configuration.getMembership().equals(targetConfiguration);
              if (success) {
                LOGGER.info("Applied new configuration: {}", configuration);
              } else {
                LOGGER.warn("Could not apply configuration: wanted={} actual={}", targetConfiguration,
                    configuration.getMembership());
              }
              return Optional.of(success);
            }
            return Optional.absent();
          }
        }.start();
      }
    });
  }

  @Override
  public RaftClusterHealth getClusterHealth() {
    ConfigurationState configurationState = ctx.getConfigurationState();
    if (configurationState.isTransitional()) {
      return null;
    }

    List<Replica> deadPeers = Lists.newArrayList();

    Replica self = ctx.self();
    for (Replica replica : configurationState.getAllMembers()) {
      if (replica == self) {
        continue;
      }

      ReplicaManager replicaManager = getManagerForReplica(replica);
      long now = System.nanoTime();
      long lastSeen = replicaManager.getLastProofOfLife();
      if ((now - lastSeen) > HEALTH_TIMEOUT) {
        deadPeers.add(replica);
      }
    }

    RaftMembership membership = configurationState.getClusterMembership();
    RaftClusterHealth health = new RaftClusterHealth(membership, deadPeers);
    return health;
  }

}
