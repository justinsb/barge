/**
 * Copyright 2013-2014 David Rusek <dave dot rusek at gmail dot com>
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
package org.robotninjas.barge.netty;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

import org.junit.rules.ExternalResource;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.proto.RaftEntry.Membership;
import org.robotninjas.barge.state.Raft;
import org.robotninjas.barge.state.StateTransitionListener;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static org.robotninjas.barge.state.Raft.StateType;

public class GroupOfCounters extends ExternalResource implements StateTransitionListener {

  // private final List<Replica> replicas;
  final Map<Integer, SimpleCounterMachine> servers = Maps.newHashMap();
  private final File target;
  private final Map<Raft, StateType> states = Maps.newConcurrentMap();

  public GroupOfCounters(File target) {
    this.target = target;
  }

  public SimpleCounterMachine addServer(int id, int ... peers) throws IOException {
    // replicas.add(Replica.fromString("localhost:" + i));
    Replica self = buildReplicaName(id);
    List<Replica> allMembers = Lists.newArrayList();
    for (int peer : peers) {
      allMembers.add(buildReplicaName(peer));
    }
    ClusterConfig seedConfig = new ClusterConfig(self, allMembers, 1000);
    SimpleCounterMachine server = new SimpleCounterMachine(id, seedConfig, this);
    servers.put(id, server);

    server.makeLogDirectory(target);
    server.startRaft();

    return server;
  }

  private Replica buildReplicaName(int id) {
    return Replica.fromString("localhost:" + (10000 + id));
  }

  @Override
  protected void after() {
    for (SimpleCounterMachine server : servers.values()) {
      server.stop();
      server.deleteLogDirectory();
    }
  }

  public void commitToLeader(byte[] bytes) throws RaftException, InterruptedException {
    getLeader().get().commit(bytes);
  }

  Optional<SimpleCounterMachine> getLeader() {
    List<SimpleCounterMachine> leaders = Lists.newArrayList();
    for (SimpleCounterMachine server : servers.values()) {
      if (server.isLeader()) {
        leaders.add(server);
      }
    }
    if (leaders.size() == 0) {
      return Optional.absent();
    }
    if (leaders.size() == 1) {
      return Optional.of(leaders.get(0));
    }
    throw new IllegalStateException("Found multiple leaders");
  }

  /**
   * Wait for all {@link SimpleCounterMachine} in the cluster to reach a consensus value.
   *
   * @param target
   *          expected value for each machine' counter.
   * @param timeout
   *          timeout in ms. Timeout is evaluated per instance of counter within the cluster.
   */
  public void waitAllToReachValue(long target, long timeout) {
    for (SimpleCounterMachine counter : servers.values()) {
      counter.waitForValue(target, timeout);
    }
  }

  void waitForLeaderElection() throws InterruptedException {
    new Prober(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return thereIsOneLeader();
      }
    }).probe(10000);
  }

  private Boolean thereIsOneLeader() {
    int numberOfLeaders = 0;
    int numberOfFollowers = 0;
    for (StateType stateType : states.values()) {
      switch (stateType) {
      case LEADER:
        numberOfLeaders++;
        break;
      case FOLLOWER:
        numberOfFollowers++;
        break;
      }
    }

    return numberOfLeaders == 1;
  }

  int clusterMemberCount() {
    int clusterMembers = 0;
    for (StateType stateType : states.values()) {
      switch (stateType) {
      case LEADER:
        clusterMembers++;
        break;
      case FOLLOWER:
        clusterMembers++;
        break;
      }
    }

    return clusterMembers;
  }

  @Override
  public void changeState(@Nonnull Raft context, @Nullable StateType from, @Nonnull StateType to) {
    states.put(context, to);
  }

  @Override
  public void invalidTransition(@Nonnull Raft context, @Nonnull StateType actual, @Nullable StateType expected) {
    // IGNORED
  }

//  public void bootstrap(int id) {
//    SimpleCounterMachine seed = servers.get(id);
//    Membership.Builder membership = Membership.newBuilder();
//    membership.addMembers(seed.self.getKey());
//    seed.bootstrap(membership.build());
//  }

  public void changeCluster(Collection<Replica> replicas) throws Exception {
    Optional<SimpleCounterMachine> leader = getLeader();
    if (!leader.isPresent()) {
      throw new IllegalStateException("No leader; can't reconfigure");
    }
    SimpleCounterMachine currentLeader = leader.get();

    RaftMembership oldMembership = currentLeader.getClusterMembership();
    RaftMembership newMembership = oldMembership.buildProposal(replicas);
    ListenableFuture<Boolean> future = currentLeader.setConfiguration(oldMembership, newMembership);
    Boolean result = future.get();
    if (!result) {
      throw new IllegalStateException("Failed to changed cluster");
    }
  }

  public void printState() {
    System.out.println("Current cluster state:");
    for (SimpleCounterMachine counter : servers.values()) {
      System.out.println("\t" + counter.toString());
    }

  }
}
