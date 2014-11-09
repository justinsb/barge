package org.robotninjas.barge.state;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

import org.robotninjas.barge.NoLeaderException;
import org.robotninjas.barge.RaftClusterHealth;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.log.RaftLog;

import javax.annotation.Nonnull;

import static org.robotninjas.barge.proto.RaftProto.*;
import static org.robotninjas.barge.state.Raft.StateType.STOPPED;

class Stopped extends BaseState {

  @Inject
  public Stopped(RaftLog log) {
    super(STOPPED, log);
  }

  @Override
  public void init(RaftStateContext ctx) {
  }

  @Nonnull
  @Override
  public RequestVoteResponse requestVote(@Nonnull RaftStateContext ctx, @Nonnull RequestVote request) {
    throw new RuntimeException("Service is stopped");
  }

  @Nonnull
  @Override
  public AppendEntriesResponse appendEntries(@Nonnull RaftStateContext ctx, @Nonnull AppendEntries request) {
    throw new RuntimeException("Service is stopped");
  }

  @Nonnull
  @Override
  public ListenableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation) throws RaftException {
    throw new RaftException("Service is stopped");
  }

  @Override
  public RaftClusterHealth getClusterHealth(RaftStateContext raftStateContext) throws NoLeaderException, RaftException {
    throw new RaftException("Service is stopped");
  }

  @Override
  public ListenableFuture<Boolean> setConfiguration(RaftStateContext ctx, RaftMembership oldMembership, RaftMembership newMembership) throws RaftException {
    throw new RaftException("Service is stopped");
  }

  @Override
  public String toString() {
    return "Stopped";
  }

}
