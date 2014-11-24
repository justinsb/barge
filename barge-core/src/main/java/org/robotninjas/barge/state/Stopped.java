package org.robotninjas.barge.state;

import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.RaftClusterHealth;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftMembership;
import javax.annotation.Nonnull;

import static org.robotninjas.barge.proto.RaftProto.*;

class Stopped extends BaseState {

  public Stopped(RaftStateContext ctx) {
    super(RaftState.STOPPED, ctx);
  }

  @Override
  public void init() {
  }

  @Nonnull
  @Override
  public RequestVoteResponse requestVote(@Nonnull RequestVote request) {
    throw new RuntimeException("Service is stopped");
  }

  @Nonnull
  @Override
  public AppendEntriesResponse appendEntries(@Nonnull AppendEntries request) {
    throw new RuntimeException("Service is stopped");
  }

  @Nonnull
  @Override
  public ListenableFuture<Object> commitOperation(@Nonnull byte[] operation) throws RaftException {
    throw new RaftException("Service is stopped");
  }

  @Override
  public RaftClusterHealth getClusterHealth() throws RaftException {
    throw new RaftException("Service is stopped");
  }

  @Override
  public ListenableFuture<Boolean> setConfiguration(RaftMembership oldMembership, RaftMembership newMembership) throws RaftException {
    throw new RaftException("Service is stopped");
  }

  @Override
  public String toString() {
    return "Stopped";
  }

}
