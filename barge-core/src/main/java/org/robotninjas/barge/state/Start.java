package org.robotninjas.barge.state;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

import org.robotninjas.barge.RaftClusterHealth;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.log.RaftLog;
import org.slf4j.MDC;

import javax.annotation.Nonnull;

import static org.robotninjas.barge.proto.RaftProto.*;
import static org.robotninjas.barge.state.Raft.StateType.FOLLOWER;
import static org.robotninjas.barge.state.Raft.StateType.START;

class Start extends BaseState {

  public Start(RaftStateContext ctx) {
    super(START, ctx);
  }

  @Override
  public void init() {
    RaftLog log = getLog();

    MDC.put("state", Raft.StateType.START.name());
    MDC.put("term", Long.toString(log.currentTerm()));
    MDC.put("self", log.self().toString());
    log.load();
    ctx.setState(this, ctx.buildStateFollower(leader));
  }

  @Nonnull
  @Override
  public RequestVoteResponse requestVote(@Nonnull RequestVote request) {
    throw new RuntimeException("Service unavailable");
  }

  @Nonnull
  @Override
  public AppendEntriesResponse appendEntries(@Nonnull AppendEntries request) {
    throw new RuntimeException("Service unavailable");
  }

  @Nonnull
  @Override
  public ListenableFuture<Object> commitOperation( @Nonnull byte[] operation) throws RaftException {
    throw new RaftException("Service has not started yet");
  }

  @Override
  public RaftClusterHealth getClusterHealth() throws RaftException {
    throw new RaftException("Service has not started yet");
  }
  
  @Override
  public ListenableFuture<Boolean> setConfiguration(RaftMembership oldMembership, RaftMembership newMembership) throws RaftException {
    throw new RaftException("Service has not started yet");
  }
  
}
