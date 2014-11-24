package org.robotninjas.barge.state;

import java.io.IOException;

import com.google.common.util.concurrent.ListenableFuture;

import org.robotninjas.barge.RaftClusterHealth;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.log.RaftLog;

import javax.annotation.Nonnull;

import static org.robotninjas.barge.proto.RaftProto.*;

class Start extends BaseState {

  public Start(RaftStateContext ctx) {
    super(RaftState.START, ctx);
  }

  @Override
  public void init() {
    RaftLog log = getLog();

    // MDC.put("state", RaftState.START.name());
    // MDC.put("term", Long.toString(log.currentTerm()));
    // MDC.put("self", self().toString());

    try {
      log.load();
    } catch (IOException e) {
      throw new IllegalStateException("Error loading log", e);
    }
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
  public ListenableFuture<Object> commitOperation(@Nonnull byte[] operation) throws RaftException {
    throw new RaftException("Service has not started yet");
  }

  @Override
  public RaftClusterHealth getClusterHealth() throws RaftException {
    throw new RaftException("Service has not started yet");
  }

  @Override
  public ListenableFuture<Boolean> setConfiguration(RaftMembership oldMembership, RaftMembership newMembership)
      throws RaftException {
    throw new RaftException("Service has not started yet");
  }

}
