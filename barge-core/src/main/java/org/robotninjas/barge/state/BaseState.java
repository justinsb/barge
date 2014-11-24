package org.robotninjas.barge.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import org.robotninjas.barge.RaftClusterHealth;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.robotninjas.barge.proto.RaftProto.*;

public abstract class BaseState {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseState.class);

  private final RaftState type;
  protected final RaftStateContext ctx;
  protected Optional<Replica> leader;

  protected BaseState(@Nonnull RaftState type, @Nonnull RaftStateContext ctx) {
    this.ctx = checkNotNull(ctx);
    this.type = checkNotNull(type);
    this.leader = Optional.absent();
  }

  public RaftState type() {
    return type;
  }

  protected RaftLog getLog() {
    return ctx.getLog();
  }

  public void destroy() {
  }

  @VisibleForTesting
  boolean shouldVoteFor(@Nonnull RequestVote request) {

    RaftLog log = getLog();

    // If votedFor is null or candidateId, and candidate's log is at
    // least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

    Optional<Replica> votedFor = log.votedFor();
    Replica candidate = Replica.fromString(request.getCandidateId());

    if (votedFor.isPresent()) {
      if (!votedFor.get().equals(candidate)) {
        return false;
      }
    }

    assert !votedFor.isPresent() || votedFor.get().equals(candidate);

    boolean logIsComplete;
    if (request.getLastLogTerm() > log.lastLogTerm()) {
      logIsComplete = true;
    } else if (request.getLastLogTerm() == log.lastLogTerm()) {
      if (request.getLastLogIndex() >= log.lastLogIndex()) {
        logIsComplete = true;
      } else {
        logIsComplete = false;
      }
    } else {
      logIsComplete = false;
    }

    if (logIsComplete) {
      // Requestor has an up-to-date log, we haven't voted for anyone else => OK
      return true;
    }

    return false;
  }

  protected void resetTimer() {

  }

  @Nonnull
  public AppendEntriesResponse appendEntries(@Nonnull AppendEntries request) {
    checkNotNull(request);

    LOGGER.debug("AppendEntries prev index {}, prev term {}, num entries {}, term {}", request.getPrevLogIndex(),
        request.getPrevLogTerm(), request.getEntriesCount(), request.getTerm());

    RaftLog log = getLog();

    boolean success = false;

    if (request.getTerm() >= log.currentTerm()) {

      if (request.getTerm() > log.currentTerm()) {

        log.currentTerm(request.getTerm());

        if (ctx.type().equals(RaftState.LEADER) || ctx.type().equals(RaftState.CANDIDATE)) {
          Follower follower = ctx.buildStateFollower(Optional.of(Replica.fromString(request.getLeaderId())));
          ctx.setState(this, follower);
          // TODO: Do we really want to append to log here?
        }

      }

      leader = Optional.of(Replica.fromString(request.getLeaderId()));
      resetTimer();
      success = log.append(request);

      if (request.getCommitIndex() > log.commitIndex()) {
        log.setCommitIndex(Math.min(request.getCommitIndex(), log.lastLogIndex()));
      }

    }

    return AppendEntriesResponse.newBuilder().setTerm(log.currentTerm()).setSuccess(success)
        .setLastLogIndex(log.lastLogIndex()).build();

  }

  @Nonnull
  public RequestVoteResponse requestVote(@Nonnull RequestVote request) {
    checkNotNull(request);

    RaftLog log = getLog();

    boolean voteGranted;

    long term = request.getTerm();
    long currentTerm = log.currentTerm();

    LOGGER.debug("RequestVote received for term {}", term);

    if (term < currentTerm) {
      // Reply false if term < currentTerm (§5.1)
      voteGranted = false;
    } else {

      // If RPC request or response contains term T > currentTerm:
      // set currentTerm = T, convert to follower (§5.1)
      if (term > currentTerm) {

        log.currentTerm(term);

        if (ctx.type().equals(RaftState.LEADER) || ctx.type().equals(RaftState.CANDIDATE)) {
          ctx.setState(this, ctx.buildStateFollower(Optional.<Replica> absent()));
        }

      }

      Replica candidate = Replica.fromString(request.getCandidateId());
      voteGranted = shouldVoteFor(request);

      if (voteGranted) {
        log.votedFor(Optional.of(candidate));
      }

    }

    return RequestVoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(voteGranted).build();

  }

  //
  // // @Nonnull
  // // @Override
  // // public ListenableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation) throws
  // RaftException {
  // // StateType stateType = ctx.type();
  // // Preconditions.checkNotNull(stateType);
  // // if (stateType.equals(FOLLOWER)) {
  // // throw new NotLeaderException(leader.get());
  // // } else if (stateType.equals(CANDIDATE)) {
  // // throw new NoLeaderException();
  // // }
  // // return Futures.immediateCancelledFuture();
  // // }

  public void doStop() {
    ctx.setState(this, ctx.buildStateStopped());
  }

  @Override
  public String toString() {
    RaftLog log = getLog();

    return type.toString() + " [" + log.getName() + " @ " + self() + "]";
  }

  public void init() {

  }

  public abstract ListenableFuture<Object> commitOperation(byte[] operation) throws RaftException;

  public abstract ListenableFuture<Boolean> setConfiguration(RaftMembership oldMembership, RaftMembership newMembership)
      throws RaftException;

  public abstract RaftClusterHealth getClusterHealth() throws RaftException;

  public Optional<Replica> getLeader() {
    return leader;
  }
  
  protected Replica self() {
    return ctx.self();
  }
}
