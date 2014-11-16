package org.robotninjas.barge.state;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.robotninjas.barge.RaftClusterHealth;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftProto;
import org.robotninjas.barge.state.Raft.StateType;

import javax.annotation.Nonnull;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.robotninjas.barge.proto.RaftProto.RequestVote;

public class BaseStateTest {

  // private final ClusterConfig config = ClusterConfigStub.getStub();
  // private final Replica self = config.local();
  private final Replica candidate = Replica.fromString("candidate");
  private final Replica self = Replica.fromString("local");
  private @Mock RaftLog mockRaftLog;
  private @Mock RaftStateContext mockRaftStateContext;

  @Before
  public void initMocks() {

    MockitoAnnotations.initMocks(this);

    when(mockRaftLog.currentTerm()).thenReturn(2l);
    when(mockRaftLog.lastLogIndex()).thenReturn(2l);
    when(mockRaftLog.lastLogTerm()).thenReturn(2l);
    when(mockRaftLog.self()).thenReturn(self);

    when(mockRaftStateContext.getLog()).thenReturn(mockRaftLog);
    // when(mockRaftLog.config()).thenReturn(config);
    // when(mockRaftLog.getReplica(anyString())).thenAnswer(new Answer<Replica>() {
    // @Override
    // public Replica answer(InvocationOnMock invocation) throws Throwable {
    // String arg = (String) invocation.getArguments()[0];
    // return config.getReplica(arg);
    // }
    // });

  }

  @Test
  public void testHaventVoted() {

    BaseState state = new EmptyState(mockRaftStateContext);

    RequestVote requestVote = RequestVote.newBuilder().setCandidateId(candidate.toString()).setLastLogIndex(2)
        .setLastLogTerm(2).setTerm(2).build();

    when(mockRaftLog.votedFor()).thenReturn(Optional.<Replica> absent());
    boolean shouldVote = state.shouldVoteFor(requestVote);

    assertTrue(shouldVote);
  }

  @Test
  public void testAlreadyVotedForCandidate() {

    BaseState state = new EmptyState(mockRaftStateContext);

    RequestVote requestVote = RequestVote.newBuilder().setCandidateId(candidate.toString()).setLastLogIndex(2)
        .setLastLogTerm(2).setTerm(2).build();

    when(mockRaftLog.votedFor()).thenReturn(Optional.of(candidate));
    boolean shouldVote = state.shouldVoteFor(requestVote);

    assertTrue(shouldVote);
  }

  @Test
  @Ignore
  public void testCandidateWithGreaterTerm() {

    BaseState state = new EmptyState(mockRaftStateContext);

    RequestVote requestVote = RequestVote.newBuilder().setCandidateId(candidate.toString()).setLastLogIndex(2)
        .setLastLogTerm(3).setTerm(2).build();

    Replica otherCandidate = Replica.fromString("other");
    when(mockRaftLog.votedFor()).thenReturn(Optional.of(otherCandidate));
    boolean shouldVote = state.shouldVoteFor(requestVote);

    assertTrue(shouldVote);
  }

  @Test
  public void testCandidateWithLesserTerm() {

    BaseState state = new EmptyState(mockRaftStateContext);

    RequestVote requestVote = RequestVote.newBuilder().setCandidateId(candidate.toString()).setLastLogIndex(2)
        .setLastLogTerm(1).setTerm(2).build();

    Replica otherCandidate = Replica.fromString("other");
    when(mockRaftLog.votedFor()).thenReturn(Optional.of(otherCandidate));
    boolean shouldVote = state.shouldVoteFor(requestVote);

    assertFalse(shouldVote);
  }

  @Test
  public void testCandidateWithLesserIndex() {

    BaseState state = new EmptyState(mockRaftStateContext);

    RequestVote requestVote = RequestVote.newBuilder().setCandidateId(candidate.toString()).setLastLogIndex(1)
        .setLastLogTerm(2).setTerm(2).build();

    Replica otherCandidate = Replica.fromString("other");
    when(mockRaftLog.votedFor()).thenReturn(Optional.of(otherCandidate));
    boolean shouldVote = state.shouldVoteFor(requestVote);

    assertFalse(shouldVote);
  }

  @Test
  @Ignore
  public void testCandidateWithGreaterIndex() {

    BaseState state = new EmptyState(mockRaftStateContext);

    RequestVote requestVote = RequestVote.newBuilder().setCandidateId(candidate.toString()).setLastLogIndex(3)
        .setLastLogTerm(2).setTerm(2).build();

    Replica otherCandidate = Replica.fromString("other");
    when(mockRaftLog.votedFor()).thenReturn(Optional.of(otherCandidate));
    boolean shouldVote = state.shouldVoteFor(requestVote);

    assertTrue(shouldVote);
  }

  static class EmptyState extends BaseState {
    protected EmptyState(RaftStateContext ctx) {
      super(StateType.START, ctx);
    }

    @Override
    public void init() {

    }

    @Override
    public void destroy() {

    }

    @Nonnull
    @Override
    public RaftProto.RequestVoteResponse requestVote(@Nonnull RequestVote request) {
      return null;
    }

    @Nonnull
    @Override
    public RaftProto.AppendEntriesResponse appendEntries(@Nonnull RaftProto.AppendEntries request) {
      return null;
    }

    @Nonnull
    @Override
    public ListenableFuture<Object> commitOperation(@Nonnull byte[] operation) throws RaftException {
      return null;
    }

    @Override
    public ListenableFuture<Boolean> setConfiguration(RaftMembership oldMembership, RaftMembership newMembership)
        throws RaftException {
      return null;
    }

    @Override
    public RaftClusterHealth getClusterHealth() {
      return null;
    }

  }

}
