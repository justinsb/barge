package org.robotninjas.barge.state;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.robotninjas.barge.*;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftProto;
import org.robotninjas.barge.proto.RaftProto.RequestVoteResponse;
import org.robotninjas.barge.rpc.Client;

import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;
import static org.robotninjas.barge.proto.RaftProto.AppendEntries;
import static org.robotninjas.barge.proto.RaftProto.RequestVote;
import static org.robotninjas.barge.state.Raft.StateType.CANDIDATE;
import static org.robotninjas.barge.state.Raft.StateType.LEADER;

public class CandidateTest {

  private final long term = 2L;

  private @Mock BargeThreadPools mockThreadPools;  
  private @Mock Replica mockReplica;
//  private final ClusterConfig config = ClusterConfigStub.getStub();
//  private final Replica self = config.local();
  private final Replica self = Replica.fromString("localhost:1");
  private @Mock Client mockRaftClient;
  private @Mock StateMachine mockStateMachine;
  private @Mock RaftLog mockRaftLog;
  private @Mock RaftStateContext mockRaftStateContext;

  private @Mock ConfigurationState configurationState;

  
  @Before
  public void initMocks() {

    MockitoAnnotations.initMocks(this);

    when(configurationState.self()).thenReturn(self);
    when(configurationState.getAllMembers()).thenReturn(Arrays.asList(self));
    when(configurationState.getAllVotingMembers()).thenReturn(Arrays.asList(self));
    
    when(mockRaftStateContext.getConfigurationState()).thenReturn(configurationState);
    
    when(mockRaftLog.self()).thenReturn(self);
    when(mockRaftLog.votedFor()).thenReturn(Optional.<Replica>absent());
    when(mockRaftLog.lastLogTerm()).thenReturn(0L);
    when(mockRaftLog.lastLogIndex()).thenReturn(0L);
    when(mockRaftLog.currentTerm()).thenReturn(term);
    
//    when(mockRaftLog.config()).thenReturn(config);
//    when(mockRaftLog.getReplica(anyString())).thenAnswer(new Answer<Replica>() {
//      @Override
//      public Replica answer(InvocationOnMock invocation) throws Throwable {
//        String arg = (String) invocation.getArguments()[0];
//        return config.getReplica(arg);
//      }
//    });


    ScheduledExecutorService mockRaftScheduler = mock(ScheduledExecutorService.class);
     when(mockThreadPools.getRaftScheduler()).thenReturn(mockRaftScheduler);
    
    ScheduledFuture mockScheduledFuture = mock(ScheduledFuture.class);
    when(mockRaftScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
    .thenReturn(mockScheduledFuture);
    
    when(mockRaftStateContext.type()).thenReturn(CANDIDATE);

    RequestVoteResponse response = RequestVoteResponse.newBuilder().setTerm(0).setVoteGranted(true).build();
    ListenableFuture<RequestVoteResponse> responseFuture = Futures.immediateFuture(response);
    when(mockRaftClient.requestVote(any(Replica.class), any(RequestVote.class))).thenReturn(responseFuture);
  }

  @Test
  public void testRequestVoteWithNewerTerm() throws Exception {

    Candidate candidate = new Candidate(mockRaftLog, mockThreadPools, 150, mockRaftClient);
    candidate.init(mockRaftStateContext);

    Replica mockCandidate = Replica.fromString("other");

    RequestVote request =
      RequestVote.newBuilder()
        .setCandidateId(mockCandidate.toString())
        .setLastLogIndex(1L)
        .setLastLogTerm(1L)
        .setTerm(4L)
        .build();

    candidate.requestVote(mockRaftStateContext, request);

    verify(mockRaftLog).currentTerm(3L);
    verify(mockRaftLog).currentTerm(4L);
    verify(mockRaftLog, times(2)).currentTerm(anyLong());

    verify(mockRaftLog).votedFor(Optional.of(self));
    verify(mockRaftLog).votedFor(Optional.of(mockCandidate));
    verify(mockRaftLog, times(2)).votedFor(any(Optional.class));

    verify(mockRaftLog, never()).commitIndex(anyLong());

    verify(mockRaftStateContext, times(1)).setState(any(Candidate.class), eq(Raft.StateType.FOLLOWER));
    verify(mockRaftStateContext, times(1)).setState(any(Candidate.class), eq(Raft.StateType.LEADER));
    verify(mockRaftStateContext, times(1)).getConfigurationState();
    verifyNoMoreInteractions(mockRaftStateContext);
    
    verifyZeroInteractions(mockRaftClient);

  }

  @Test
  public void testRequestVoteWithOlderTerm() throws Exception {
    Candidate candidate = new Candidate(mockRaftLog, mockThreadPools, 150, mockRaftClient);
    candidate.init(mockRaftStateContext);

    Replica mockCandidate = Replica.fromString("other");

    RequestVote request =
      RequestVote.newBuilder()
        .setCandidateId(mockCandidate.toString())
        .setLastLogIndex(1L)
        .setLastLogTerm(1L)
        .setTerm(1L)
        .build();

    candidate.requestVote(mockRaftStateContext, request);

    verify(mockRaftLog).currentTerm(3L);
    verify(mockRaftLog, times(1)).currentTerm(anyLong());

    verify(mockRaftLog).votedFor(Optional.of(self));
    verify(mockRaftLog, never()).votedFor(Optional.of(mockCandidate));
    verify(mockRaftLog, times(1)).votedFor(any(Optional.class));

    verify(mockRaftLog, never()).commitIndex(anyLong());

    verify(mockRaftStateContext).setState(any(Candidate.class), eq(Raft.StateType.LEADER));
    verify(mockRaftStateContext, times(1)).getConfigurationState();
    verifyNoMoreInteractions(mockRaftStateContext);
    
    verifyZeroInteractions(mockRaftClient);
  }

  @Test
  public void testRequestVoteWithSameTerm() throws Exception {
    Candidate candidate = new Candidate(mockRaftLog, mockThreadPools, 150, mockRaftClient);
    candidate.init(mockRaftStateContext);

    Replica mockCandidate = Replica.fromString("other");

    RequestVote request =
      RequestVote.newBuilder()
        .setCandidateId(mockCandidate.toString())
        .setLastLogIndex(1L)
        .setLastLogTerm(1L)
        .setTerm(2L)
        .build();

    candidate.requestVote(mockRaftStateContext, request);

    verify(mockRaftLog).currentTerm(3L);
    verify(mockRaftLog, times(1)).currentTerm(anyLong());

    verify(mockRaftLog).votedFor(Optional.of(self));
    verify(mockRaftLog, times(1)).votedFor(Optional.of(mockCandidate));
    verify(mockRaftLog, times(2)).votedFor(any(Optional.class));

    verify(mockRaftLog, never()).commitIndex(anyLong());

    verify(mockRaftStateContext).setState(any(State.class), any(RaftStateContext.StateType.class));

    verify(mockRaftStateContext, times(1)).getConfigurationState();
    verifyNoMoreInteractions(mockRaftStateContext);

    verifyZeroInteractions(mockRaftClient);
  }

  @Test
  public void testAppendEntriesWithNewerTerm() throws Exception {

    Candidate candidate = new Candidate(mockRaftLog, mockThreadPools, 1, mockRaftClient);
    candidate.init(mockRaftStateContext);

    Replica mockLeader = Replica.fromString("other");

    AppendEntries request =
      AppendEntries.newBuilder()
        .setTerm(4L)
        .setLeaderId(mockLeader.toString())
        .setPrevLogIndex(1L)
        .setPrevLogTerm(1L)
        .setCommitIndex(1L)
        .build();

    candidate.appendEntries(mockRaftStateContext, request);

    verify(mockRaftLog).currentTerm(3L);
    verify(mockRaftLog).currentTerm(4L);
    verify(mockRaftLog, times(2)).currentTerm(anyLong());

    verify(mockRaftLog).votedFor(Optional.of(self));
    verify(mockRaftLog, times(1)).votedFor(any(Optional.class));

    verify(mockRaftLog, times(1)).commitIndex(anyLong());

    verify(mockRaftStateContext).setState(any(Candidate.class), eq(Raft.StateType.FOLLOWER));
    verify(mockRaftStateContext).setState(any(Candidate.class), eq(Raft.StateType.LEADER));
    verify(mockRaftStateContext, times(1)).getConfigurationState();
    verifyNoMoreInteractions(mockRaftStateContext);
    
    verifyZeroInteractions(mockRaftClient);

  }

  @Test
  public void testAppendEntriesWithOlderTerm() throws Exception {

    Candidate candidate = new Candidate(mockRaftLog, mockThreadPools, 1, mockRaftClient);
    candidate.init(mockRaftStateContext);

    Replica mockLeader = Replica.fromString("other");

    AppendEntries request =
      RaftProto.AppendEntries.newBuilder()
        .setTerm(1L)
        .setLeaderId(mockLeader.toString())
        .setPrevLogIndex(1L)
        .setPrevLogTerm(1L)
        .setCommitIndex(1L)
        .build();

    candidate.appendEntries(mockRaftStateContext, request);

    verify(mockRaftLog).currentTerm(3L);
    verify(mockRaftLog, times(1)).currentTerm(anyLong());

    verify(mockRaftLog).votedFor(Optional.of(self));
    verify(mockRaftLog, times(1)).votedFor(any(Optional.class));

    verify(mockRaftLog, never()).commitIndex(anyLong());

    verify(mockRaftStateContext).setState(any(Candidate.class), eq(Raft.StateType.LEADER));
    verify(mockRaftStateContext, times(1)).getConfigurationState();
    verifyNoMoreInteractions(mockRaftStateContext);
    
    verifyZeroInteractions(mockRaftClient);

  }

  @Test
  public void testAppendEntriesWithSameTerm() throws Exception {

    Candidate candidate = new Candidate(mockRaftLog, mockThreadPools, 1, mockRaftClient);
    candidate.init(mockRaftStateContext);

    AppendEntries request =
      AppendEntries.newBuilder()
        .setTerm(2L)
        .setLeaderId("leader:1000")
        .setPrevLogIndex(1L)
        .setPrevLogTerm(1L)
        .setCommitIndex(1L)
        .build();

    candidate.appendEntries(mockRaftStateContext, request);

    verify(mockRaftLog).currentTerm(3L);
    verify(mockRaftLog, times(1)).currentTerm(anyLong());

    verify(mockRaftLog).votedFor(Optional.of(self));
    verify(mockRaftLog, times(1)).votedFor(any(Optional.class));

    verify(mockRaftLog, times(1)).commitIndex(anyLong());

    verify(mockRaftStateContext).setState(any(Candidate.class), eq(LEADER));
    verify(mockRaftStateContext, times(1)).getConfigurationState();
    verifyNoMoreInteractions(mockRaftStateContext);
    
    verifyZeroInteractions(mockRaftClient);

  }

}
